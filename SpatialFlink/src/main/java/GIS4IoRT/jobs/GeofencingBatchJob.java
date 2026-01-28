package GIS4IoRT.jobs;

import GIS4IoRT.operators.geofencing.batch.PointPolygonOutsideJoinQuery;
import GIS4IoRT.operators.geofencing.batch.ParcelLoader;
import GIS4IoRT.utils.ConfigLoader;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.spatialStreams.Deserialization;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.List;

public class GeofencingBatchJob implements Serializable {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JobConfig implements Serializable {
        public int parallelism = 3;

        public double cellLengthMeters = 0;
        public int uniformGridSize = 100;

        public double gridMinX = 3.404038119601803;
        public double gridMinY = 46.313162063542265;
        public double gridMaxX = 3.482977203745311;
        public double gridMaxY = 46.36624964925836;

        public String robotInputPath = "/data/robots.csv";
        public String parcelsInputPath = "/data/parcelles.csv";
        public String outputPath = "/data/output_alerts.csv";

        // --- Filtering (optional, empty = all) ---
        // Usage in CLI: --targetRobots robot1,robot2
        public String targetRobots = "";
        public String targetParcels = "";

        public double radius = 0.000000001;
    }


    public static void main(String[] args) throws Exception {
        // --- 1. SETUP ---
        final ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        JobConfig config = new JobConfig();
        ParameterTool params = ConfigLoader.load(args, config);
        batchEnv.getConfig().setGlobalJobParameters(params);

        batchEnv.setParallelism(config.parallelism);

        UniformGrid uGrid;
        if (config.cellLengthMeters > 0) {
            uGrid = new UniformGrid(config.cellLengthMeters, config.gridMinX, config.gridMaxX, config.gridMinY, config.gridMaxY);
        } else {
            uGrid = new UniformGrid(config.uniformGridSize, config.gridMinX, config.gridMaxX, config.gridMinY, config.gridMaxY);
        }

        // --- 2. LOAD DATA ---
        DataSet<String> raw = batchEnv.readTextFile(config.robotInputPath);

        List<Integer> csvTsvSchemaAttr1 = Arrays.asList(0, 1, 2, 3);
        String inputDelimiter1 = ",";
        DateFormat inputDateFormat = null;

        // --- 3. SPATIAL MAPPING ---
        DataSet<Point> points = raw.map(new Deserialization.CSVTSVToTSpatial(
                uGrid,
                inputDateFormat,
                inputDelimiter1,
                csvTsvSchemaAttr1
        ))
        .filter(p -> {
            if (config.targetRobots.isEmpty()) return true;
            return Arrays.asList(config.targetRobots.split(",")).contains(p.objID);
        })
        .name("CSV-to-SpatialPoint");


        // --- 4. PREPARE POLYGONS ---
        DataSet<Polygon> queryPolygonSet = ParcelLoader.load(batchEnv,config.parcelsInputPath, uGrid)
                    .filter(p -> {
                    if (config.targetParcels.isEmpty()) return true;
                    return Arrays.asList(config.targetParcels.split(",")).contains(p.objID);
                });

        // --- 5. EXECUTE SPATIAL QUERY ---
        QueryConfiguration realtimeConf = new QueryConfiguration(QueryType.RealTime);
        realtimeConf.setApproximateQuery(false);

        DataSet<Point> joinResults = new PointPolygonOutsideJoinQuery<Point>(realtimeConf, uGrid, uGrid)
                .runBatch(points, queryPolygonSet, config.radius);

        // --- 6. SINK ---
        joinResults
                .sortPartition(new KeySelector<Point, Long>() {
                    @Override
                    public Long getKey(Point p) throws Exception {
                        return p.timeStampMillisec;
                    }
                }, Order.ASCENDING)
                .setParallelism(1)
                .map(p -> p.objID + "," + p.timeStampMillisec + "," + p.point.getY() + "," + p.point.getX())
                .writeAsText(config.outputPath, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        batchEnv.execute("GeofencingBatchJob");
    }

}




