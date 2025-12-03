package GeoFlink;
import org.apache.flink.core.fs.FileSystem;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.spatialOperators.range.PointPolygonOutsideRangeQuery;
import GeoFlink.spatialStreams.Deserialization;
import GeoFlink.utils.Params;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BatchJob implements Serializable {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        config.setString(RestOptions.BIND_PORT, "8082");


        //ParameterTool parameters = ParameterTool.fromArgs(args);
        Params params = new Params("query1b-conf.yml");
        //System.out.println(params);

        /* Cluster */
        boolean onCluster = params.clusterMode;
        String bootStrapServers = params.kafkaBootStrapServers;

        /* Input Stream1 */
        String inputTopicName = params.inputTopicName1;
        String inputFormat = params.inputFormat1;
        String dateFormatStr = params.dateFormatStr1;
        List<Integer> csvTsvSchemaAttr1 = params.csvTsvSchemaAttr1;
        List<Double> gridBBox1 = params.gridBBox1;
        int uniformGridSize = params.numGridCells1;
        double cellLengthMeters = params.cellLength1;
        String inputDelimiter1 = params.inputDelimiter1;


        /* Stream Output */
        String outputTopicName = params.outputTopicName;
        String outputDelimiter = params.outputDelimiter;

        /* Query */
        int queryOption = params.queryOption;
        int parallelism = params.parallelism;
        boolean approximateQuery = params.queryApproximate;
        int omegaDuration = params.queryOmegaDuration;
        List<Coordinate> queryPointCoordinates = params.queryPoints;
        List<List<Coordinate>> queryPolygons = params.queryPolygons;
        int allowedLateness = params.queryOutOfOrderTuples;

        boolean isBatch =  false; //TODO make it one of the parameters


        /* Windows */
        String windowType = params.windowType;
        int windowSize = params.windowInterval;
        int windowSlideStep = params.windowStep;

        double gridMinX = gridBBox1.get(0);
        double gridMinY = gridBBox1.get(1);
        double gridMaxX = gridBBox1.get(2);
        double gridMaxY = gridBBox1.get(3);

        List<Coordinate> queryPolygonCoordinates = queryPolygons.get(0);

        //String bootStrapServers;
        DateFormat inputDateFormat;

        if(dateFormatStr.equals("null"))
            inputDateFormat = null;
        else
            inputDateFormat = new SimpleDateFormat(dateFormatStr);

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", bootStrapServers);
        kafkaProperties.setProperty("group.id", "messageStream");

        // Defining Grid
        UniformGrid uGrid;
        UniformGrid qGrid;

        // Dataset-specific Parameters
        Set<Polygon> queryPolygonSet;


        if(cellLengthMeters > 0) {
            uGrid = new UniformGrid(cellLengthMeters, gridMinX, gridMaxX, gridMinY, gridMaxY);
        }else{
            uGrid = new UniformGrid(uniformGridSize, gridMinX, gridMaxX, gridMinY, gridMaxY);
        }


        //temporarily replacing polygon from the query for the one provided in Postgis format
        //TODO - Edit Params.java to include WKB parsing
        String geom = "0103000020E61000000100000005000000C081182C28780B4068436B9D732B4740803F61E91B770B4074FEB9DD6E2B474080629C0F4C770B4010DE9EC4602B474000977541CD780B403C520AF8692B4740C081182C28780B4068436B9D732B4740";
        byte[] wkb = javax.xml.bind.DatatypeConverter.parseHexBinary(geom);
        WKBReader reader = new WKBReader();
        Geometry geometry = reader.read(wkb);
        List<Coordinate> coordinateList = Arrays.asList(geometry.getCoordinates());

//        System.out.println("Typ: " + geometry.getGeometryType());
//        System.out.println(queryPolygonCoordinates);
//        System.out.println(coordinateList);

        List<List<Coordinate>> listCoordinatePolygon = new ArrayList<List<Coordinate>>();
        listCoordinatePolygon.add(coordinateList);
        queryPolygonSet = Stream.of(new Polygon(listCoordinatePolygon, uGrid)).collect(Collectors.toSet());


        QueryConfiguration realtimeConf = new QueryConfiguration(QueryType.RealTime);
        realtimeConf.setApproximateQuery(approximateQuery);
        realtimeConf.setWindowSize(omegaDuration);


        QueryConfiguration realtimeNaiveConf = new QueryConfiguration(QueryType.RealTimeNaive);
        realtimeNaiveConf.setApproximateQuery(approximateQuery);
        realtimeNaiveConf.setWindowSize(omegaDuration);


        QueryConfiguration windowConf = new QueryConfiguration(QueryType.WindowBased);
        windowConf.setApproximateQuery(approximateQuery);
        windowConf.setAllowedLateness(allowedLateness);
        windowConf.setWindowSize(windowSize);
        windowConf.setSlideStep(windowSlideStep);


        switch(queryOption) {
            case 1: {
                ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
                batchEnv.setParallelism(parallelism);
//                String path = ParcelStreamingJob.class.getClassLoader().getResource("data.csv").getPath();
//                DataSet<String> raw = batchEnv.readTextFile(path);
                DataSet<String> raw = batchEnv.readTextFile("/data/data.csv");

                DataSet<Point> points = raw.map(new Deserialization.CSVTSVToTSpatial(uGrid, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1));
                //points.print();
                DataSet<Point> rNeighbors = new PointPolygonOutsideRangeQuery(realtimeConf, uGrid).runBatch(points, queryPolygonSet, 0.0000001);

                DataSet<Tuple2<Long, Boolean>> windowed = rNeighbors
                        .map(new MapFunction<Point, Tuple2<Long, Boolean>>() {
                            @Override
                            public Tuple2<Long, Boolean> map(Point p) {
                                return new Tuple2<>(p.timeStampMillisec / 1000, true);
                            }
                        })

                        .groupBy(0)
                        .reduce(new ReduceFunction<Tuple2<Long, Boolean>>() {
                            @Override
                            public Tuple2<Long, Boolean> reduce(Tuple2<Long, Boolean> a, Tuple2<Long, Boolean> b) {
                                return new Tuple2<>(a.f0, a.f1 || b.f1);
                            }
                        });


                windowed.map(t -> String.format("Second %d: %s", t.f0, t.f1 ? "TRUE" : "FALSE"))
                        .sortPartition(new KeySelector<String, Long>() {
                            @Override
                            public Long getKey(String value) {
                                String[] parts = value.split(" ");
                                return Long.parseLong(parts[1].replace(":", ""));
                            }
                        }, Order.ASCENDING)
                        .setParallelism(1) //in theory partitionByRange could also help to achieve the global order
                        .writeAsText("/data/output.txt", FileSystem.WriteMode.OVERWRITE);
                //.print();
                batchEnv.execute();

                break;
            }
            default:
                System.out.println("Input Unrecognized. Please select option from 1-10.");
        }
    }
}
