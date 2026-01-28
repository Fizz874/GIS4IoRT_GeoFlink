package GIS4IoRT.operators.geofencing.batch;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// Loads static polygon geometries (parcels/zones) and indexes them into the Uniform Grid.

public class ParcelLoader {


    public static DataSet<Polygon> load(ExecutionEnvironment env, String filePath, UniformGrid uGrid) {
        return env.readTextFile(filePath)
                .flatMap(new PolygonParser(uGrid));
    }

    public static class PolygonParser extends RichFlatMapFunction<String, Polygon> {

        private final UniformGrid uGrid;
        private transient WKBReader wkbReader;

        public PolygonParser(UniformGrid uGrid) {
            this.uGrid = uGrid;
        }

        @Override
        public void open(Configuration parameters) {
            this.wkbReader = new WKBReader();
        }

        @Override
        public void flatMap(String line, Collector<Polygon> out) throws Exception {
            if (line.startsWith("name") || line.trim().isEmpty()) {
                return;
            }

            try {
                String[] parts = line.split(",");

                if (parts.length < 3) return;

                String zoneId = parts[1].trim();
                String hexWkb = parts[2].trim();

                byte[] geometryBytes = WKBReader.hexToBytes(hexWkb);
                Geometry geometry = wkbReader.read(geometryBytes);

                if (geometry instanceof org.locationtech.jts.geom.Polygon) {
                    out.collect(convertJTS(zoneId, (org.locationtech.jts.geom.Polygon) geometry, uGrid));
                }
                else if (geometry instanceof org.locationtech.jts.geom.MultiPolygon) {
                    org.locationtech.jts.geom.MultiPolygon mp = (org.locationtech.jts.geom.MultiPolygon) geometry;
                    for (int i = 0; i < mp.getNumGeometries(); i++) {
                        org.locationtech.jts.geom.Polygon p = (org.locationtech.jts.geom.Polygon) mp.getGeometryN(i);

                        out.collect(convertJTS(zoneId + "_" + i, p, uGrid));
                    }
                }
            } catch (Exception e) {
                System.err.println("Error parsing line: " + line + " -> " + e.getMessage());
            }
        }

        private Polygon convertJTS(String id, org.locationtech.jts.geom.Polygon jtsPoly, UniformGrid uGrid) {
            List<List<Coordinate>> shape = new ArrayList<>();
            shape.add(Arrays.asList(jtsPoly.getExteriorRing().getCoordinates()));
            for (int i = 0; i < jtsPoly.getNumInteriorRing(); i++) {
                shape.add(Arrays.asList(jtsPoly.getInteriorRingN(i).getCoordinates()));
            }

            return new Polygon(id, shape, System.currentTimeMillis(), uGrid);
        }
    }
}