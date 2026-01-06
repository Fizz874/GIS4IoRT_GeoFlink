package GIS4IoRT.operators;

import GIS4IoRT.objects.AssignedPoint;
import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.spatialOperators.join.JoinQuery;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.CoGroupFunction; // Zmiana z JoinFunction
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PointPolygonOutsideJoinQuery<T extends Point> extends JoinQuery<T, Polygon> {

    public PointPolygonOutsideJoinQuery(QueryConfiguration conf, SpatialIndex index1, SpatialIndex index2){
        super.initializeJoinQuery(conf, index1, index2);
    }

    public DataStream<Tuple2<T, Polygon>> run(DataStream<T> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex1();
        UniformGrid qGrid = (UniformGrid) this.getSpatialIndex2();

        if(this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return windowBased(ordinaryPointStream, queryPolygonStream, uGrid, qGrid, queryRadius, omegaJoinDurationSeconds, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
        }
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(ordinaryPointStream, queryPolygonStream, uGrid, qGrid, queryRadius, windowSize, slideStep, allowedLateness, approximateQuery);
        }
        else if(this.getQueryConfiguration().getQueryType() == QueryType.RealTimeNaive) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTimeNaive(ordinaryPointStream, queryPolygonStream, uGrid, qGrid, queryRadius, omegaJoinDurationSeconds, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
        }
        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    private DataStream<Tuple2<T, Polygon>> windowBased(DataStream<T> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, UniformGrid qGrid, double queryRadius, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        DataStream<T> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<T>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(T p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryPolygonStream, queryRadius, qGrid);

        DataStream<Polygon> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<T, Polygon>> joinOutput = pointStreamWithTsAndWm.coGroup(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<T, String>() {
                    @Override
                    public String getKey(T p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new CoGroupFunction<T, Polygon, Tuple2<T, Polygon>>() {
                    @Override
                    public void coGroup(Iterable<T> points, Iterable<Polygon> polygons, Collector<Tuple2<T, Polygon>> out) throws Exception {

                        Map<String, Polygon> zoneMap = new HashMap<>();
                        for (Polygon p : polygons) {
                            zoneMap.put(p.objID, p);
                        }


                        for (T p : points) {

                            List<String> assignedZones = null;
                            if (p instanceof AssignedPoint) {
                                assignedZones = ((AssignedPoint) p).assignedZoneIDs;
                            }


                            boolean isInsideAny = false;

//                            if (assignedZones != null && !assignedZones.isEmpty()) {
//                                for (String zoneID : assignedZones) {
//                                    Polygon targetZone = zoneMap.get(zoneID);
//
//                                    if (targetZone != null) {
//                                        if (approximateQuery) {
//                                            isInsideAny = true;
//                                            break;
//                                        } else {
//                                            if (DistanceFunctions.getDistance(p, targetZone) <= queryRadius) {
//                                                isInsideAny = true;
//                                                break;
//                                            }
//                                        }
//                                    } else{
//                                        System.out.println("--- SYNC ERROR ---");
//                                        System.out.println("Robot Time: " + p.timeStampMillisec);
//                                        System.out.println("Robot Grid: " + p.gridID);
//                                        System.out.println("Lookig for Zone: " + zoneID);
//                                        System.out.println("Available Zones on the Map: " + zoneMap.keySet());
//
//                                        for(Polygon poly : zoneMap.values()) {
//                                            System.out.println(" -> Available Zone: " + poly.objID + " Time: " + poly.timeStampMillisec);
//                                        }
//                                        System.out.println("------------------");
//                                    }
//                                }
//                            } else {
//                                isInsideAny = true;
//                            }
//
//                            if (!isInsideAny) {
//
//                                    out.collect(Tuple2.of(p, null));
//
//                            }

//------------------------------------------------------------------------
                            //TODO change back after testing
                            for (String zoneID : assignedZones) {
                                Polygon targetZone = zoneMap.get(zoneID);

                                if (targetZone != null) {

                                    if (DistanceFunctions.getDistance(p, targetZone) <= 0.0) { // lub <= replicationRadius

                                        out.collect(Tuple2.of(p, targetZone));
                                    }
                                }
                            }
//------------------------------------------------------------------------


                        }
                    }
                });

        return joinOutput;
    }


    private DataStream<Tuple2<T, Polygon>> realTimeNaive(DataStream<T> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, UniformGrid qGrid, double queryRadius, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        DataStream<T> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<T>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(T p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> queryStreamWithTsAndWm =
                queryPolygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        return pointStreamWithTsAndWm.coGroup(queryStreamWithTsAndWm)
                .where(k -> "1").equalTo(k -> "1")
                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new CoGroupFunction<T, Polygon, Tuple2<T, Polygon>>() {
                    @Override
                    public void coGroup(Iterable<T> points, Iterable<Polygon> polygons, Collector<Tuple2<T, Polygon>> out) {
                        Map<String, Polygon> zoneMap = new HashMap<>();
                        for (Polygon p : polygons) {
                            zoneMap.put(p.objID, p);
                        }

                        for (T p : points) {

                            List<String> assignedZones = null;
                            if (p instanceof AssignedPoint) {
                                assignedZones = ((AssignedPoint) p).assignedZoneIDs;
                            }

                            boolean isInsideAny = false;

                            if (assignedZones != null && !assignedZones.isEmpty()) {
                                for (String zoneID : assignedZones) {
                                    Polygon targetZone = zoneMap.get(zoneID);

                                    if (targetZone != null) {
                                        if (approximateQuery) {
                                            isInsideAny = true;
                                            break;
                                        } else {
                                            if (DistanceFunctions.getDistance(p, targetZone) <= queryRadius) {
                                                isInsideAny = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                            } else {
                                isInsideAny = true;
                            }

                            if (!isInsideAny) {
                                out.collect(Tuple2.of(p, null));
                            }
                        }
                    }
                });
    }

}