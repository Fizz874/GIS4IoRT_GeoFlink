package GIS4IoRT.operators.geofencing.batch;

import GIS4IoRT.objects.AssignedPoint;
import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.spatialOperators.join.JoinQuery;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.*;

// Identifies points located strictly outside the defined polygons within the specified buffer radius.

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

    public DataSet<Point> runBatch(
            DataSet<Point> ordinaryPointStream,
            DataSet<Polygon> queryPolygonStream,
            double queryRadius
           ) {

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex1();
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();

        DataSet<Tuple2<Polygon, Boolean>> replicatedQueryStream = getReplicatedPolygonDataSet(queryPolygonStream, queryRadius, uGrid);

        return ordinaryPointStream.coGroup(replicatedQueryStream)
                .where(p -> p.gridID)
                .equalTo(t -> t.f0.gridID)
                .with(new CoGroupFunction<Point, Tuple2<Polygon, Boolean>, Point>() {
                    @Override
                    public void coGroup(Iterable<Point> points, Iterable<Tuple2<Polygon, Boolean>> polygons, Collector<Point> out) {

                        List<Tuple2<Polygon, Boolean>> polygonList = new ArrayList<>();
                        for (Tuple2<Polygon, Boolean> poly : polygons) {
                            polygonList.add(poly);
                        }

                        if (polygonList.isEmpty()) {
                            for (Point p : points) {
                                out.collect(p);
                            }
                            return;
                        }

                        for (Point p : points) {
                            boolean isInsideAny = false;

                            for (Tuple2<Polygon, Boolean> entry : polygonList) {
                                Polygon poly = entry.f0;
                                boolean isCandidate = entry.f1;

                                if (!isCandidate) {
                                    isInsideAny = true;
                                    break;
                                }

                                if (approximateQuery) {
                                    isInsideAny = true;
                                    break;
                                } else {
                                    if (DistanceFunctions.getDistance(p, poly) <= queryRadius) {
                                        isInsideAny = true;
                                        break;
                                    }
                                }
                            }

                            if (!isInsideAny) {
                                out.collect(p);
                            }
                        }
                    }
                });
    }

    private DataSet<Tuple2<Polygon, Boolean>> getReplicatedPolygonDataSet(DataSet<Polygon> queryPolygons, double queryRadius, UniformGrid uGrid) {

        return queryPolygons.flatMap(new RichFlatMapFunction<Polygon, Tuple2<Polygon, Boolean>>() {

            @Override
            public void flatMap(Polygon poly, Collector<Tuple2<Polygon, Boolean>> out) throws Exception {
                Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, poly);
                Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, poly, guaranteedNeighboringCells);

                for (String gridID : guaranteedNeighboringCells) {
                    Polygon p = new Polygon(
                            poly.getCoordinates(),
                            poly.objID,
                            poly.gridIDsSet,
                            gridID,
                            poly.timeStampMillisec,
                            poly.boundingBox
                    );
                    out.collect(new Tuple2<>(p, false));
                }

                for (String gridID : candidateNeighboringCells) {
                    Polygon p = new Polygon(
                            poly.getCoordinates(),
                            poly.objID,
                            poly.gridIDsSet,
                            gridID,
                            poly.timeStampMillisec,
                            poly.boundingBox
                    );
                    out.collect(new Tuple2<>(p, true));
                }
            }
        }).returns(new TypeHint<Tuple2<Polygon, Boolean>>(){});
    }

}