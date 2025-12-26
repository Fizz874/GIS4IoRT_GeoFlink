package GIS4IoRT.operators;

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
import java.util.List;

public class PointPolygonOutsideJoinQuery extends JoinQuery<Point, Polygon> {

    public PointPolygonOutsideJoinQuery(QueryConfiguration conf, SpatialIndex index1, SpatialIndex index2){
        super.initializeJoinQuery(conf, index1, index2);
    }

    public DataStream<Tuple2<Point, Polygon>> run(DataStream<Point> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, double queryRadius) {
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
            // Tutaj też należałoby podmienić join na coGroup, jeśli ta metoda jest używana
            // Dla uproszczenia przykładu skupiam się na głównej metodzie windowBased
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTimeNaive(ordinaryPointStream, queryPolygonStream, uGrid, qGrid, queryRadius, omegaJoinDurationSeconds, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
        }
        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // ---------------------------------------------------------
    // KLUCZOWA ZMIANA: WINDOW BASED ("Anti-Join" Logic)
    // ---------------------------------------------------------
    private DataStream<Tuple2<Point, Polygon>> windowBased(DataStream<Point> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, UniformGrid qGrid, double queryRadius, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // 1. Strumień punktów z Watermarkami
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        // 2. Replikacja wielokątów (standardowa procedura GeoFlink)
        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryPolygonStream, queryRadius, qGrid);

        // 3. Strumień wielokątów z Watermarkami
        DataStream<Polygon> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        // 4. Użycie coGroup zamiast join
        DataStream<Tuple2<Point, Polygon>> joinOutput = pointStreamWithTsAndWm.coGroup(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new CoGroupFunction<Point, Polygon, Tuple2<Point, Polygon>>() {
                    @Override
                    public void coGroup(Iterable<Point> points, Iterable<Polygon> polygons, Collector<Tuple2<Point, Polygon>> out) throws Exception {

                        // Buforujemy wielokąty w liście, aby móc iterować po nich wielokrotnie (dla każdego punktu)
                        List<Polygon> polygonList = new ArrayList<>();
                        for (Polygon p : polygons) {
                            polygonList.add(p);
                        }

                        // Iterujemy po wszystkich punktach w tym oknie (i w tej komórce siatki)
                        for (Point p : points) {
                            boolean isInsideAny = false;

                            // Sprawdzamy czy punkt pasuje do któregokolwiek wielokąta
                            for (Polygon poly : polygonList) {
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
                                out.collect(Tuple2.of(p, null));
                            }
                        }
                    }
                });

        return joinOutput;
    }


    private DataStream<Tuple2<Point, Polygon>> realTimeNaive(DataStream<Point> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, UniformGrid qGrid, double queryRadius, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
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
                .apply(new CoGroupFunction<Point, Polygon, Tuple2<Point, Polygon>>() {
                    @Override
                    public void coGroup(Iterable<Point> points, Iterable<Polygon> polygons, Collector<Tuple2<Point, Polygon>> out) {
                        List<Polygon> polygonList = new ArrayList<>();
                        for (Polygon p : polygons) polygonList.add(p);

                        for (Point p : points) {
                            boolean isInsideAny = false;
                            for (Polygon poly : polygonList) {
                                if (approximateQuery || DistanceFunctions.getDistance(p, poly) <= queryRadius) {
                                    isInsideAny = true;
                                    break;
                                }
                            }
                            if (!isInsideAny) {
                                out.collect(Tuple2.of(p, null));
                            }
                        }
                    }
                });
    }

}