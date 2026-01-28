package GIS4IoRT.operators.collisionDetection;

import GIS4IoRT.objects.CollisionEvent;
import GIS4IoRT.utils.GpsDistanceFunctions;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.util.Map;

// Executes core proximity detection logic within local grid partitions using MapState to track the latest robot positions.

public class StatefulCollisionDetector extends KeyedProcessFunction<String, Point, CollisionEvent> {

    private MapState<String, Point> gridState;
    private final double threshold;
    private final long ttlMillis;
    private final UniformGrid grid;

    public StatefulCollisionDetector(double threshold, long ttlMillis, UniformGrid grid) {
        this.threshold = threshold;
        this.ttlMillis = ttlMillis;
        this.grid = grid;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.milliseconds(ttlMillis))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupFullSnapshot()
                .build();

        MapStateDescriptor<String, Point> descriptor =
                new MapStateDescriptor<>("gridRobots", String.class, Point.class);
        descriptor.enableTimeToLive(ttlConfig);

        gridState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(Point incomingRobot, Context ctx, Collector<CollisionEvent> out) throws Exception {

        for (Map.Entry<String, Point> entry : gridState.entries()) {
            Point otherRobot = entry.getValue();
            String otherId = entry.getKey();

            if (incomingRobot.objID.equals(otherId)) continue;


            double dist = GpsDistanceFunctions.getDistance(incomingRobot, otherRobot);

            if (dist <= threshold) {

                String key = incomingRobot.objID.compareTo(otherRobot.objID) < 0
                        ? incomingRobot.objID + ":" + otherRobot.objID
                        : otherRobot.objID + ":" + incomingRobot.objID;

                double midLat = (incomingRobot.point.getY() + otherRobot.point.getY()) / 2.0;
                double midLon = (incomingRobot.point.getX() + otherRobot.point.getX()) / 2.0;
                String ownerGridID = HelperClass.assignGridCellID(new Coordinate(midLon,midLat),grid);

                if (ctx.getCurrentKey().equals(ownerGridID)) {
                    out.collect(new CollisionEvent(
                            key,
                            incomingRobot.objID,
                            otherRobot.objID,
                            dist,
                            System.currentTimeMillis(),
                            incomingRobot.timeStampMillisec,
                            otherRobot.timeStampMillisec,
                            incomingRobot.point.getY(),
                            incomingRobot.point.getX(),
                            otherRobot.point.getY(),
                            otherRobot.point.getX()
                    ));                }
            }
        }

        gridState.put(incomingRobot.objID, incomingRobot);
    }
}