package GIS4IoRT.operators.geofencing;

import GIS4IoRT.objects.AssignedPoint;
import GIS4IoRT.objects.ZoneEvent;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Spatial Join (Checks distance(robot, zone) <= radius)
// Executes core geofencing logic by joining enriched robot positions with active zone definitions.
// Performs local distance calculations within the grid partition to verify spatial constraints.

public class ZoneGridJoinFunction extends KeyedCoProcessFunction<String, AssignedPoint, ZoneEvent, AssignedPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(ZoneGridJoinFunction.class);
    private MapState<String, Polygon> localZones;
    private final double queryRadius;

    public ZoneGridJoinFunction(double queryRadius) {
        this.queryRadius = queryRadius;
    }

    @Override
    public void open(Configuration parameters) {
        localZones = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("gridZones", String.class, Polygon.class)
        );
    }

    @Override
    public void processElement1(AssignedPoint robot, Context ctx, Collector<AssignedPoint> out) throws Exception {

        boolean insideAnyZone = false;

        for (String requiredZoneID : robot.assignedZoneIDs) {
            Polygon zone = localZones.get(requiredZoneID);

            if (zone != null) {

                double dist = DistanceFunctions.getDistance(robot, zone);

                if (dist <= queryRadius) {
                    insideAnyZone = true;
                    break;
                }
            }

        }

        if (!insideAnyZone) {
            out.collect(robot);
        }
    }

    @Override
    public void processElement2(ZoneEvent event, Context ctx, Collector<AssignedPoint> out) throws Exception {

        switch (event.type) {
            case ADD: localZones.put(event.zoneID, event.polygon); break;
            case DELETE: localZones.remove(event.zoneID); break;
            case CLEAR: localZones.clear(); break;
        }
    }
}