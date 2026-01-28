package GIS4IoRT.operators.geofencing;

import GIS4IoRT.objects.AssignedPoint;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

// Implements Broadcast State to maintain dynamic robot-to-zone assignments.
// Enriches the stream by attaching target Zone IDs to authorized robots and filters unauthorized traffic.

public class SessionBroadcastManager extends BroadcastProcessFunction<Point, String, AssignedPoint> {

    public static final MapStateDescriptor<String, List<String>> ASSIGNMENT_STATE_DESC =
            new MapStateDescriptor<>(
                    "robot-assignments-broadcast",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    new ListTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO)
            );

    public SessionBroadcastManager() {}

    @Override
    public void processElement(Point point, ReadOnlyContext ctx, Collector<AssignedPoint> out) throws Exception {
        List<String> assignedZones = ctx.getBroadcastState(ASSIGNMENT_STATE_DESC).get(point.objID);

        if (assignedZones == null || assignedZones.isEmpty()) {
            return;
        }

        out.collect(new AssignedPoint(point, assignedZones));
    }

    @Override
    public void processBroadcastElement(String command, Context ctx, Collector<AssignedPoint> out) throws Exception {
        // Format: ROBOT:ACTION:ROBOT_ID:ZONES
        // Np. "ROBOT:ALLOW:Robot1:ZoneA,ZoneB"

        try {
            String[] parts = command.split(":");
            if (parts.length < 3) return;

            String type = parts[0];
            String action = parts[1].toUpperCase();
            String robotID = parts[2];

            if (!"ROBOT".equals(type)) return;

            BroadcastState<String, List<String>> state = ctx.getBroadcastState(ASSIGNMENT_STATE_DESC);

            List<String> currentZones = state.get(robotID);
            if (currentZones == null) {
                currentZones = new ArrayList<>();
            } else {
                currentZones = new ArrayList<>(currentZones);
            }

            List<String> commandZones = new ArrayList<>();
            if (parts.length >= 4 && parts[3] != null && !parts[3].isEmpty()) {
                String[] zIds = parts[3].split(",");
                for (String z : zIds) {
                    commandZones.add(z.trim());
                }
            }

            if ("ALLOW".equals(action)) {
                for (String z : commandZones) {
                    if (!currentZones.contains(z)) {
                        currentZones.add(z);
                    }
                }
                if (!currentZones.isEmpty()) {
                    state.put(robotID, currentZones);
                }
            }
            else if ("BLOCK".equals(action)) {
                if (commandZones.isEmpty()) {
                    state.remove(robotID);
                } else {
                    currentZones.removeAll(commandZones);

                    if (currentZones.isEmpty()) {
                        state.remove(robotID);
                    } else {
                        state.put(robotID, currentZones);
                    }
                }
            }
            else if ("RESET".equals(action) || "CLEAR".equals(action)) {
                state.remove(robotID);
            }
            else if ("RESET_ALL".equals(action)) {
                state.clear();
            }

        } catch (Exception e) {
            System.err.println("Command error: " + command + " -> " + e.getMessage());
        }
    }
}