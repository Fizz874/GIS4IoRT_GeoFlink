package GIS4IoRT.operators;

import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

// Implements the Broadcast State pattern to dynamically filter the data stream based on real-time control signals.
// It acts as a gatekeeper, ensuring only whitelisted or active robot identifiers are propagated to other operators.

public class WhitelistGatekeeper extends BroadcastProcessFunction<Point, String, Point> {

    public static final MapStateDescriptor<String, Boolean> ALLOWED_LIST_DESC =
            new MapStateDescriptor<>(
                    "allowedRobotsState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.BOOLEAN_TYPE_INFO
            );

    @Override
    public void processBroadcastElement(String command, Context ctx, Collector<Point> out) throws Exception {
        if (command == null || !command.startsWith("ROBOT")) return;

        String[] parts = command.split(":");
        if (parts.length < 3) return;

        String action = parts[1].toUpperCase();
        String robotId = parts[2];

        BroadcastState<String, Boolean> state = ctx.getBroadcastState(ALLOWED_LIST_DESC);

        if ("ALLOW".equals(action)) {
            state.put(robotId, true);
            System.out.println("GATEKEEPER: Robot allowed: " + robotId);
        }
        else if ("BLOCK".equals(action) || "DENY".equals(action)) {
            state.remove(robotId);
            System.out.println("GATEKEEPER: Robot blocked: " + robotId);
        }
        else if ("RESET".equals(action)) {
            state.clear();
            System.out.println("GATEKEEPER: Robot list cleared");
        }
    }

    @Override
    public void processElement(Point point, ReadOnlyContext ctx, Collector<Point> out) throws Exception {

        ReadOnlyBroadcastState<String, Boolean> state = ctx.getBroadcastState(ALLOWED_LIST_DESC);

        String robotId = point.objID;

        if (state.contains(robotId)) {
            out.collect(point);
        }
    }
}