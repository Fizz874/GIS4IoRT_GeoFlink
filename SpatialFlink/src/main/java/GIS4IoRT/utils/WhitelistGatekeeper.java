package GIS4IoRT.utils;

import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;


public class WhitelistGatekeeper extends BroadcastProcessFunction<Point, String, Point> {

    public static final MapStateDescriptor<String, String> ALLOWED_LIST_DESC =
            new MapStateDescriptor<>(
                    "allowedRobotsState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
            );

    @Override
    public void processBroadcastElement(String command, Context ctx, Collector<Point> out) throws Exception {
        if (command == null || !command.startsWith("ROBOT")) return;

        String[] parts = command.split(":");
        if (parts.length < 3) return;

        String action = parts[1].toUpperCase();
        String robotId = parts[2];

        BroadcastState<String, String> state = ctx.getBroadcastState(ALLOWED_LIST_DESC);

        if ("ALLOW".equals(action)) {
            state.put(robotId, "ALLOWED");
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

        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(ALLOWED_LIST_DESC);

        String robotId = point.objID;

        if (state.contains(robotId)) {
            out.collect(point);
        }
    }
}