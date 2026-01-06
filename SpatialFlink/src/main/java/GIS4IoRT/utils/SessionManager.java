package GIS4IoRT.utils;

import GIS4IoRT.objects.AssignedPoint;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SessionManager extends KeyedCoProcessFunction<String, Point, String, AssignedPoint> {

    private ValueState<List<String>> sessionState;

    @Override
    public void open(Configuration parameters) {
        sessionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "sessionState",
                        Types.LIST(Types.STRING)
                )
        );
    }

    @Override
    public void processElement1(Point p, Context ctx, Collector<AssignedPoint> out) throws Exception {
        List<String> allowedZones = sessionState.value();

        if (allowedZones != null && !allowedZones.isEmpty()) {
            out.collect(new AssignedPoint(p, allowedZones));
        }
    }

    @Override
    public void processElement2(String command, Context ctx, Collector<AssignedPoint> out) throws Exception {
        try {
            String[] parts = command.split(":");
            if (parts.length < 3) return;

            String type = parts[0];
            String action = parts[1].toUpperCase();

            if (!"ROBOT".equals(type)) return;

            List<String> currentZones = sessionState.value();
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
                    sessionState.update(currentZones);
                }
            }
            else if ("BLOCK".equals(action)) {
                if (commandZones.isEmpty()) {
                    sessionState.clear();
                } else {
                    currentZones.removeAll(commandZones);

                    if (currentZones.isEmpty()) {
                        sessionState.clear();
                    } else {
                        sessionState.update(currentZones);
                    }
                }
            }
            else if ("RESET".equals(action)) {
                sessionState.clear();
            }

        } catch (Exception e) {
            System.err.println("Command error: " + command);
        }
    }
}