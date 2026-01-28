package GIS4IoRT.operators.collisionDetection;

import GIS4IoRT.objects.CollisionEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Locale;

// Implements temporal de-noising logic to suppress repetitive alerts for the same robot pair within a configurable cool-down window.

public class CollisionDeduplicator extends KeyedProcessFunction<String, CollisionEvent, String> {

    private ValueState<Long> lastAlertTimeState;
    private final long cooldownMillis; // e.g. 10000ms (10 s)

    public CollisionDeduplicator(long cooldownMillis) {
        this.cooldownMillis = cooldownMillis;
    }

    @Override
    public void open(Configuration parameters) {
        lastAlertTimeState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastAlertTime", Long.class));
    }

    @Override
    public void processElement(CollisionEvent event, Context ctx, Collector<String> out) throws Exception {
        Long lastAlert = lastAlertTimeState.value();
        long now = event.alertTimestamp;

        if (lastAlert == null || (now - lastAlert >= cooldownMillis)) {
            boolean r1IsA = event.r1.compareTo(event.r2) < 0;

            String robotA = r1IsA ? event.r1 : event.r2;
            String robotB = r1IsA ? event.r2 : event.r1;

            long tsA = r1IsA ? event.t1 : event.t2;
            double latA = r1IsA ? event.lat1 : event.lat2;
            double lonA = r1IsA ? event.lon1 : event.lon2;

            long tsB = r1IsA ? event.t2 : event.t1;
            double latB = r1IsA ? event.lat2 : event.lat1;
            double lonB = r1IsA ? event.lon2 : event.lon1;

            String json = String.format(Locale.US,
                    "{\"type\":\"collision\",\"robot_a\":\"%s\",\"robot_b\":\"%s\",\"dist\":%s," +
                            "\"ts_a\":%d,\"lat_a\":%s,\"lon_a\":%s," +
                            "\"ts_b\":%d,\"lat_b\":%s,\"lon_b\":%s}",
                    robotA, robotB, event.dist,
                    tsA, latA, lonA,
                    tsB, latB, lonB
            );

            out.collect(json);
            lastAlertTimeState.update(now);
        }
    }
}