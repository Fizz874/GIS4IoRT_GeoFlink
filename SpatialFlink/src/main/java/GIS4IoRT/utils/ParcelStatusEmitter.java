package GIS4IoRT.utils;

import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class ParcelStatusEmitter extends KeyedProcessFunction<String, Tuple2<Point, Polygon>, String> {

    private ValueState<Long> lastSeenTimestamp;
    private ValueState<Long> nextTimerTimestamp;

    private final long TOLERANCE_MS = 1200;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastSeenTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastSeen", Long.class));
        nextTimerTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("nextTimer", Long.class));
    }

    @Override
    public void processElement(Tuple2<Point, Polygon> value, Context ctx, Collector<String> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        lastSeenTimestamp.update(now);

        ensureTimerRunning(ctx);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        Long lastSeen = lastSeenTimestamp.value();

        boolean isOut = false;

        if (lastSeen == null) {
            //isOut = true;
        } else {
            long diff = now - lastSeen;
            if (diff > TOLERANCE_MS) {
                isOut = true;
            } else {
                isOut = false;
            }
        }

        if (isOut) {
            String robotID = ctx.getCurrentKey();
            out.collect("Time: " + now + " | Robot: " + robotID + " | IsOut: " + isOut);
        }
        long nextTime = timestamp + 1000;
        ctx.timerService().registerProcessingTimeTimer(nextTime);
        nextTimerTimestamp.update(nextTime);
    }

    private void ensureTimerRunning(Context ctx) throws Exception {
        if (nextTimerTimestamp.value() == null) {
            long now = ctx.timerService().currentProcessingTime();

            long remainder = now % 1000;
            long nextTime = now - remainder + 1200;

            if (nextTime <= now) {
                nextTime += 1000;
            }

            ctx.timerService().registerProcessingTimeTimer(nextTime);
            nextTimerTimestamp.update(nextTime);
        }
    }
}