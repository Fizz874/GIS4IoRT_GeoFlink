package GIS4IoRT.operators.sensorProximity;


import GIS4IoRT.objects.*;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

// Manages Sensor State by joining static configuration with dynamic readings.

public class SensorManager extends KeyedCoProcessFunction<String, SensorConfig, SensorRaw, SensorPoint> {

    private ValueState<SensorConfig> configState;
    private ValueState<SensorPoint> lastSensorState;
    private final UniformGrid grid;

    public SensorManager(UniformGrid grid) {
        this.grid = grid;
    }

    @Override
    public void open(Configuration parameters) {
        configState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("sensorConfig", SensorConfig.class));
        lastSensorState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastSensor", SensorPoint.class));
    }

    @Override
    public void processElement1(SensorConfig config, Context ctx, Collector<SensorPoint> out) throws Exception {
        if (config == null) return;
        if (Objects.equals(config.command, "REMOVE")) {
            SensorPoint last = lastSensorState.value();
            if (last != null) {
                SensorPoint retraction = new SensorPoint(last, last.humidity, last.radius, last.threshold);
                retraction.gridID = last.gridID;
                retraction.isRetract = true;
                out.collect(retraction);
            }

            configState.clear();
            lastSensorState.clear();
        } else {
            configState.update(config);
        }
    }

    @Override
    public void processElement2(SensorRaw reading, Context ctx, Collector<SensorPoint> out) throws Exception {

        SensorConfig config = configState.value();
        if (config == null || Objects.equals(config.command, "REMOVE")|| config.radius <= 0.0) return;


        Point basePoint = new Point(
                reading.id,
                reading.lon, reading.lat,
                reading.timestamp,
                this.grid
        );


        SensorPoint currentSensor = new SensorPoint(basePoint, reading.humidity, config.radius, config.threshold);

        SensorPoint last = lastSensorState.value();


        if (last != null) {
            boolean gridChanged = !last.gridID.equals(currentSensor.gridID);
            boolean radiusChanged = Double.compare(last.radius, currentSensor.radius) != 0;

            if (gridChanged || radiusChanged) {
                SensorPoint retraction = new SensorPoint(last, last.humidity, last.radius, last.threshold);
                retraction.gridID = last.gridID;
                retraction.isRetract = true;
                out.collect(retraction);
            }
        }


        out.collect(currentSensor);
        lastSensorState.update(currentSensor);
    }
}