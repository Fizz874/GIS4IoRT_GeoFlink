package GIS4IoRT.operators.sensorProximity;

import GIS4IoRT.objects.*;
import GIS4IoRT.utils.GpsDistanceFunctions;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import GeoFlink.utils.DistanceFunctions;

public class SensorGridJoinFunction extends KeyedCoProcessFunction<String, Point, SensorPoint, String> {

    private MapState<String, SensorPoint> sensorsInGrid;

    @Override
    public void open(Configuration parameters) {

        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<String, SensorPoint> desc =
                new MapStateDescriptor<>("gridSensors", String.class, SensorPoint.class);
        desc.enableTimeToLive(ttlConfig);

        sensorsInGrid = getRuntimeContext().getMapState(desc);
    }

    @Override
    public void processElement1(Point robot, Context ctx, Collector<String> out) throws Exception {
        Iterable<SensorPoint> sensors = sensorsInGrid.values();

        for (SensorPoint sensor : sensors) {
            if (sensor.humidity > sensor.threshold) {
                double dist = GpsDistanceFunctions.getDistance(robot, sensor);
                if (dist <= sensor.radius) {
                    out.collect(String.format("ALERT: Robot %s near sensor %s (Dist: %.2fm, Hum: %.1f)", //TODO change format
                            robot.objID, sensor.objID, dist, sensor.humidity));
                }
            }
        }
    }


    @Override
    public void processElement2(SensorPoint sensor, Context ctx, Collector<String> out) throws Exception {
        if (sensor.isRetract) {
            sensorsInGrid.remove(sensor.objID);
        } else {
            sensorsInGrid.put(sensor.objID, sensor);
        }
    }
}