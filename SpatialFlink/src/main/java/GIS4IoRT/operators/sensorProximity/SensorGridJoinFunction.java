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

import java.util.Locale;

// Executes the core proximity detection logic by joining Robot and Sensor streams on GridID.
// Calculates Euclidean distance locally within each partition to detect proximity events.

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
                    String json = String.format(Locale.US,
                            "{\"type\":\"sensor_proximity\",\"robot\":\"%s\",\"sensor\":\"%s\",\"dist\":%s,\"hum\":%s," +
                                    "\"ts_r\":%d,\"lat_r\":%s,\"lon_r\":%s," +
                                    "\"ts_s\":%d,\"lat_s\":%s,\"lon_s\":%s}",
                            robot.objID,
                            sensor.objID,
                            dist,
                            sensor.humidity,
                            robot.timeStampMillisec,
                            robot.point.getY(),
                            robot.point.getX(),
                            sensor.timeStampMillisec,
                            sensor.point.getY(),
                            sensor.point.getX()
                    );
                    out.collect(json);
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