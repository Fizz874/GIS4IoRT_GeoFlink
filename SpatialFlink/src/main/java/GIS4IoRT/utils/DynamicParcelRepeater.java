package GIS4IoRT.utils;


import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.geojson.GeoJsonReader; // Używamy JTS Readera

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class DynamicParcelRepeater extends KeyedProcessFunction<String, String, Polygon> {

    private MapState<String, Polygon> activeZonesState;
    private ValueState<Long> nextTimerState;

    private final UniformGrid uGrid;
    private final long INTERVAL_MS = 1000;

    private transient WKBReader wkbReader;

    public DynamicParcelRepeater(UniformGrid uGrid) {
        this.uGrid = uGrid;
    }

    @Override
    public void open(Configuration parameters) {
        activeZonesState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("activeZones", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(Polygon.class))
        );
        nextTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("repeaterTimer", Long.class)
        );

        this.wkbReader = new WKBReader();
    }

    //TODO dodać CLEAR?
    @Override
    public void processElement(String command, Context ctx, Collector<Polygon> out) throws Exception {
        // Format: ZONE:AKCJA:ID:DANE

        if (command == null || !command.startsWith("ZONE")) return;

        String[] parts = command.split(":", 4);
        if (parts.length < 3) return;

        String action = parts[1].toUpperCase();
        String zoneId = parts[2];

        if ("ADD".equals(action)) {
            if (parts.length < 4) return;
            String hexWkbPayload = parts[3];

            try {
                byte[] geometryBytes = WKBReader.hexToBytes(hexWkbPayload);
                Geometry geometry = wkbReader.read(geometryBytes);

                if (geometry instanceof org.locationtech.jts.geom.Polygon) {
                    org.locationtech.jts.geom.Polygon jtsPolygon = (org.locationtech.jts.geom.Polygon) geometry;

                    List<List<Coordinate>> shape = new ArrayList<>();
                    shape.add(Arrays.asList(jtsPolygon.getExteriorRing().getCoordinates()));
                    for (int i = 0; i < jtsPolygon.getNumInteriorRing(); i++) {
                        shape.add(Arrays.asList(jtsPolygon.getInteriorRingN(i).getCoordinates()));
                    }

                    Polygon p = new Polygon(zoneId, shape, System.currentTimeMillis(), uGrid);

                    activeZonesState.put(zoneId, p);
                    System.out.println("REPEATER: Zone added/updated: " + zoneId);
                }
            } catch (Exception e) {
                System.err.println("REPEATER: Error parsing zone " + zoneId + ": " + e.getMessage());
            }
        }


        else if ("DELETE".equals(action) || "REMOVE".equals(action)) {
            activeZonesState.remove(zoneId);
            System.out.println("REPEATER: Zone removed: " + zoneId);
        }

        if (nextTimerState.value() == null) {
            registerNextTimer(ctx.timerService());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Polygon> out) throws Exception {
        long currentTime = System.currentTimeMillis();

        for (Polygon p : activeZonesState.values()) {
            out.collect(p);
        }

        registerNextTimer(ctx.timerService());
    }


    private void registerNextTimer(org.apache.flink.streaming.api.TimerService timerService) throws Exception {
        long now = timerService.currentProcessingTime();
        long nextTime = now + INTERVAL_MS;
        timerService.registerProcessingTimeTimer(nextTime);
        nextTimerState.update(nextTime);
    }
}