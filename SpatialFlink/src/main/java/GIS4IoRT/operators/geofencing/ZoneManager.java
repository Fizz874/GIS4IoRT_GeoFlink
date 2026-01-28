package GIS4IoRT.operators.geofencing;

import GIS4IoRT.objects.ZoneEvent;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

// Routes zone events to grid cells
// Handles spatial replication by propagating zone events to all grid cells intersected by the zone's radius.

public class ZoneManager extends KeyedProcessFunction<String, String, ZoneEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ZoneManager.class);
    private final UniformGrid uGrid;
    private transient WKBReader wkbReader;
    private final double replicationRadius;

    private MapState<String, Polygon> activeZonesState;

    public ZoneManager(double replicationRadius,UniformGrid uGrid) {
        this.uGrid = uGrid;
        this.replicationRadius = replicationRadius;
    }

    @Override
    public void open(Configuration parameters) {
        wkbReader = new WKBReader();
        activeZonesState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("globalActiveZones", String.class, Polygon.class)
        );
    }

    @Override
    public void processElement(String command, Context ctx, Collector<ZoneEvent> out) throws Exception {
        if (command == null || !command.startsWith("ZONE")) return;

        String[] parts = command.split(":", 4);
        if (parts.length < 2) return;

        String action = parts[1].toUpperCase();


        if ("CLEAR".equals(action) || "RESET".equals(action)) {
            for (String zoneID : activeZonesState.keys()) {
                Polygon p = activeZonesState.get(zoneID);
                distributeEvent(out, p, ZoneEvent.Type.DELETE);
            }
            activeZonesState.clear();
            System.out.println("ZONE MANAGER: System Cleared");
            return;
        }

        if (parts.length < 3) return;
        String zoneId = parts[2];

        if ("ADD".equals(action)) {
            if (parts.length < 4) return;
            String hexWkbPayload = parts[3];

            try {
                byte[] geometryBytes = WKBReader.hexToBytes(hexWkbPayload);
                Geometry geometry = wkbReader.read(geometryBytes);

                org.locationtech.jts.geom.Polygon jtsPolygon = null;

                if (geometry instanceof org.locationtech.jts.geom.Polygon) {
                    jtsPolygon = (org.locationtech.jts.geom.Polygon) geometry;
                }
                else if (geometry instanceof org.locationtech.jts.geom.MultiPolygon) {
                    org.locationtech.jts.geom.MultiPolygon mp = (org.locationtech.jts.geom.MultiPolygon) geometry;
                    double maxArea = -1;

                    for (int i = 0; i < mp.getNumGeometries(); i++) {
                        if (mp.getGeometryN(i) instanceof org.locationtech.jts.geom.Polygon) {
                            org.locationtech.jts.geom.Polygon p = (org.locationtech.jts.geom.Polygon) mp.getGeometryN(i);
                            if (p.getArea() > maxArea) {
                                maxArea = p.getArea();
                                jtsPolygon = p;
                            }
                        }
                    }
                    if (jtsPolygon != null) {
                        LOG.info("DEBUG_ZONE_LOAD: Converted MultiPolygon {} to largest Polygon (Area: {}).", zoneId, maxArea);
                    }
                }

                if (jtsPolygon != null) {

                    List<List<Coordinate>> shape = new ArrayList<>();
                    shape.add(Arrays.asList(jtsPolygon.getExteriorRing().getCoordinates()));
                    for (int i = 0; i < jtsPolygon.getNumInteriorRing(); i++) {
                        shape.add(Arrays.asList(jtsPolygon.getInteriorRingN(i).getCoordinates()));
                    }

                    Polygon p = new Polygon(zoneId, shape, System.currentTimeMillis(), uGrid);

                    activeZonesState.put(zoneId, p);

                    distributeEvent(out, p, ZoneEvent.Type.ADD);

                    System.out.println("ZONE MANAGER: Added " + zoneId);
                } else {
                    LOG.warn("ZONE MANAGER: Ignored zone {} - unsupported geometry type: {}", zoneId, geometry.getGeometryType());
                }

            } catch (Exception e) {
                System.err.println("ZONE MANAGER: Error parsing zone " + zoneId + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        else if ("DELETE".equals(action) || "REMOVE".equals(action)) {
            Polygon existingZone = activeZonesState.get(zoneId);

            if (existingZone != null) {
                distributeEvent(out, existingZone, ZoneEvent.Type.DELETE);
                activeZonesState.remove(zoneId);
                System.out.println("ZONE MANAGER: Removed " + zoneId);
            } else {
                System.out.println("ZONE MANAGER: Unknown zone " + zoneId);
            }
        }
    }


    private void distributeEvent(Collector<ZoneEvent> out, Polygon p, ZoneEvent.Type type) {


        Set<String> guaranteed = uGrid.getGuaranteedNeighboringCells(replicationRadius, p);

        Set<String> candidates = uGrid.getCandidateNeighboringCells(replicationRadius, p, guaranteed);

        Set<String> allTargetCells = new HashSet<>();
        if (guaranteed != null) allTargetCells.addAll(guaranteed);
        if (candidates != null) allTargetCells.addAll(candidates);

        for (String cellID : allTargetCells) {
            if (type == ZoneEvent.Type.ADD) {
                out.collect(new ZoneEvent(p.objID, cellID, p));
            } else {
                out.collect(new ZoneEvent(type, p.objID, cellID));
            }
        }
    }
}