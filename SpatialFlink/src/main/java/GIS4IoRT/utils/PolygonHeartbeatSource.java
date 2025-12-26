package GIS4IoRT.utils;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.*;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.List;

public class PolygonHeartbeatSource extends RichSourceFunction<Polygon> {

    private volatile boolean isRunning = true;

    private final List<List<List<Coordinate>>> allZonesDefinitions;
    private final List<String> zoneNames;
    private final UniformGrid uGrid;
    private final long intervalMs;

    public PolygonHeartbeatSource(List<List<List<Coordinate>>> allZonesDefinitions, List<String> zoneNames, UniformGrid uGrid, long intervalMs) {
        this.allZonesDefinitions = allZonesDefinitions;
        this.zoneNames = zoneNames;
        this.uGrid = uGrid;
        this.intervalMs = intervalMs;
    }

    @Override
    public void run(SourceContext<Polygon> ctx) throws Exception {
        List<Polygon> polygonsToEmit = new ArrayList<>();

        for (int i = 0; i < allZonesDefinitions.size(); i++) {
            List<List<Coordinate>> singleZoneDef = allZonesDefinitions.get(i);

            Polygon p = new Polygon(singleZoneDef, uGrid);

            if (zoneNames != null && i < zoneNames.size()) {
                p.objID = zoneNames.get(i);
            } else {
                p.objID = "Zone_" + i;
            }

            polygonsToEmit.add(p);
        }

        while (isRunning) {
            long now = System.currentTimeMillis();
            for (Polygon p : polygonsToEmit) {
                ctx.collectWithTimestamp(p, now);
            }
            Thread.sleep(intervalMs);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}