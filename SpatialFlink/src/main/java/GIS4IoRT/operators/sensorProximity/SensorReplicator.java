package GIS4IoRT.operators.sensorProximity;


import GIS4IoRT.objects.SensorPoint;
import GeoFlink.spatialIndices.UniformGrid;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.HashSet;

// Handles spatial boundary conditions by replicating sensor objects into adjacent grid cells.
// Ensures that sensors overlapping multiple partitions are visible to robots in all affected areas.

public class SensorReplicator implements FlatMapFunction<SensorPoint, SensorPoint> {

    private final UniformGrid grid;
    private static final double METERS_PER_DEGREE = 111139.0;


    public SensorReplicator(UniformGrid grid) {
        this.grid = grid;
    }

    @Override
    public void flatMap(SensorPoint original, Collector<SensorPoint> out) throws Exception {

        double radiusMeters = original.radius;
        double radiusDegrees = (radiusMeters / METERS_PER_DEGREE) * 1.5;
        HashSet<String> neighbors = grid.getNeighboringCells(radiusDegrees, original);

        if (original.gridID != null) {
            neighbors.add(original.gridID);
        }

        for (String targetGridID : neighbors) {

            SensorPoint clone = new SensorPoint(
                    original,
                    original.humidity,
                    original.radius,
                    original.threshold
            );

            clone.gridID = targetGridID;

            clone.isRetract = original.isRetract;

            out.collect(clone);
        }
    }
}