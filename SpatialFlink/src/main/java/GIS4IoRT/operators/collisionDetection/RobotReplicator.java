package GIS4IoRT.operators.collisionDetection;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

// Handles spatial partitioning boundary conditions by replicating data points into adjacent grid cells
// if they fall within the specified overlap threshold.

public class RobotReplicator implements FlatMapFunction<Point, Point> {

    private final UniformGrid grid;
    private final double safetyMarginMeters;
    private static final double METERS_PER_DEGREE = 111139.0;

    public RobotReplicator(UniformGrid grid, double threshold) {
        this.grid = grid;
        this.safetyMarginMeters = threshold;
    }

    @Override
    public void flatMap(Point original, Collector<Point> out) throws Exception {

        double bufferDegrees = (safetyMarginMeters / METERS_PER_DEGREE) * 1.5;


        HashSet<String> targetCells = grid.getNeighboringCells(bufferDegrees, original);

        if (original.gridID != null) {
            targetCells.add(original.gridID);
        }

        for (String cellId : targetCells) {
            Point clone = new Point(original.objID, original.point.getX(), original.point.getY(), original.timeStampMillisec, grid);
            clone.gridID = cellId;
            out.collect(clone);
        }
    }
}