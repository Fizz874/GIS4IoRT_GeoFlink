package GIS4IoRT.operators.sensorProximity;

import GIS4IoRT.objects.SensorConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

// Parses static sensor metadata (ID, Location, Radius) from the control stream.

public class SensorConfigParser implements FlatMapFunction<String, SensorConfig> {

    @Override
    public void flatMap(String line, Collector<SensorConfig> out) {
        try {
            if (line == null || !line.startsWith("SENSOR")) {
                return;
            }

            String[] parts = line.split(":");


            if (parts.length < 3) return;

            String commandStr = parts[1].trim().toUpperCase();
            String id = parts[2].trim();

            if ("REMOVE".equals(commandStr)) {
                // SENSOR:REMOVE:ID
                out.collect(new SensorConfig(id, 0.0, 0.0, "REMOVE"));
            }
            else if ("ADD".equals(commandStr) || "UPDATE".equals(commandStr)) {
                // SENSOR:ADD:ID:RADIUS:THRESHOLD
                if (parts.length < 5) {
                    System.err.println("Invalid ADD config format: " + line);
                    return;
                }

                double radius = Double.parseDouble(parts[3]);
                double threshold = Double.parseDouble(parts[4]);

                out.collect(new SensorConfig(id, radius, threshold, "ADD"));
            }
            else {
                try {
                    String oldFormatId = parts[1];
                    double radius = Double.parseDouble(parts[2]);
                    double threshold = Double.parseDouble(parts[3]);

                    out.collect(new SensorConfig(oldFormatId, radius, threshold,"ADD"));
                } catch (Exception e) {
                    System.err.println("Unknown command or format: " + line);
                }
            }

        } catch (Exception e) {
            System.err.println("Parsing error: " + line + " -> " + e.getMessage());
        }
    }
}