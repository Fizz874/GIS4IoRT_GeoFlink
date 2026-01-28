package GIS4IoRT.operators.sensorProximity;

import GIS4IoRT.objects.SensorRaw;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

// Parses dynamic environmental telemetry from the sensor stream

public class SensorParser implements FlatMapFunction<String, SensorRaw> {

    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String line, Collector<SensorRaw> out) {
        try {
            if (line == null || line.trim().isEmpty()) return;

            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }

            JsonNode node = jsonParser.readTree(line);

            String id = node.get("sensor_id").asText();
            double hum = node.get("humidity").asDouble();
            double lon = node.get("position_x").asDouble();
            double lat = node.get("position_y").asDouble();
            long timestamp = node.get("timestamp").asLong();

            out.collect(new SensorRaw(id, hum, lon, lat, timestamp));

        } catch (Exception e) {
            System.err.println("JSON Parsing error: " + e.getMessage() + " | Line: " + line);
        }
    }
}