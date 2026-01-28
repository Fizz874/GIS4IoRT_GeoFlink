package GIS4IoRT.utils.deserialization;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

//Deserializes raw JSON telemetry into domain-specific Point objects
public class JsonToPointMapper implements MapFunction<String, Point> {

    private final UniformGrid uGrid;
    private final String idPath;
    private final String timestampPath;
    private final String nanosecPath;
    private final String latPath;
    private final String lonPath;
    private final boolean isTimeInSeconds;

    private transient ObjectMapper objectMapper;


    public JsonToPointMapper(UniformGrid uGrid,
                             String idPath,
                             String timestampPath,
                             String nanosecPath,
                             String latPath,
                             String lonPath,
                             boolean isTimeInSeconds) {
        this.uGrid = uGrid;
        this.idPath = idPath;
        this.timestampPath = timestampPath;
        this.nanosecPath = nanosecPath;
        this.latPath = latPath;
        this.lonPath = lonPath;
        this.isTimeInSeconds = isTimeInSeconds;
    }

    @Override
    public Point map(String jsonString) {
        try {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            JsonNode root = objectMapper.readTree(jsonString);

            JsonNode idNode = root.at(idPath);
            if (idNode.isMissingNode()) {
                System.err.println("MISSING ID at '" + idPath + "' in: " + jsonString);
                return null;
            }
            String oID = idNode.asText();

            JsonNode latNode = root.at(latPath);
            JsonNode lonNode = root.at(lonPath);

            if (latNode.isMissingNode() || lonNode.isMissingNode()) {
                System.err.println("MISSING COORDS in: " + jsonString);
                return null;
            }
            double lat = latNode.asDouble();
            double lon = lonNode.asDouble();

            long timestamp;
            JsonNode timeNode = root.at(timestampPath);

            if (timeNode.isMissingNode()) {
                timestamp = System.currentTimeMillis();
            } else {
                long rawTime = timeNode.asLong();
                timestamp = isTimeInSeconds ? rawTime * 1000 : rawTime;

                if (nanosecPath != null) {
                    JsonNode nanoNode = root.at(nanosecPath);
                    if (!nanoNode.isMissingNode()) {
                        long nanos = nanoNode.asLong();
                        timestamp += (nanos / 1_000_000);
                    }
                }
            }
            return new Point(oID, lon, lat, timestamp, uGrid);

        } catch (Exception e) {
            System.err.println("JSON PARSE ERROR: " + jsonString);
            e.printStackTrace();
            return null;
        }
    }
}