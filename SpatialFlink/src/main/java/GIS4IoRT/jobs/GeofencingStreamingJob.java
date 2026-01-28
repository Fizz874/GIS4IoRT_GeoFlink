package GIS4IoRT.jobs;

import GIS4IoRT.objects.AssignedPoint;
import GIS4IoRT.objects.ZoneEvent;
import GIS4IoRT.operators.geofencing.SessionBroadcastManager;
import GIS4IoRT.operators.geofencing.ZoneGridJoinFunction;
import GIS4IoRT.operators.geofencing.ZoneManager;
import GIS4IoRT.utils.ConfigLoader;
import GIS4IoRT.utils.deserialization.JsonToPointMapper;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Properties;
import java.util.regex.Pattern;

public class GeofencingStreamingJob implements Serializable {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JobConfig {
        // System Configuration
        public boolean localWebUi = true;
        public int parallelism = 5;
        public String bootStrapServers = "localhost:9092";
        public String configName = "geo";


        // Grid Configuration
        public double cellLengthMeters = 0;
        public int uniformGridSize = 100;
        public double gridMinX = 0.0;
        public double gridMinY = 0.0;
        public double gridMaxX = 0.0;
        public double gridMaxY = 0.0;

        // Kafka Configuration
        public String inputTopicName = "test_input";
        public String outputTopicName = "geofence_output";
        public String controlTopicName = "control_geo";

        // Spatial Tolerance
        public double radius = 0.00000000001;
    }

    public static void main(String[] args) throws Exception {

        // SETUP AND CONFIGURATION
        StreamExecutionEnvironment env;
        JobConfig config = new JobConfig();
        ParameterTool params = ConfigLoader.load(args, config);

        if (config.localWebUi) {
            Configuration localConfig = new Configuration();
            localConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
            localConfig.setString(RestOptions.BIND_PORT, "8082");
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(localConfig);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(config.parallelism);
        env.setBufferTimeout(10);
        // Grid Setup
        UniformGrid uGrid;
        if (config.cellLengthMeters > 0) {
            uGrid = new UniformGrid(config.cellLengthMeters, config.gridMinX, config.gridMaxX, config.gridMinY, config.gridMaxY);
        } else {
            uGrid = new UniformGrid(config.uniformGridSize, config.gridMinX, config.gridMaxX, config.gridMinY, config.gridMaxY);
        }

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", config.bootStrapServers);
        kafkaProperties.setProperty("group.id", "geofencing-"+config.configName);
        kafkaProperties.setProperty("partition.discovery.interval.ms", "10000");

        // DATA SOURCES

        // Control Stream (StartFromEarliest to restore Zone/Assignment state)
        DataStream<String> controlStream = env.addSource(
                new FlinkKafkaConsumer<>(Pattern.compile(config.controlTopicName), new SimpleStringSchema(), kafkaProperties)
                        .setStartFromEarliest()
        ).name("Control-Source");

        // Robot Stream (StartFromLatest for real-time processing)
        DataStream<String> geoInputStream = env.addSource(
                new FlinkKafkaConsumer<>(Pattern.compile(config.inputTopicName), new SimpleStringSchema(), kafkaProperties)
                        .setStartFromLatest()
        ).name("Robot-Source");


        // ROBOT PIPELINE (Enrichment)

        DataStream<Point> spatialPointStream = geoInputStream
                .map(new JsonToPointMapper(
                        uGrid,
                        "/id",
                        "/ts",
                        null,
                        "/lat",
                        "/lon",
                        false
                ))
                .filter(p -> p != null)
                .name("JSON-Deserializer");


        DataStream<String> robotCommands = controlStream
                .filter(str -> str != null && str.startsWith("ROBOT"));

        // Session Manager (Blocks unknown robots and assigns zones to allowed ones)
        DataStream<AssignedPoint> enrichedStream = spatialPointStream
                .connect(robotCommands.broadcast(SessionBroadcastManager.ASSIGNMENT_STATE_DESC))
                .process(new SessionBroadcastManager())
                .name("Session-Manager");

        // ZONE PIPELINE (Routing)

        DataStream<String> zoneCommands = controlStream
                .filter(str -> str != null && str.startsWith("ZONE"));

        // Zone Manager (Routes zone events to grid cells)
        // Uses 'radius' to determine overlapping + neighbor cells
        DataStream<ZoneEvent> zoneEvents = zoneCommands
                .keyBy(cmd -> "ZONE_MANAGER_GLOBAL")
                .process(new ZoneManager(config.radius, uGrid))
                .name("Zone-Manager");


        // CORE LOGIC (Join)

        // Spatial Join (Checks distance(robot, zone) <= radius)
        DataStream<AssignedPoint> alerts = enrichedStream
                .keyBy(r -> r.gridID)
                .connect(zoneEvents.keyBy(e -> e.gridID))
                .process(new ZoneGridJoinFunction(config.radius))
                .name("Zone-Grid-Join");

        DataStream<String> throttledAlerts = alerts
                .keyBy(point -> point.objID)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1))) // Okno 1s
                .reduce(
                (r1, r2) -> r2,new ProcessWindowFunction<AssignedPoint, String, String, TimeWindow>() {
                    @Override
                    public void process(String robotID, Context context, Iterable<AssignedPoint> elements, Collector<String> out) {


                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        String jsonMessage = String.format(
                                "{\"type\":\"outside_zone\",\"window_start\":%d,\"window_end\":%d,\"object_id\":\"%s\"}",
                                start, end, robotID
                        );

                        out.collect(jsonMessage);
                    }
                })
                .name("Alert-1s-Aggregator");

        // SINK

        throttledAlerts.addSink(createKafkaProducer(config.outputTopicName, kafkaProperties))
                .name("Alert-Sink");

//        // BENCHMARK SINK
//        DataStream<String> jsonAlerts = alerts
//                .map(ap -> {
//                    String zonesStr = (ap.assignedZoneIDs != null && !ap.assignedZoneIDs.isEmpty())
//                            ? String.join("|", ap.assignedZoneIDs)
//                            : "NONE";
//
//                    return String.format(Locale.US,
//                            "{\"type\":\"geofence\",\"robot\":\"%s\",\"ts\":%d,\"lat\":%s,\"lon\":%s,\"msg\":\"OUTSIDE\",\"zones\":\"%s\"}",
//                            ap.objID,
//                            ap.timeStampMillisec,
//                            ap.point.getY(),
//                            ap.point.getX(),
//                            zonesStr
//                    );
//                })
//                .returns(Types.STRING)
//                .name("Full-JSON-Serializer");
//
//
//        jsonAlerts.addSink(createKafkaProducer(config.outputTopicName, kafkaProperties));

        env.execute("GeofencingStreamingJob");
    }

    @SuppressWarnings("deprecation")
    private static FlinkKafkaProducer<String> createKafkaProducer(String topic, Properties kafkaProperties) {
        return new FlinkKafkaProducer<>(
                topic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                kafkaProperties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
    }
}