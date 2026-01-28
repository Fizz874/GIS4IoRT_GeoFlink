package GIS4IoRT.jobs;

import GIS4IoRT.objects.CollisionEvent;
import GIS4IoRT.operators.collisionDetection.CollisionDeduplicator;
import GIS4IoRT.operators.collisionDetection.RobotReplicator;
import GIS4IoRT.operators.collisionDetection.StatefulCollisionDetector;
import GIS4IoRT.operators.WhitelistGatekeeper;
import GIS4IoRT.utils.ConfigLoader;
import GIS4IoRT.utils.deserialization.JsonToPointMapper;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;

import java.util.regex.Pattern;
import java.io.Serializable;
import java.util.*;

public class CollisionDetectionStreamingJob implements Serializable {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JobConfig {
        // System Configuration
        public boolean localWebUi = true;
        public int parallelism = 3;
        public String bootStrapServers = "localhost:9092";
        public String configName = "collision";

        // Grid Configuration
        public double cellLengthMeters = 0;
        public int uniformGridSize = 100;
        public double gridMinX = 0.0;
        public double gridMinY = 0.0;
        public double gridMaxX = 0.0;
        public double gridMaxY = 0.0;

        // Kafka Configuration
        public String inputTopicName = "test_input";
        public String outputTopicName = "output_collision";
        public String controlTopicName = "control_collsion";

        // Logic Parameters
        public double collisionThreshold = 1.5;
        public long robotStateTtlMillis = 5000;
        public long robotAlertCooldownMillis = 5000;
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

        // Initialize Spatial Grid
        UniformGrid uGrid;
        if (config.cellLengthMeters > 0) {
            uGrid = new UniformGrid(config.cellLengthMeters, config.gridMinX, config.gridMaxX, config.gridMinY, config.gridMaxY);
        } else {
            uGrid = new UniformGrid(config.uniformGridSize, config.gridMinX, config.gridMaxX, config.gridMinY, config.gridMaxY);
        }

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", config.bootStrapServers);
        kafkaProperties.setProperty("group.id", "collision-detection-" + config.configName);
        kafkaProperties.setProperty("partition.discovery.interval.ms", "10000"); // Dynamic partition discovery

        // DATA SOURCES

        // Control Stream (StartFromEarliest to rebuild the whitelist state)
        DataStream<String> controlStream = env.addSource(
                new FlinkKafkaConsumer<>(Pattern.compile(config.controlTopicName), new SimpleStringSchema(), kafkaProperties)
                        .setStartFromEarliest()
        ).name("Control-Source");

        // Robot Stream (StartFromLatest for real-time processing)
        DataStream<String> geoInputStream = env.addSource(
                new FlinkKafkaConsumer<>(Pattern.compile(config.inputTopicName), new SimpleStringSchema(), kafkaProperties)
                        .setStartFromLatest()
        ).name("Robot-Source");

        // PROCESSING PIPELINE

        // JSON Deserialization -> Point
        DataStream<Point> spatialPointStream = geoInputStream
                .map(new JsonToPointMapper(uGrid, "/id", "/ts", null, "/lat", "/lon", false))
                .filter(Objects::nonNull)
                .name("JSON-Deserializer");

        // Robot Filtering (Whitelist Gatekeeper)
        DataStream<Point> activeRobots = spatialPointStream
                .connect(controlStream.broadcast(WhitelistGatekeeper.ALLOWED_LIST_DESC))
                .process(new WhitelistGatekeeper())
                .name("Robot-Gatekeeper");

        // Spatial Replication (For robots too close to another grid)
        DataStream<Point> replicatedRobots = activeRobots
                .flatMap(new RobotReplicator(uGrid, config.collisionThreshold))
                .name("Robot-Replicator");

        // Collision Detection (State-based, window-less logic)
        // Result: Stream of CollisionEvent objects
        DataStream<CollisionEvent> rawCollisions = replicatedRobots
                .keyBy(p -> p.gridID)
                .process(new StatefulCollisionDetector(
                        config.collisionThreshold,
                        config.robotStateTtlMillis,
                        uGrid
                ))
                .name("Stateful-Collision-Detector");

        // Alert Cool-down
        // Result: Formatted JSON String
        DataStream<String> finalAlerts = rawCollisions
                .keyBy(event -> event.pairKey) // Key by unique robot pair
                .process(new CollisionDeduplicator(config.robotAlertCooldownMillis))
                .name("Collision-Deduplicator");

        finalAlerts.print();

        // SINK
        finalAlerts.addSink(createKafkaProducer(config.outputTopicName, kafkaProperties))
                .name("Alert-Sink");

        env.execute("CollisionDetectionStreamingJob");
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


