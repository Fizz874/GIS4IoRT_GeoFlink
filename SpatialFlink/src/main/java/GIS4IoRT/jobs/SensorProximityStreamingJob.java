package GIS4IoRT.jobs;


import GIS4IoRT.objects.SensorConfig;
import GIS4IoRT.objects.SensorPoint;
import GIS4IoRT.objects.SensorRaw;
import GIS4IoRT.operators.sensorProximity.*;
import GIS4IoRT.utils.ConfigLoader;
import GIS4IoRT.operators.WhitelistGatekeeper;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import scala.Serializable;

import java.util.*;
import java.util.regex.Pattern;


public class SensorProximityStreamingJob implements Serializable {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JobConfig {
        // System Configuration
        public boolean localWebUi = true;
        public int parallelism = 1;
        public String bootStrapServers = "localhost:9092";
        public String configName = "sensorProximity";

        // Grid Configuration
        public double cellLengthMeters = 0;
        public int uniformGridSize = 100;
        public double gridMinX = 0.0;
        public double gridMinY = 0.0;
        public double gridMaxX = 0.0;
        public double gridMaxY = 0.0;

        // Kafka Configuration
        public String inputTopicName = "multi_gps_fix";     // Robot GPS data
        public String sensorTopicName = "sensor_proximity"; // Environment sensor data
        public String outputTopicName = "output_sensor";
        public String controlTopicName = "output_control";  // Config updates (WhiteList/SensorConfig)
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
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
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
        kafkaProperties.setProperty("group.id", "sensor-proximity-" + config.configName);
        kafkaProperties.setProperty("partition.discovery.interval.ms", "10000");

        // DATA SOURCES

        // Control Stream (StartFromEarliest to reconstruct state)
        FlinkKafkaConsumer<String> controlConsumer = new FlinkKafkaConsumer<>(
                Pattern.compile(config.controlTopicName), new SimpleStringSchema(), kafkaProperties);
        controlConsumer.setStartFromEarliest();

        // Robot Stream (StartFromLatest for real-time processing)
        FlinkKafkaConsumer<String> inputConsumer = new FlinkKafkaConsumer<>(
                Pattern.compile(config.inputTopicName), new SimpleStringSchema(), kafkaProperties);
        inputConsumer.setStartFromLatest();

        // Sensor Stream (StartFromLatest for real-time processing)
        FlinkKafkaConsumer<String> sensorConsumer = new FlinkKafkaConsumer<>(
                Pattern.compile(config.sensorTopicName), new SimpleStringSchema(), kafkaProperties);
        sensorConsumer.setStartFromLatest();


        DataStream<String> controlStream = env.addSource(controlConsumer).name("Control-Source");
        DataStream<String> geoInputStream = env.addSource(inputConsumer).name("Robot-Source");
        DataStream<String> rawSensorStream = env.addSource(sensorConsumer).name("Sensor-Source");


        // ROBOT PIPELINE (Deserialization + Filtering)

        // JSON Deserialization -> Point Object
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

        // Extract Robot-specific Control Commands
        DataStream<String> robotControl = controlStream
                .filter(str -> str != null && str.startsWith("ROBOT"));

        // Whitelist Gatekeeper (Broadcast State Pattern)
        // Filters out robots that are not explicitly allowed in the system
        DataStream<Point> activeRobots = spatialPointStream
                .connect(robotControl.broadcast(WhitelistGatekeeper.ALLOWED_LIST_DESC))
                .process(new WhitelistGatekeeper())
                .name("Robot-Gatekeeper");


        // SENSOR PIPELINE (State Mgmt + Spatial Replication)

        // Parse Sensor Static Configuration (Location, Radius)
        DataStream<SensorConfig> sensorConfig = controlStream
                .flatMap(new SensorConfigParser())
                .name("Config-Parser");

        // Parse Dynamic Sensor Readings (Humidity, Temp)
        DataStream<SensorRaw> rawReadings = rawSensorStream
                .flatMap(new SensorParser())
                .name("Reading-Parser");

        // State Manager (Merges Static Config with Real-time Data)
        // Ensures we have both location and current value for every sensor
        DataStream<SensorPoint> managedSensors = sensorConfig
                .keyBy(c -> c.id)
                .connect(rawReadings.keyBy(r -> r.id))
                .process(new SensorManager(uGrid))
                .name("Sensor-Manager");

        // Spatial Replication (Handles sensors overlapping grid boundaries)
        // Duplicates sensor objects to adjacent grids if the radius extends beyond current cell
        DataStream<SensorPoint> replicatedSensors = managedSensors
                .flatMap(new SensorReplicator(uGrid))
                .name("Sensor-Replicator");


        // CORE LOGIC

        // Co-Process Function to detect Robot-Sensor Proximity
        // Both streams are partitioned by GridID for local processing
        DataStream<String> alerts = activeRobots
                .keyBy(r -> r.gridID)
                .connect(replicatedSensors.keyBy(s -> s.gridID))
                .process(new SensorGridJoinFunction())
                .name("Grid-Join-Processor");


        // SINK
        alerts.addSink(createKafkaProducer(config.outputTopicName, kafkaProperties));

        env.execute("SensorProximityStreamingJob");
    }

    @SuppressWarnings("deprecation")
    private static FlinkKafkaProducer<String> createKafkaProducer(
            String topic,
            Properties kafkaProperties
    ) {
        return new FlinkKafkaProducer<>(
                topic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                kafkaProperties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
    }
}

