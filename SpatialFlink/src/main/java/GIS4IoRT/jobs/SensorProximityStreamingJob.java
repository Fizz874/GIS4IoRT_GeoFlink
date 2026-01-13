package GIS4IoRT.jobs;


import GIS4IoRT.objects.SensorConfig;
import GIS4IoRT.objects.SensorPoint;
import GIS4IoRT.objects.SensorRaw;
import GIS4IoRT.operators.sensorProximity.*;
import GIS4IoRT.utils.ConfigLoader;
import GIS4IoRT.operators.sensorProximity.WhitelistGatekeeper;
import GIS4IoRT.utils.deserialization.JsonToPointMapper;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialStreams.Deserialization;
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

import java.text.DateFormat;
import java.util.*;
import java.util.regex.Pattern;


public class SensorProximityStreamingJob implements Serializable {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JobConfig {
        //Default parameter values
        public boolean localWebUi = true;
        public int parallelism = 1;
        public String bootStrapServers = "localhost:9092";
        public String configName = "sensorProximity";
        public double cellLengthMeters = 0;
        public int uniformGridSize = 100;
        public double gridMinX = 3.430;
        public double gridMinY = 46.336;
        public double gridMaxX = 3.436;
        public double gridMaxY = 46.342;
        public String inputTopicName = "multi_gps_fix";
        public String sensorTopicName = "sensor_proximity";
        public String outputTopicName = "geofence_output";
        public String controlTopicName = "query1Control";
    }



    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = null;


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

        env.getConfig().setGlobalJobParameters(params); //Visibility of parameters in Flink Web UI
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(config.parallelism);



        UniformGrid uGrid;

        if (config.cellLengthMeters > 0) {
            uGrid = new UniformGrid(config.cellLengthMeters, config.gridMinX, config.gridMaxX, config.gridMinY, config.gridMaxY);
        } else {
            uGrid = new UniformGrid(config.uniformGridSize, config.gridMinX, config.gridMaxX, config.gridMinY, config.gridMaxY);
        }



        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", config.bootStrapServers);
        kafkaProperties.setProperty("group.id", "sensor-proximity-" + config.configName);//TODO
        kafkaProperties.setProperty("partition.discovery.interval.ms", "10000");
        //Processing


        // Kafka Consumers
        FlinkKafkaConsumer<String> controlConsumer = new FlinkKafkaConsumer<>(
                Pattern.compile(config.controlTopicName), new SimpleStringSchema(), kafkaProperties);
        controlConsumer.setStartFromEarliest();

        FlinkKafkaConsumer<String> inputConsumer = new FlinkKafkaConsumer<>(
                Pattern.compile(config.inputTopicName), new SimpleStringSchema(), kafkaProperties);
        inputConsumer.setStartFromLatest();

        FlinkKafkaConsumer<String> sensorConsumer = new FlinkKafkaConsumer<>(
                Pattern.compile(config.sensorTopicName), new SimpleStringSchema(), kafkaProperties);
        sensorConsumer.setStartFromLatest();

        // Data Streams
        DataStream<String> controlStream = env.addSource(controlConsumer).name("Control-Source");
        DataStream<String> geoInputStream = env.addSource(inputConsumer).name("Robot-Source");
        DataStream<String> rawSensorStream = env.addSource(sensorConsumer).name("Sensor-Source");


        // --- 2. Robot Pipeline (Stream + Broadcast Gatekeeper) ---
        // Deserialization of trajectory data
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
        // Filter control commands specific to robots
        DataStream<String> robotControl = controlStream
                .filter(str -> str != null && str.startsWith("ROBOT"));

        // Apply Whitelist Gatekeeper (Broadcast State Pattern)
        DataStream<Point> activeRobots = spatialPointStream
                .connect(robotControl.broadcast(WhitelistGatekeeper.ALLOWED_LIST_DESC))
                .process(new WhitelistGatekeeper())
                .name("Robot-Gatekeeper");


        // --- 3. Sensor Pipeline (State Management + Spatial Replication) ---
        // Parse Configuration and Telemetry
        DataStream<SensorConfig> sensorConfig = controlStream
                .flatMap(new SensorConfigParser())
                .name("Config-Parser");

        DataStream<SensorRaw> rawReadings = rawSensorStream
                .flatMap(new SensorParser())
                .name("Reading-Parser");

        // State Management: Handle Config + Reading + Motion Detection
        DataStream<SensorPoint> managedSensors = sensorConfig
                .keyBy(c -> c.id)
                .connect(rawReadings.keyBy(r -> r.id))
                .process(new SensorManager(uGrid))
                .name("Sensor-Manager");

        // Spatial Replication: Handle sensors overlapping grid boundaries
        DataStream<SensorPoint> replicatedSensors = managedSensors
                .flatMap(new SensorReplicator(uGrid))
                .name("Sensor-Replicator");


        // --- 4. Spatial Join & Alert Generation ---
        // Shuffle by GridID -> Detect Proximity
        DataStream<String> alerts = activeRobots
                .keyBy(r -> r.gridID)
                .connect(replicatedSensors.keyBy(s -> s.gridID))
                .process(new SensorGridJoinFunction())
                .name("Grid-Join-Processor");


        // --- 5. Execution ---
        alerts.print().name("Alert-Sink"); //TODO comment out
        alerts.addSink(createKafkaProducer(config.outputTopicName, kafkaProperties));





//        DataStream<String> controlStream = env.addSource(new FlinkKafkaConsumer<>(config.controlTopicName, new SimpleStringSchema(), kafkaProperties).setStartFromEarliest());
//
//        DataStream<String> geoInputStream = env.addSource(new FlinkKafkaConsumer<>(config.inputTopicName, new SimpleStringSchema(), kafkaProperties)
//                .setStartFromLatest());
//
//        DataStream<String> rawSensorStream = env.addSource(new FlinkKafkaConsumer<>(config.sensorTopicName, new SimpleStringSchema(), kafkaProperties)
//                .setStartFromLatest());
//
//        DataStream<SensorRaw> rawReadings = rawSensorStream.flatMap(new SensorParser());
//
//
//        DataStream<String> robotControl = controlStream
//                .filter(str -> str.startsWith("ROBOT"));
//
//        DataStream<SensorConfig> sensorConfig = controlStream
//                .flatMap(new SensorConfigParser());
//
//        DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoInputStream, inputType, inputDateFormat, inputDelimiter, csvTsvSchemaAttr, "timestamp", "oID", uGrid);
//
//
//
//        DataStream<Point> filteredPoints = spatialPointStream
//                .connect(robotControl.broadcast(WhitelistGatekeeper.ALLOWED_LIST_DESC))
//                .process(new WhitelistGatekeeper());
//
//
//        DataStream<SensorPoint> managedSensors = sensorConfig
//                .keyBy(c -> c.id) // Kluczujemy po ID
//                .connect(rawReadings.keyBy(r -> r.id))
//                .process(new SensorManager(uGrid));
//
//
//        DataStream<SensorPoint> replicatedSensors = managedSensors
//                .flatMap(new SensorReplicator(uGrid));
//
//        DataStream<String> alerts = filteredPoints
//                .keyBy(r -> r.gridID)
//                .connect(replicatedSensors.keyBy(s -> s.gridID))
//                .process(new SensorGridJoinFunction());
//



//        joinResult
//                .keyBy((KeySelector<Tuple2<AssignedPoint, Polygon>, String>) value -> value.f0.objID)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
//                .process(new ProcessWindowFunction<Tuple2<AssignedPoint, Polygon>, String, String, TimeWindow>() {
//                    @Override
//                    public void process(String key, Context context, Iterable<Tuple2<AssignedPoint, Polygon>> elements, Collector<String> out) {
//                        long windowStart = context.window().getStart(); // window start in ms
//                        long windowEnd = context.window().getEnd();   // window end in ms
//
//                        String timeInfo = String.format("Window [%tT - %tT]: ", windowStart, windowEnd);
//                        if (elements.iterator().hasNext()) {
//                            //String message = String.format("%s Object ID: %s -> TRUE (Outside of the zone)", timeInfo, key);
//                            String message = String.format(
//                                    "{\"window_start\": \"%s\", \"window_end\": \"%s\", \"object_id\": \"%s\", \"message\": \"%s\"}",
//                                    windowStart, windowEnd, key, "Outside of the zone"
//                            );
//                            out.collect(message);
//                            //out.collect(timeInfo + "TRUE");
//                        }
//
//                    }
//                })
//                //.print();
//                .addSink(createKafkaProducer(config.outputTopicName, kafkaProperties));



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

