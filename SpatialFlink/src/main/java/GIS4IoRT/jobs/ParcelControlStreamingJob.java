package GIS4IoRT.jobs;


import GIS4IoRT.utils.ConfigLoader;
import GIS4IoRT.utils.DynamicParcelRepeater;
import GIS4IoRT.utils.WhitelistGatekeeper;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.spatialOperators.join.PointPolygonJoinQuery;
import GeoFlink.spatialStreams.Deserialization;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
import scala.Serializable;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.util.*;


//a reiteration of ParcelStreamingJob but this time with custom parameter handling
public class ParcelControlStreamingJob implements Serializable {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JobConfig {
        //Default parameter values
        public boolean localWebUi = false;
        public int parallelism = 1;
        public String bootStrapServers = "localhost:9092";

        public double cellLengthMeters = 0;
        public int uniformGridSize = 100;
        public double gridMinX = 3.430;
        public double gridMinY = 46.336;
        public double gridMaxX = 3.436;
        public double gridMaxY = 46.342;
        public String inputTopicName = "multi_gps_fix";
        public String outputTopicName = "geofence_output";
        public String controlTopicName = "query1Control";
        public boolean approximateQuery = false;
        public int omegaDuration = 1;
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





        DateFormat inputDateFormat = null;
        String inputType = "CSV";
        String inputDelimiter = ",";
        List<Integer> csvTsvSchemaAttr = Arrays.asList(0, 1, 2, 3);


        UniformGrid uGrid;

        if (config.cellLengthMeters > 0) {
            uGrid = new UniformGrid(config.cellLengthMeters, config.gridMinX, config.gridMaxX, config.gridMinY, config.gridMaxY);
        } else {
            uGrid = new UniformGrid(config.uniformGridSize, config.gridMinX, config.gridMaxX, config.gridMinY, config.gridMaxY);
        }


        QueryConfiguration realtimeConf = new QueryConfiguration(QueryType.RealTime);
        realtimeConf.setApproximateQuery(config.approximateQuery);
        realtimeConf.setWindowSize(config.omegaDuration);


        Properties kafkaProperties = new Properties();
        //kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("bootstrap.servers", config.bootStrapServers);
        kafkaProperties.setProperty("group.id", "messageStream");


        //Processing
        DataStream<String> controlStream = env.addSource(new FlinkKafkaConsumer<>(config.controlTopicName, new SimpleStringSchema(), kafkaProperties).setStartFromEarliest());

        DataStream<String> robotStream = controlStream
                .filter(str -> str.startsWith("ROBOT"));

        DataStream<String> zoneStream = controlStream
                .filter(str -> str.startsWith("ZONE"));


        DataStream<Polygon> polygonStream = zoneStream
                .keyBy(cmd -> "ZONE_MANAGER")
                .process(new DynamicParcelRepeater(uGrid));


        DataStream<String> geoJSONStream = env.addSource(new FlinkKafkaConsumer<>(config.inputTopicName, new SimpleStringSchema(), kafkaProperties)
                .setStartFromLatest());

        DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputType, inputDateFormat, inputDelimiter, csvTsvSchemaAttr, "timestamp", "oID", uGrid);


        DataStream<Point> filteredPoints = spatialPointStream
                .connect(robotStream.broadcast(WhitelistGatekeeper.ALLOWED_LIST_DESC))
                .process(new WhitelistGatekeeper());


        PointPolygonJoinQuery joinQuery = new PointPolygonJoinQuery(realtimeConf, uGrid, uGrid);
        //TODO change back to new PointPolygonOutsideJoinQuery(realtimeConf, uGrid, uGrid);
        DataStream<Tuple2<Point, Polygon>> joinResult = joinQuery.run(
                filteredPoints,
                polygonStream,
                0.0000001
        );


        joinResult
                .keyBy((KeySelector<Tuple2<Point, Polygon>, String>) value -> value.f0.objID)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new ProcessWindowFunction<Tuple2<Point, Polygon>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<Point, Polygon>> elements, Collector<String> out) {
                        long windowStart = context.window().getStart(); // window start in ms
                        long windowEnd = context.window().getEnd();   // window end in ms

                        String timeInfo = String.format("Window [%tT - %tT]: ", windowStart, windowEnd);
                        if (elements.iterator().hasNext()) {
                            //String message = String.format("%s Object ID: %s -> TRUE (Outside of the zone)", timeInfo, key);
                            String message = String.format(
                                    "{\"window_start\": \"%s\", \"window_end\": \"%s\", \"object_id\": \"%s\", \"message\": \"%s\"}",
                                    windowStart, windowEnd, key, "Outside of the zone"
                            );
                            out.collect(message);
                            //out.collect(timeInfo + "TRUE");
                        }

                    }
                })
                .addSink(createKafkaProducer(config.outputTopicName, kafkaProperties));



        env.execute("ParcelControlStreamingJob");

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
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

}

