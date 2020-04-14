package ingestion;

import ingestion.util.serdes.JsonPOJODeserializer;
import ingestion.util.serdes.JsonPOJOSerializer;
import model.AggregateTuple;
import model.TemperatureReading;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import querying.QueryingService;
import querying.util.TSExtractor;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamsAggregator {
    public static final String READINGS_TOPIC = System.getenv("READINGS_TOPIC") != null ? System.getenv("READINGS_TOPIC") : "temperature-readings";
    public static final String APP_NAME = System.getenv("APP_NAME") != null ? System.getenv("APP_NAME") : "temperature-aggregator";
    public static final String KBROKERS = System.getenv("KBROKERS") != null ? System.getenv("KBROKERS") : "localhost:9092";
    public static final String REST_ENDPOINT_HOSTNAME = System.getenv("REST_ENDPOINT_HOSTNAME") != null ? System.getenv("REST_ENDPOINT_HOSTNAME") : "localhost";
    public static final int REST_ENDPOINT_PORT = System.getenv("REST_ENDPOINT_PORT") != null ? Integer.parseInt(System.getenv("REST_ENDPOINT_PORT")) : 7070;
    public static final Integer GEOHASH_PRECISION = System.getenv("GEOHASH_PRECISION") != null ? Integer.parseInt(System.getenv("GEOHASH_PRECISION")) : 6;
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd:HHmmss:SSS");

    private static AggregateTuple temperatureAggregator(String key,
                                                        TemperatureReading value,
                                                        AggregateTuple aggregate) {
        aggregate.key = key;
        // aggregate.geohash = key; //.split("#")[0];
        // aggregate.timestamp = LocalDateTime.parse(key.split("#")[1], DATE_TIME_FORMATTER)
        //        .toInstant(ZoneId.systemDefault().getRules().getOffset(Instant.now())).toEpochMilli(); // back to milliseconds timestamp
        aggregate.count = aggregate.count + 1; // increment by 1 the current record count
        aggregate.sum = aggregate.sum + (Double) value.getTempVal(); // add the incoming value to the current sum
        aggregate.avg = aggregate.sum / aggregate.count; // update the average
        return aggregate;
    }

    private static Topology buildTopology(String readingsTopic, Integer geohashPrecision) {
        final StreamsBuilder builder = new StreamsBuilder();
        // Set up Serializers and Deserializers
        Map <String, Object> serdeProps = new HashMap<>();
        final Serializer < TemperatureReading > tempSerializer = new JsonPOJOSerializer<> ();
        serdeProps.put("JsonPOJOClass", TemperatureReading.class);
        tempSerializer.configure(serdeProps, false);

        final Deserializer <TemperatureReading> tempDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", TemperatureReading.class);
        tempDeserializer.configure(serdeProps, false);

        final Serde <TemperatureReading> temperatureSerde = Serdes.serdeFrom(tempSerializer, tempDeserializer);

        serdeProps = new HashMap<>();
        final Serializer <AggregateTuple> aggSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", AggregateTuple.class);
        aggSerializer.configure(serdeProps, false);

        final Deserializer <AggregateTuple> aggDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", AggregateTuple.class);
        aggDeserializer.configure(serdeProps, false);

        final Serde <AggregateTuple> aggregateTupleSerde = Serdes.serdeFrom(aggSerializer, aggDeserializer);

        // Create an stream from the 'temperature-readings' Kafka topic
        KStream <String, TemperatureReading> sourceStream = builder.stream(readingsTopic,
                Consumed.with(Serdes.String(), temperatureSerde));

        // Create a per-hour KGroupedStream
        KGroupedStream <String, TemperatureReading> perHourKeyedStream = sourceStream.selectKey(
                (sensorId, reading) -> {
                    ZonedDateTime readingDate = ZonedDateTime.ofInstant(
                            Instant.ofEpochMilli(reading.getTimestamp()),
                            ZoneId.systemDefault());

                    // Get 'Date:Time:Millis' formatted timestamp string truncated to the exact hour
                    // String hourTimestamp = readingDate.truncatedTo(ChronoUnit.HOURS)
                    //        .toLocalDateTime().format(DATE_TIME_FORMATTER);

                    // Return the new truncated key (<6 char geohash-prefix>#<hour-truncated timestamp>)
                    return reading.getGeohash().substring(0, geohashPrecision); // + "#" + hourTimestamp;
                }
        ).groupByKey();

        KTable <Windowed<String>, AggregateTuple> perHourAggregate = perHourKeyedStream
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .aggregate(
                    () -> new AggregateTuple("", "", 0L, 0L, 0.0, 0.0), // Lambda expression for the Initializer
                    (key, value, aggregate) -> temperatureAggregator(key, value, aggregate), // Lambda expression for the Aggregator
                    Materialized. <String, AggregateTuple, WindowStore<Bytes, byte[]>> as(
                            "view-gh" + geohashPrecision + "-hour").withValueSerde(aggregateTupleSerde).withCachingEnabled()
                );

        Topology topology = builder.build();
        System.out.println(topology.describe());
        return topology;
    }

    private static Properties streamsConfig(final String stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KBROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, REST_ENDPOINT_HOSTNAME + ":" + REST_ENDPOINT_PORT);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TSExtractor.class);
        return props;
    }

    private static QueryingService startRestProxy(final KafkaStreams streams, final HostInfo hostInfo)
            throws Exception {
        final QueryingService
                interactiveQueriesRestService = new QueryingService(streams, hostInfo);
        interactiveQueriesRestService.start();
        return interactiveQueriesRestService;
    }

    public static void main(String[] args) throws Exception {
        String endpointHost = null;
        String readingsTopic = null;
        int endpointPort = 0;
        int geohashPrecision = 6;
        boolean cleanup = false;

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption( "t", "readings-topic", true, "Topic the temperature values are being registered to. Defaults to '" + READINGS_TOPIC + "'");
        options.addOption( "gh", "geohash-precision", true, "Geohash precision used to perform the continuous aggregation. Defaults to " + GEOHASH_PRECISION);
        options.addOption( "h", "endpoint-host", true, "REST endpoint hostname. Defaults to " + REST_ENDPOINT_HOSTNAME);
        options.addOption( "p", "endpoint-port", true, "REST endpoint port. Defaults to " + REST_ENDPOINT_PORT);
        options.addOption( "cl", "cleanup", false, "Should a cleanup be performed before staring. Defaults to false" );

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            if( line.hasOption( "readings-topic" ) ) {
                readingsTopic = line.getOptionValue("readings-topic");
            } else {
                readingsTopic = READINGS_TOPIC;
            }
            if( line.hasOption( "geohash-precision" ) ) {
                geohashPrecision = Integer.parseInt(line.getOptionValue("geohash-precision"));
            } else {
                geohashPrecision = GEOHASH_PRECISION;
            }
            if( line.hasOption( "endpoint-host" ) ) {
                endpointHost = line.getOptionValue("endpoint-host");
            } else {
                endpointHost = REST_ENDPOINT_HOSTNAME;
            }
            if( line.hasOption( "endpoint-host" ) ) {
                endpointPort = Integer.parseInt(line.getOptionValue("endpoint-port"));
            } else {
                endpointPort = REST_ENDPOINT_PORT;
            }
            if( line.hasOption( "cleanup" ) ) {
                cleanup = true;
            }
        }
        catch( Exception exp ) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("CotIngestStream", exp.getMessage(), options,null, true);
        }

        final HostInfo restEndpoint = new HostInfo(endpointHost, endpointPort);

        System.out.println("Connecting to Kafka cluster via bootstrap servers " + KBROKERS);
        System.out.println("REST endpoint at http://" + endpointHost + ":" + endpointPort);

        final KafkaStreams streams = new KafkaStreams(buildTopology(readingsTopic, geohashPrecision), streamsConfig("/tmp/temperature"));

        if(cleanup) {
            streams.cleanUp();
        }
        streams.start();

        // Start the Restful proxy for servicing remote access to state stores
        final QueryingService queryingService = startRestProxy(streams, restEndpoint);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                try {
                    streams.close();
                    queryingService.stop();
                } catch (Throwable e) {
                    System.exit(1);
                }
                latch.countDown();
            }
        });

        latch.await();
        System.exit(0);
    }
}