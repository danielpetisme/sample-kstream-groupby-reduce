package com.examples.danielpetisme;

import io.github.netmikey.logunit.api.LogCapturer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class GroupByReduceTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(GroupByReduceTest.class);

    @RegisterExtension
    LogCapturer logCapturer = LogCapturer.create().captureForLogger(Logger.ROOT_LOGGER_NAME);

    static final String IputTopic1 = "in1";
    static final String OutputTopic = "out";


    Topology createTopologyWithNoSuppress() {

        final var builder = new StreamsBuilder();

        builder.stream(IputTopic1, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> LOGGER.info("input values for key [" + k + "] => [" + v + "]"))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(1)))
                .reduce((v1, v2) -> v1 + v2)
                .toStream()
                .map((Windowed<String> key, String value) -> new KeyValue<>(key.key(), value))
                .peek((k, v) -> LOGGER.info("Produced values for key [" + k + "] => [" + v + "]"))
                .to(OutputTopic,
                        Produced.<String, String>as("aggregated-value")
                                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
        return builder.build();
    }

    Topology createTopologyWithNoSuppressAndWindows() {

        final var builder = new StreamsBuilder();

        builder.stream(IputTopic1, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> LOGGER.info("input values for key [" + k + "] => [" + v + "]"))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(1)))
                .reduce((v1, v2) -> v1 + v2)
                .toStream()
                .map((Windowed<String> key, String value) -> new KeyValue<>(key.key() + "-" + key.window(), value))
                .peek((k, v) -> LOGGER.info("Produced values for key [" + k + "] => [" + v + "]"))
                .to(OutputTopic,
                        Produced.<String, String>as("aggregated-value")
                                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
        return builder.build();
    }

    Topology createTopologyWithSuppress() {

        final var builder = new StreamsBuilder();

        builder.stream(IputTopic1, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> LOGGER.info("input values for key [" + k + "] => [" + v + "]"))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(1)))
                .reduce((v1, v2) -> v1 + v2)
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .map((Windowed<String> key, String value) -> new KeyValue<>(key.key(), value))
                .peek((k, v) -> LOGGER.info("Produced values for key [" + k + "] => [" + v + "]"))
                .to(OutputTopic,
                        Produced.<String, String>as("aggregated-value")
                                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
        return builder.build();
    }

    final List<TestRecord<String, String>> input = List.of(
            new TestRecord("1", "a", null, 1L),
            new TestRecord("1", "b", null, 2L),
            new TestRecord("2", "a", null, 3L),
            new TestRecord("1", "c", null, 4L),
            new TestRecord("1", "a", null, 25_000L)
    );

    //        Input values for key [1] => [a]
//        Producer values for key [1] => [a]
//        Input values for key [1] => [b]
//        Producer values for key [1] => [null]
//        Producer values for key [1] => [ab]
//        Input values for key [2] => [a]
//        Producer values for key [2] => [a]
//        Input values for key [1] => [c]
//        Producer values for key [1] => [null]
//        Producer values for key [1] => [abc]
//        Input values for key [1] => [a]
//        Producer values for key [1] => [a]
    final List<TestRecord<String, String>> expectedWithNoCacheAndNoSuppress = List.of(
            new TestRecord("1", "a", null, 1L),
            new TestRecord("1", null, null, 1L),
            new TestRecord("1", "ab", null, 2L),
            new TestRecord("2", "a", null, 3L),
            new TestRecord("1", null, null, 2L),
            new TestRecord("1", "abc", null, 4L),
            new TestRecord("1", "a", null, 25_000L)
    );

    final List<TestRecord<String, String>> expectedWithSuppress = List.of(
            new TestRecord("1", "abc", null, 4L),
            new TestRecord("2", "a", null, 3L)
    );

    // Topology Test Driver vs Kafka Stream caching
    //https://kafka.apache.org/32/javadoc/org/apache/kafka/streams/TopologyTestDriver.html
    // Note that the TopologyTestDriver processes input records synchronously. This implies that commit.interval.ms and cache.max.bytes.buffering configuration have no effect. The driver behaves as if both configs would be set to zero, i.e., as if a "commit" (and thus "flush") would happen after each input record.
    @Test
    @Order(1)
    public void runTestWithTopologyTestDriver_And_WithNoSuppress() throws Exception {
        runTopologyTestDriver(createTopologyWithNoSuppress(), input, expectedWithNoCacheAndNoSuppress);
    }


    // This test fail on purpose
    @Test
    @Order(2)
    public void runTestWithKafka_And_WithCache_And_WithNoSuppress() throws Exception {

        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            kafka.start();
            runTestContainer(
                    kafka,
                    createTopologyWithNoSuppress(),
                    input, expectedWithNoCacheAndNoSuppress,
                    Collections.emptyMap()
            );
        }
    }

    @Test
    @Order(3)
    public void runTestWithKafka_And_WithoutKafkaStreamsCache_And_WithNoSuppress() throws Exception {
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            kafka.start();
            runTestContainer(
                    kafka,
                    createTopologyWithNoSuppress(),
                    input,
                    expectedWithNoCacheAndNoSuppress,
                    Map.of(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
            );
        }
    }

    @Test
    @Order(4)
    public void runTestWithKafka_And_WithCache_And_WithNoSuppress_Demonstration() throws Exception {
        final List<TestRecord<String, String>> expectedWithCacheAndNoSuppressDefaultConfig = List.of(
//                new TestRecord("1", "a", null, 1L), These intermediate events are cached
//                new TestRecord("1", null, null, 1L),
//                new TestRecord("1", "ab", null, 2L),
                new TestRecord("2", "a", null, 3L),
//                new TestRecord("1", null, null, 2L),
                new TestRecord("1", "abc", null, 4L),
                new TestRecord("1", "a", null, 25_000L)
        );

        //When cache is not tweak, the changes are masked by Kafka Streams cache and only the final result is propagated
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            kafka.start();
            runTestContainer(
                    kafka,
                    createTopologyWithNoSuppress(),
                    input, expectedWithCacheAndNoSuppressDefaultConfig,
                    Collections.emptyMap()
            );
        }

        final List<TestRecord<String, String>> expectedWithNoCacheAndNoSuppressCachingCommitIntervalUpdated = List.of(
//                new TestRecord("1", "a", null, 1L), These intermediate events are cached
//                new TestRecord("1", null, null, 1L),
                new TestRecord("1", "ab", null, 2L),
                new TestRecord("2", "a", null, 3L),
                new TestRecord("1", null, null, 2L),
                new TestRecord("1", "abc", null, 4L),
                new TestRecord("1", "a", null, 25_000L)
        );

        //The cache size may alter the generated events
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            kafka.start();
            runTestContainer(
                    kafka,
                    createTopologyWithNoSuppress(),
                    input, expectedWithNoCacheAndNoSuppressCachingCommitIntervalUpdated,
                    Map.of(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100", StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "100")
            );
        }

        //Default config producer less intermediate event than a bit of caching
        assertThat(expectedWithCacheAndNoSuppressDefaultConfig.size()).isLessThan(expectedWithNoCacheAndNoSuppressCachingCommitIntervalUpdated.size());

        //A bit of caching producer less events than no caching
        assertThat(expectedWithNoCacheAndNoSuppressCachingCommitIntervalUpdated.size()).isLessThan(expectedWithNoCacheAndNoSuppress.size());

    }


    // Suppress filter intermediate result per key and window
    @Test
    @Order(5)
    public void runTestWithTopologyTestDriver_And_WithSuppress() throws Exception {
        runTopologyTestDriver(createTopologyWithSuppress(), input, expectedWithSuppress);
    }

    @Test
    @Order(6)
    public void runTestWithKafka_And_WithCache_And_WithSuppress() throws Exception {
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            kafka.start();
            runTestContainer(
                    kafka,
                    createTopologyWithSuppress(),
                    input, expectedWithSuppress,
                    Collections.emptyMap()
            );
        }
    }

    @Test
    @Order(7)
    public void runTestWithKafka_And_WithoutKafkaStreamsCache_And_WithSuppress() throws Exception {
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            kafka.start();
            runTestContainer(
                    kafka,
                    createTopologyWithSuppress(),
                    input, expectedWithSuppress,
                    Map.of(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
            );
        }
    }

    // Suppress is triggering at processing time and requires closing events (ie. events occuring after the window period)
    @Test
    @Order(8)
    public void runTestWithTopologyTestDriver_And_WithSuppress_And_ClosingEvent() throws Exception {
        runTopologyTestDriver(createTopologyWithSuppress(), input, expectedWithSuppress);

        var inputWithClosingEvent = new ArrayList<>(input);
        inputWithClosingEvent.add(new TestRecord("1", "d", null, 31_000L));

        var expectedWithClosingEvent = new ArrayList<>(expectedWithSuppress);
        expectedWithClosingEvent.add(new TestRecord<>("1", "a", null, 25_000L));
        runTopologyTestDriver(createTopologyWithSuppress(), inputWithClosingEvent, expectedWithClosingEvent);
    }

    @Test
    @Order(9)
    // Counting he windows open // closed
    public void runTestWithTopologyTestDriver_And_WithNoSuppress_And_ClosingEvent() throws Exception {
        // When events are generated within the session window, the window extends
        List<TestRecord<String, String>> inputWithEventInSessionWindow = List.of(
                new TestRecord("1", "a", null, 1L),
                new TestRecord("1", "b", null, 2L),
                new TestRecord("2", "a", null, 3L),
                new TestRecord("1", "c", null, 4L),
                new TestRecord("1", "d", null, 5_000L), //<--Adding an event in the session window
                new TestRecord("1", "a", null, 25_000L)
        );

        List<TestRecord<String, String>> expectedWithEventInSessionWindow = List.of(
                new TestRecord("1-Window{startMs=1, endMs=1}", "a", null, 1L),
                new TestRecord("1-Window{startMs=1, endMs=1}", null, null, 1L),
                new TestRecord("1-Window{startMs=1, endMs=2}", "ab", null, 2L),
                new TestRecord("2-Window{startMs=3, endMs=3}", "a", null, 3L),
                new TestRecord("1-Window{startMs=1, endMs=2}", null, null, 2L),
                new TestRecord("1-Window{startMs=1, endMs=4}", "abc", null, 4L),
                new TestRecord("1-Window{startMs=1, endMs=4}", null, null, 4L),
                new TestRecord("1-Window{startMs=1, endMs=5000}", "abcd", null, 5_000L),
                new TestRecord("1-Window{startMs=25000, endMs=25000}", "a", null, 25_000L)
        );

        runTopologyTestDriver(createTopologyWithNoSuppressAndWindows(), inputWithEventInSessionWindow, expectedWithEventInSessionWindow);
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            kafka.start();
            runTestContainer(
                    kafka,
                    createTopologyWithNoSuppressAndWindows(),
                    inputWithEventInSessionWindow, expectedWithEventInSessionWindow,
                    Map.of(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
            );
        }

        List<TestRecord<String, String>> expectedWithEventInSessionWindowWithCache = List.of(
                new TestRecord("2-Window{startMs=3, endMs=3}", "a", null, 3L),
                new TestRecord("1-Window{startMs=1, endMs=5000}", "abcd", null, 5_000L),
                new TestRecord("1-Window{startMs=25000, endMs=25000}", "a", null, 25_000L)
        );

        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            kafka.start();
            runTestContainer(
                    kafka,
                    createTopologyWithNoSuppressAndWindows(),
                    inputWithEventInSessionWindow, expectedWithEventInSessionWindowWithCache,
                    Collections.emptyMap()
            );
        }

        // When events are generated outside the session window, a new window is created
        List<TestRecord<String, String>> inputWithEventOutSessionWindow = List.of(
                new TestRecord("1", "a", null, 1L),
                new TestRecord("1", "b", null, 2L),
                new TestRecord("2", "a", null, 3L),
                new TestRecord("1", "c", null, 4L),
                new TestRecord("1", "d", null, 7_000L), //<--Adding an event outside the session window
                new TestRecord("1", "a", null, 25_000L)
        );

        List<TestRecord<String, String>> expectedWithEventOutSessionWindow = List.of(
                new TestRecord("1-Window{startMs=1, endMs=1}", "a", null, 1L),
                new TestRecord("1-Window{startMs=1, endMs=1}", null, null, 1L),
                new TestRecord("1-Window{startMs=1, endMs=2}", "ab", null, 2L),
                new TestRecord("2-Window{startMs=3, endMs=3}", "a", null, 3L),
                new TestRecord("1-Window{startMs=1, endMs=2}", null, null, 2L),
                new TestRecord("1-Window{startMs=1, endMs=4}", "abc", null, 4L),
                new TestRecord("1-Window{startMs=7000, endMs=7000}", "d", null, 7_000L),
                new TestRecord("1-Window{startMs=25000, endMs=25000}", "a", null, 25_000L)
        );

        runTopologyTestDriver(createTopologyWithNoSuppressAndWindows(), inputWithEventOutSessionWindow, expectedWithEventOutSessionWindow);
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            kafka.start();
            runTestContainer(
                    kafka,
                    createTopologyWithNoSuppressAndWindows(),
                    inputWithEventOutSessionWindow, expectedWithEventOutSessionWindow,
                    Map.of(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
            );
        }

        List<TestRecord<String, String>> expectedWithEventOutSessionWindowWithCache = List.of(
                new TestRecord("2-Window{startMs=3, endMs=3}", "a", null, 3L),
                new TestRecord("1-Window{startMs=1, endMs=4}", "abc", null, 4L),
                new TestRecord("1-Window{startMs=7000, endMs=7000}", "d", null, 7_000L),
                new TestRecord("1-Window{startMs=25000, endMs=25000}", "a", null, 25_000L)

        );

        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            kafka.start();
            runTestContainer(
                    kafka,
                    createTopologyWithNoSuppressAndWindows(),
                    inputWithEventOutSessionWindow, expectedWithEventOutSessionWindowWithCache,
                    Collections.emptyMap()
            );
        }
    }


    @Test
    @Order(10)
    // Getting out of order events - skipping previous windows
    public void runTestWithTopologyTestDriver_And_WithNoSuppress_And_No_Order() throws Exception {
        // When events are generated within the session window, the window extends
        List<TestRecord<String, String>> inputWithEventNotOrdered = List.of(
                new TestRecord("1", "a", null, 25_000L),
                new TestRecord("1", "a", null, 1L),
                new TestRecord("1", "b", null, 2L),
                new TestRecord("2", "a", null, 3L),
                new TestRecord("1", "c", null, 4L)
        );

        List<TestRecord<String, String>> expectedWithEventNotOrdered = List.of(
                new TestRecord("1-Window{startMs=25000, endMs=25000}", "a", null, 25_000L)
        );

        runTopologyTestDriver(createTopologyWithNoSuppressAndWindows(), inputWithEventNotOrdered, expectedWithEventNotOrdered);

        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            kafka.start();
            runTestContainer(
                    kafka,
                    createTopologyWithNoSuppressAndWindows(),
                    inputWithEventNotOrdered, expectedWithEventNotOrdered,
                    Collections.emptyMap()
            );
        }

        logCapturer.assertContains("Skipping record for expired window. topic=[in1] partition=[0] offset=[1] timestamp=[1] window=[1,1] expiration=[19000] streamTime=[25000]");
        logCapturer.assertContains("Skipping record for expired window. topic=[in1] partition=[0] offset=[2] timestamp=[2] window=[2,2] expiration=[19000] streamTime=[25000]");
        logCapturer.assertContains("Skipping record for expired window. topic=[in1] partition=[0] offset=[3] timestamp=[3] window=[3,3] expiration=[19000] streamTime=[25000]");
        logCapturer.assertContains("Skipping record for expired window. topic=[in1] partition=[0] offset=[4] timestamp=[4] window=[4,4] expiration=[19000] streamTime=[25000]");
    }


    private void runTopologyTestDriver(Topology topology, List<TestRecord<String, String>> inputRecords, List<TestRecord<String, String>> expectedRecords) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wallClock-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.STATE_DIR_CONFIG, Paths.get("target", "topology-test-driver", UUID.randomUUID().toString()).toString());

        TopologyTestDriver driver = new TopologyTestDriver(topology, config);
        TestInputTopic<String, String> input1;
        TestOutputTopic<String, String> output;


        input1 = driver.createInputTopic(
                IputTopic1,
                Serdes.String().serializer(),
                Serdes.String().serializer()
        );

        output = driver.createOutputTopic(
                OutputTopic,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        );

//        input1.pipeInput("1", "a", 1);
//        input1.pipeInput("1", "b", 2);
//        input1.pipeInput("2", "a", 3);
//        input1.pipeInput("1", "c", 4);
//
//        //When sleeps ends the window is not closed because there is no new input event
//        Thread.sleep(Duration.ofSeconds(20).toMillis()); //<-- No need since using event time (vs LogAppendTime
//
//        //Event for close the window.
//        input1.pipeInput("1", "a", 25000);

        inputRecords.forEach((record -> {
            input1.pipeInput(record.key(), record.value(), record.timestamp());
        }));

        var actualRecords = output.readRecordsToList();
        actualRecords.forEach(System.out::println);
        assertThat(actualRecords).hasSameElementsAs(expectedRecords);
    }

    private void runTestContainer(KafkaContainer kafka, Topology topology, List<TestRecord<String, String>> inputRecords, List<TestRecord<String, String>> expectedRecords, Map<String, String> overrideStreamsConfig) throws Exception {
        var adminClient = AdminClient.create(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        ));

        for (String topicName : List.of(IputTopic1, OutputTopic)) {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                LOGGER.info("Creating topic {}", topicName);
                final NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
                try {
                    CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                    topicsCreationResult.all().get();
                } catch (Exception e) {
                    //silent ignore if topic already exists
                }
            }
        }

        KafkaProducer<String, String> inputProducer = new KafkaProducer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.CLIENT_ID_CONFIG, "wallClockTestContainer-producer-" + UUID.randomUUID(),
                        ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE,
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"
                ),
                new StringSerializer(), new StringSerializer()
        );

        KafkaConsumer<String, String> outputConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "wallClockTestContainer-consumer-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer());
        outputConsumer.subscribe(Collections.singletonList(OutputTopic));

        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "wallClockTestContainer-stream-" + UUID.randomUUID());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, Paths.get("target", "testcontainers", UUID.randomUUID().toString()).toString());
        streamsConfig.putAll(overrideStreamsConfig);


        KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        inputRecords.forEach((record) -> {
            try {
                inputProducer.send(
                        new ProducerRecord<>(IputTopic1, 0, record.timestamp(), record.key(), record.value()),
                        (RecordMetadata metadata, Exception exception) -> {
                            if (exception != null) {
                                fail(exception.getMessage());
                            } else {
                                LOGGER.info("--> Sending data, k: {} - v: {}", record.key(), record.value());
                            }
                        }
                ).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });


        List<ConsumerRecord<String, String>> loaded = new ArrayList<>();

        long start = System.currentTimeMillis();
        while (loaded.size() < expectedRecords.size() && System.currentTimeMillis() - start < 40_000) {
            ConsumerRecords<String, String> records = outputConsumer.poll(Duration.of(3, ChronoUnit.SECONDS));
            records.forEach((record) -> loaded.add(record));
        }

        inputProducer.close(Duration.ofSeconds(3));
        outputConsumer.close(Duration.ofSeconds(3));
        streams.close(Duration.ofSeconds(3));

        List<TestRecord<String, String>> actualRecords = loaded.stream()
                .map(consumerRecord -> new TestRecord<>(consumerRecord.key(), consumerRecord.value(), consumerRecord.headers(), consumerRecord.timestamp()))
                .collect(Collectors.toList());
        assertThat(actualRecords).hasSameElementsAs(expectedRecords);
    }
}
