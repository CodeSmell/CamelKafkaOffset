package codesmell.test;

import codesmell.camel.CamelUtil;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.endpoint.dsl.DirectEndpointBuilderFactory.DirectEndpointBuilder;
import org.apache.camel.builder.endpoint.dsl.KafkaEndpointBuilderFactory.KafkaEndpointBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.DefaultKafkaManualCommitFactory;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.direct;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.kafka;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * copied this from 
 * https://github.com/Krivda/camel-bug-reproduction/
 * 
 * this was to test this issue along 
 * with CAMEL-20044
 */
@CamelSpringBootTest
@EnableAutoConfiguration
@UseAdviceWith
@EmbeddedKafka(controlledShutdown = true, partitions = 2)
class CamelKafkaIssue19894 {
    private static final Logger LOGGER = LoggerFactory.getLogger(CamelKafkaIssue19894.class);
    
    private static final String ROUTE_ID = "kafkaConsumer";
    
    @Autowired
    protected CamelContext camelContext;

    @Autowired
    protected ProducerTemplate kafkaProducer;

    @Value("${spring.embedded.kafka.brokers}")
    private String kafkaBrokerAddress;
    
    private final String kafkaGroupId = "test_group_id";
    private final String kafkaTopicName = "test_topic";
    
    private final List<String> consumedRecords = new ArrayList<>();

    @BeforeEach
    public void setupTestRoutes() throws Exception {
        AdviceWithRouteBuilder.addRoutes(camelContext, builder -> {
            createProducerRoute(builder);
            createConsumerRoute(builder);
        });
        camelContext.start();
    }

    @Test
    public void shouldNotGetOverFirstError() throws Exception {
        // partitions are consumed backwards :)
        final List<String> producedRecordsPartition0 = List.of("5", "6", "7", "8", "9", "10", "11"); // <- Error is thrown once on record "5"
        final List<String> producedRecordsPartition1 = List.of("1", "2", "3", "4");

        final List<String> expectedConsumedRecords = List.of("1", "2", "3", "4"); // only records form part_0 should be consumed


        this.produceRecords(producedRecordsPartition0, producedRecordsPartition1);
        
        camelContext.getRouteController().startRoute(ROUTE_ID);

        await()
            .timeout(30, TimeUnit.SECONDS)
            .pollDelay(20, TimeUnit.SECONDS)
            .untilAsserted(() ->
            // Assertion fails as all records on topic are reconsumed on error.
            assertThat(consumedRecords).isEqualTo(expectedConsumedRecords));

    }

    private void produceRecords(final List<String> producedRecordsPartition0, List<String> producedRecordsPartition1) throws Exception {
        producedRecordsPartition0.forEach(v -> kafkaProducer.sendBodyAndHeader(v, KafkaConstants.PARTITION_KEY, 0));
        producedRecordsPartition1.forEach(v -> kafkaProducer.sendBodyAndHeader(v, KafkaConstants.PARTITION_KEY, 1));
    }

    private void createConsumerRoute(RouteBuilder builder) {
        builder
             .from(kafkaTestTopic()
                .groupId(kafkaGroupId)
                .autoOffsetReset("earliest")
                .breakOnFirstError(true)
                .autoCommitEnable(false)
                .allowManualCommit(true)
                // with only 1 consumerCount 
                // relying on Camel implementation
                // to consume from partitions in specific order
                //
                //  get 8 recs total: 4 from part_0 and 4 from part_1 in first poll. 
                // recs "9" and "10" won't be in first poll
                .maxPollRecords(8)
                .commitTimeoutMs(1000000L)
             )
            .routeId(ROUTE_ID)
            .autoStartup(false)
            .process(exchange -> {
                LOGGER.info(CamelUtil.buildKafkaDescription("Consumed", exchange, true));
            })  
            .process(this::ifIsFifthRecordThrowException)
            .process().body(String.class, body -> consumedRecords.add(body))
            .end();
    }

    private void ifIsFifthRecordThrowException(Exchange e) {
        if (e.getMessage().getBody().equals("5")) {
            throw new RuntimeException("ERROR_TRIGGERED_BY_TEST");
        }
    }

    private void createProducerRoute(RouteBuilder builder) {
        final DirectEndpointBuilder mockKafkaProducer = direct("mockKafkaProducer");
        kafkaProducer.setDefaultEndpoint(mockKafkaProducer.resolve(camelContext));

        builder
            .from(mockKafkaProducer)
            .to(kafkaTestTopic())
            .log("Sent: ${body}");
    }

    private KafkaEndpointBuilder kafkaTestTopic() {
        var basic = kafka(kafkaTopicName)
                .brokers(kafkaBrokerAddress);

        // setting the Commit Manager
        basic
            .advanced()
                .kafkaManualCommitFactory(new DefaultKafkaManualCommitFactory()); // Ensure commits after each partition is processed
        return basic;
    }
}
