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
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.direct;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.kafka;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;


/**
 * this test shows how Camel Batch works w/ BreakOnFirstError
 * when there is no onException 
 * and no explicit commit for a message w/ an error
 * 
 * similar to
 * https://github.com/Krivda/camel-bug-reproduction
 */
@CamelSpringBootTest
@EnableAutoConfiguration
@UseAdviceWith
@EmbeddedKafka(controlledShutdown = true, partitions = 1)
public class CamelKafkaBreakOnFirstErrorWithoutOnException {
    private static final Logger LOGGER = LoggerFactory.getLogger(CamelKafkaBreakOnFirstErrorWithoutOnException.class);
   
    private final String ROUTE_ID = "kafkaConsumerBreakOnFirstError";
    
    @Autowired
    protected CamelContext camelContext;

    @Autowired
    protected ProducerTemplate kafkaProducer;

    @Value("${spring.embedded.kafka.brokers}")
    private String kafkaBrokerAddress;

    private final String kafkaGroupId = "codeSmell";
    private final String kafkaTopicName = "topicCodeSmell";
    private final List<String> consumedRecords = new CopyOnWriteArrayList<>();
    private final List<String> consumedErrorRecords = new CopyOnWriteArrayList<>();

    @BeforeEach
    public void setupTestRoutes() throws Exception {
        AdviceWithRouteBuilder.addRoutes(camelContext, builder -> {
            createProducerRoute(builder);
            createConsumerRoute(builder);
        });
        camelContext.start();
    }

    @Test
    public void shouldRetryPayloadWithErrorContinuously() throws Exception {

        List<String> payloadsToPublish = List.of("1", "2", "3", "4", "5", "6", "7", "8");
        List<String> expectedConsumedRecords = List.of("1", "2", "3", "4");

        this.produceRecords(payloadsToPublish);
        
        camelContext.getRouteController().startRoute(ROUTE_ID);

        await()
            .timeout(3, TimeUnit.SECONDS)
            .pollDelay(1, TimeUnit.SECONDS)
            .until(() -> consumedErrorRecords.size() > 2);
            
        assertThat(consumedRecords).isEqualTo(expectedConsumedRecords);
        
        // the payload with the 
        // error will cont to be retried
        // but we stopped it after at least 3 retries
        assertThat(consumedErrorRecords.size()).isGreaterThanOrEqualTo(3);
        
        for (String payload : consumedErrorRecords) {
            assertThat(payload).isEqualTo("5");
        }
    }

    private void produceRecords(final List<String> recordsToPublish) {
        recordsToPublish.forEach(v -> kafkaProducer.sendBody(v));
    }

    private void createConsumerRoute(RouteBuilder builder) {
        builder
             .from(kafkaTestTopic()
                .groupId(kafkaGroupId)
                .autoOffsetReset("earliest")
                .breakOnFirstError(true)
                .autoCommitEnable(false)
                .allowManualCommit(true)
                .maxPollRecords(3)
             )
            .routeId(ROUTE_ID)
            .autoStartup(false)
            .process(exchange -> {
                LOGGER.info(CamelUtil.buildKafkaDescription("Consumed", exchange, true));
            })       
            .process(this::ifIsFifthRecordThrowException)
            // will not add the payload with an 
            // error to the list
            .process().body(String.class, body -> consumedRecords.add(body))
            .process(this::doCommitOffset)
            .end();
    }

    private void ifIsFifthRecordThrowException(Exchange e) {
        String payload = e.getMessage().getBody(String.class);
        if ("5".equals(payload)) {
            this.consumedErrorRecords.add(payload);
            throw new RuntimeException("TEST ERROR");
        }
    }

    private void createProducerRoute(RouteBuilder builder) {
        final DirectEndpointBuilder mockKafkaProducer = direct("mockKafkaProducer");
        kafkaProducer.setDefaultEndpoint(mockKafkaProducer.resolve(camelContext));

        builder
            .from(mockKafkaProducer)
            .to(kafkaTestTopic())
            .log("Published: ${body}");
    }

    private KafkaEndpointBuilder kafkaTestTopic() {
        var basic = kafka(kafkaTopicName)
                .brokers(kafkaBrokerAddress);

        // can either use KafkaManualCommit
        // or set an actual CommitManager
        //this.addCommitManager(basic);
        
        return basic;
    }
    
    private void addCommitManager(KafkaEndpointBuilder basic) {
        basic
            .advanced()
            .kafkaManualCommitFactory(new DefaultKafkaManualCommitFactory());
    }
    
    private void doCommitOffset(Exchange exchange) {
        LOGGER.info(CamelUtil.buildKafkaDescription("Committing", exchange, true));
        KafkaManualCommit manual = exchange.getMessage()
                .getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        if (Objects.nonNull(manual)) {
            manual.commit();
        } else {
            LOGGER.error("KafkaManualCommit is MISSING");
        }
    }
}