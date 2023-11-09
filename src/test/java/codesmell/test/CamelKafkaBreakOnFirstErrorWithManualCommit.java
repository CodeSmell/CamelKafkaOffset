package codesmell.test;


import codesmell.camel.CamelUtil;
import codesmell.camel.config.CamelConfig;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.endpoint.dsl.DirectEndpointBuilderFactory.DirectEndpointBuilder;
import org.apache.camel.builder.endpoint.dsl.KafkaEndpointBuilderFactory.KafkaEndpointBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
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
import org.springframework.context.annotation.Import;
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
 * when there is an onException 
 * and we manage the commits
 */
@CamelSpringBootTest
@EnableAutoConfiguration
@UseAdviceWith
@Import({ 
    CamelConfig.class
    })
@EmbeddedKafka(controlledShutdown = true, partitions = 1)
public class CamelKafkaBreakOnFirstErrorWithManualCommit {
    private static final Logger LOGGER = LoggerFactory.getLogger(CamelKafkaBreakOnFirstErrorWithManualCommit.class);
   
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

    @BeforeEach
    public void setupTestRoutes() throws Exception {
        AdviceWithRouteBuilder.addRoutes(camelContext, builder -> {
            createConsumerRoute(builder);
            createProducerRoute(builder);
        });
        camelContext.start();
    }

    @Test
    public void shouldRetryPayloadWithErrorTwice() throws Exception {
        
        List<String> payloadsToPublish = List.of("1", "2", "3", "4", "5", "6", "7", "8");
        
        // current behavior
        //List<String> expectedConsumedRecords = List.of("1", "2", "3", "4", "5", "5", "6", "7", "8");
        // new behavior
        List<String> expectedConsumedRecords = List.of("1", "2", "3", "4", "5", "6", "7", "8");

        this.produceRecords(payloadsToPublish);
        
        camelContext.getRouteController().startRoute(ROUTE_ID);

        await()
            .timeout(3, TimeUnit.SECONDS)
            .pollDelay(1, TimeUnit.SECONDS)
            .until(() -> consumedRecords.size() > (expectedConsumedRecords.size() -1));
            
        assertThat(consumedRecords).isEqualTo(expectedConsumedRecords);
    }

    private void produceRecords(final List<String> recordsToPublish) {
        recordsToPublish.forEach(v -> kafkaProducer.sendBody(v));
    }

    private void createConsumerRoute(RouteBuilder builder) {
        
        builder
            .onException(RuntimeException.class)
                // would leave this exception unhandled
                // but also commit offset
                .handled(false)
                .log(LoggingLevel.ERROR, "Having an unretryable issue due to ${exception.message}")
                .process(this::doCommitOffset)
                .end();
        
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
                LOGGER.info(CamelUtil.buildKafkaDescription("Consuming", exchange, true));
            })       
            .process().body(String.class, body -> consumedRecords.add(body))
            .process(this::ifIsFifthRecordThrowException)
            .process(this::doCommitOffset)
            .end();
    }

    private void ifIsFifthRecordThrowException(Exchange e) {
        String payload = e.getMessage().getBody(String.class);
        if ("5".equals(payload)) {
            throw new RuntimeException("TEST ERROR");
        }
    }
    
    private void doCommitOffset(Exchange exchange) {
        LOGGER.debug(CamelUtil.buildKafkaDescription("Committing", exchange, true));
        KafkaManualCommit manual = exchange.getMessage()
            .getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        if (Objects.nonNull(manual)) {
            manual.commit();
        } else {
            LOGGER.error("KafkaManualCommit is MISSING");
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
        return basic;
    }
}