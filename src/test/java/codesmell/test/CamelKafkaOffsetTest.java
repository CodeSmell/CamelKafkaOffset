package codesmell.test;

import codesmell.camel.CamelUtil;
import codesmell.camel.config.CamelConfig;
import codesmell.camel.kafka.KafkaOffsetManagerProcessor;
import codesmell.exception.NonRetryException;
import codesmell.exception.RetryableException;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.endpoint.dsl.DirectEndpointBuilderFactory.DirectEndpointBuilder;
import org.apache.camel.builder.endpoint.dsl.KafkaEndpointBuilderFactory.KafkaEndpointBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.direct;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.kafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ActiveProfiles("test")
@CamelSpringBootTest
@EnableAutoConfiguration
@UseAdviceWith
@Import({ 
    CamelConfig.class
    })
@EmbeddedKafka(controlledShutdown = true, partitions = 3)
public class CamelKafkaOffsetTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CamelKafkaOffsetTest.class);
    
    public static final String UNHANDLED_ERROR_NORETRY_ROUTE_URI = "direct:handleError";

    @Autowired
    private CamelContext camelContext;

    @Autowired
    private ProducerTemplate kafkaProducer;
    
    @Autowired
    KafkaOffsetManagerProcessor kafkaOffsetManager;
    
    private final String kafkaGroupId = "test_group_id";
    private final String kafkaTopicName = "foobarTopic";
    private final List<String> consumedRecords = new CopyOnWriteArrayList<>();
    @Value("${spring.embedded.kafka.brokers}")
    private String kafkaBrokerAddress;

    @BeforeEach
    public void setupTestRoutes() throws Exception {
        Awaitility.setDefaultTimeout(60, TimeUnit.SECONDS);
        
        AdviceWithRouteBuilder.addRoutes(camelContext, builder -> {
            createConsumerRoute(builder);
            createProducerRoute(builder);
        });
        camelContext.start();
    }

    @Test
    public void offsetRestIncorrectlyOnRejoin() {
        final List<String> producedRecords = List.of("1", "2", "3", "4", "5", "NORETRY-ERROR",
                "6", "7", "NORETRY-ERROR", "8", "9", "10", "11");

        this.produceRecords(producedRecords);

        await().pollDelay(10, TimeUnit.SECONDS)
            .pollInterval(10, TimeUnit.SECONDS)
            .until(() -> consumedRecords.size() == 15 | isConsumedMoreThanOnce(false));
        
        // a dump of what happened
        boolean consumedMoreThanOnce = this.isConsumedMoreThanOnce(true);
        
        assertThat(consumedMoreThanOnce).isFalse();
        assertThat(consumedRecords.size()).isEqualTo(15);
    }

    private void produceRecords(final List<String> producedRecords) {
        for (String payload : producedRecords) {
            LOGGER.info("publishing payload >>> " + payload);
            kafkaProducer.sendBodyAndHeader(payload, KafkaConstants.KEY, payload);
        }
    }

    private void createConsumerRoute(RouteBuilder builder) {
        builder.onException(RetryableException.class)
            // would leave this exception unhandled
            // so that any DB transaction work
            // would be rolled back
            // but also NOT commit offset       
            .handled(false)
            .log(LoggingLevel.ERROR, "Having a retryable issue due to ${exception.message}");
        
        builder.onException(NonRetryException.class)
            // would leave this exception unhandled
            // so that any DB transaction work
            // would be rolled back
            // but also commit offset
            .handled(false)
            .log(LoggingLevel.ERROR, "Having an unretryable issue due to ${exception.message}")
            .to(UNHANDLED_ERROR_NORETRY_ROUTE_URI);
        
        builder.from(UNHANDLED_ERROR_NORETRY_ROUTE_URI)
            // can't retry so move offset
            .process(exchange -> {
                LOGGER.info(CamelUtil.buildKafkaDescription("Unhandled exception on message from Kafka - will not retry", exchange, false));
            })
            .process(kafkaOffsetManager)
            .end();
        
        builder.from(kafkaTestTopic()
             .groupId(kafkaGroupId)
             .autoOffsetReset("earliest")
             .autoCommitEnable(false)
             .allowManualCommit(true)
             .breakOnFirstError(true)
             .maxPollRecords(1)
             .consumersCount(3)
            )
            .process(exchange -> {
                LOGGER.info(CamelUtil.buildKafkaDescription("Message consumed from Kafka", exchange, true));
            })
            .process(exchange -> {
                exchange.getMessage().setHeader(KafkaOffsetManagerProcessor.LOG_EVENT_BODY, true);
            })
            .process().body(String.class, body -> consumedRecords.add(body))
            // would normally be doing DB inserts/updates
            // after validation of the data
            .process(this::ifIsPayloadWithErrorThrowException)
            .process(kafkaOffsetManager)      
            .end();
    }

    private void ifIsPayloadWithErrorThrowException(Exchange exchange) {
        String payload = exchange.getMessage().getBody(String.class);
        if (payload.equals("RETRY-ERROR")) {
            throw new RetryableException("RETRYABLE ERROR TRIGGERED BY TEST");
        } else if (payload.equals("NORETRY-ERROR")) {
            throw new NonRetryException("NON RETRY ERROR TRIGGERED BY TEST");
        }
    }

    private void createProducerRoute(RouteBuilder builder) {
        final DirectEndpointBuilder simpleKafkaProducer = direct("simpleKafkaProducer");
        kafkaProducer.setDefaultEndpoint(simpleKafkaProducer.resolve(camelContext));

        builder.from(simpleKafkaProducer)
            .to(kafkaTestTopic());
    }

    private KafkaEndpointBuilder kafkaTestTopic() {
        return kafka(kafkaTopicName)
            .brokers(kafkaBrokerAddress);
    }
    
    private LinkedHashMap<String, Long> generateMapOfConsumedRecordsWithCounts() {
        return consumedRecords.stream()
                //.filter(s -> !s.equals("NORETRY-ERROR"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                    .collect(LinkedHashMap::new, (m,e) -> m.put(e.getKey(), e.getValue()), Map::putAll);
    }
    
    private boolean isConsumedMoreThanOnce(boolean logEach) {
        boolean consumedMoreThanOnce = false;
        
        LinkedHashMap<String, Long> countMap = this.generateMapOfConsumedRecordsWithCounts();
        for (Map.Entry<String,Long> entry : countMap.entrySet()) {
            if (logEach) {
                LOGGER.info("Consumed Key: " + entry.getKey() + " " + entry.getValue() + " times");
            }
            if (!entry.getKey().equals("NORETRY-ERROR") && entry.getValue() > 1) {
                consumedMoreThanOnce = true;
            }
        }
        return consumedMoreThanOnce;
    }
    
}