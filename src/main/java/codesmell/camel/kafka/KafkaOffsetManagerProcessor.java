package codesmell.camel.kafka;

import codesmell.camel.CamelUtil;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Objects;

/**
 * manages committing the offset after consuming a message from a Kafka topic
 *
 */
@Service
public class KafkaOffsetManagerProcessor implements Processor {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaOffsetManagerProcessor.class);
    
    public static final String LOG_EVENT_BODY = "kafkaLogEventBody";

    @Override
    public void process(Exchange exchange) throws Exception {
        
        Boolean logMessage = exchange.getMessage().getHeader(LOG_EVENT_BODY, Boolean.class);
        if (Objects.isNull(logMessage)) {
            logMessage = false;
        }
        
        LOGGER.debug(CamelUtil.buildKafkaDescription("Evaluating event with offset", exchange, logMessage));
        

        
        // is this the last message consumed in the current poll
        Boolean lastOne = exchange.getMessage()
            .getHeader(KafkaConstants.LAST_RECORD_BEFORE_COMMIT, Boolean.class);
        
        if (Objects.isNull(lastOne)) {
            this.logAutCommitWarning();
        } else {
            if (lastOne) {
                LOGGER.debug(CamelUtil.buildKafkaDescription("time to commit the offset...", exchange, logMessage));
                KafkaManualCommit manual = exchange.getMessage()
                    .getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);

                if (manual != null) {
                    LOGGER.info(CamelUtil.buildKafkaDescription("manually committing the offset for batch", 
                            exchange, logMessage));
                    manual.commit();
                } else {
                    LOGGER.error(CamelUtil.buildKafkaDescription(
                        "OFFSET MANAGER IS MISSING when trying to commit the offset", exchange, logMessage));
                }
            } else {
                LOGGER.debug(CamelUtil.buildKafkaDescription("NOT time to commit the offset yet", exchange, logMessage));
            }
        }
    }
    
    private void logAutCommitWarning() {
        StringBuilder sb = new StringBuilder();
        sb.append("\r\n");
        sb.append("**************").append("\r\n");
        sb.append("autoCommitEnable is on! Please set autoCommitEnable to false!").append("\r\n");
        sb.append("**************");

        LOGGER.warn(sb.toString());
    }

}
