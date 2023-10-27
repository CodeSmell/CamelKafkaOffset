package codesmell.camel;

import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class CamelUtil {

    /**
     * Generate consistent text describing the Kafka message
     * This will not include the body
     * @param prefix
     * @param exchange
     * 
     * @return
     */
    public static String buildKafkaDescription(String prefix, Exchange exchange) {
        return CamelUtil.buildKafkaDescription(prefix, exchange, false);
    }
    
    /**
     * Generate consistent text describing the Kafka message
     * @param prefix
     * @param exchange
     * @param includeBody
     * 
     * @return
     */
    public static String buildKafkaDescription(String prefix, Exchange exchange, boolean includeBody) {
        String eol = "\n";

        StringBuilder sb = new StringBuilder();
        if (StringUtils.hasText(prefix)) {
            sb.append(prefix);
            sb.append(eol);
        }
        
        sb.append("Message consumed from ");
        sb.append(exchange.getMessage().getHeader(KafkaConstants.TOPIC, String.class));
        sb.append(eol);
        sb.append("The Partition:Offset is ");
        sb.append(exchange.getMessage().getHeader(KafkaConstants.PARTITION, String.class));
        sb.append(":");
        sb.append(exchange.getMessage().getHeader(KafkaConstants.OFFSET, String.class));
        sb.append(eol);
        sb.append("The Key is ");
        sb.append(exchange.getMessage().getHeader(KafkaConstants.KEY, String.class));
        
        if (includeBody) {
            sb.append(eol);
            sb.append(exchange.getMessage().getBody(String.class));
        }
        
        return sb.toString();
    }
    
    /**
     * find the exception in Camel regardless of whether it was handled or not
     */
    public static Exception findCamelException(Exchange exchange) {
        Exception theException = exchange.getException();
        if (Objects.isNull(theException)) {
            theException = (Exception) exchange.getProperty(Exchange.EXCEPTION_CAUGHT);
        }
        return theException;
    }

    /**
     * To get the header value from exchange header
     * @param exchange
     * @param headerName
     * @return
     */
    public static String getCamelHeader(Exchange exchange, String headerName) {
        String header = null;
        if (!Objects.isNull(exchange.getMessage().getHeader(headerName))) {
            header =  new String((byte[]) exchange.getMessage().getHeader(headerName), StandardCharsets.UTF_8);
        }
        return header;
    }

    
    private CamelUtil() {
        // you can't make me
    }
}
