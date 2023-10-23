package codesmell.camel.config;

import codesmell.camel.kafka.KafkaOffsetManagerProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CamelConfig {

    @Bean
    KafkaOffsetManagerProcessor buildKafkaOffsetManagerProcessor() {
        return new KafkaOffsetManagerProcessor();
    }

}
