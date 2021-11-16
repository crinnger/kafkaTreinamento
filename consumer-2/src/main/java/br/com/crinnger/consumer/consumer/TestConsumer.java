package br.com.crinnger.consumer.consumer;

import br.com.crinnger.consumer.config.ConsumerKafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TestConsumer {

    @KafkaListener(topics = ConsumerKafkaConfig.TOPIC1, groupId = "group-2")
    public void listen(String message){
        //log.info("Thread: {}", Thread.currentThread().getId());
        log.info("Recived: {}",message);
    }
}
