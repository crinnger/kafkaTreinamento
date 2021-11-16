package br.com.crinnger.consumer.consumer;

import br.com.crinnger.consumer.config.ConsumerKafkaConfig;
import br.com.crinnger.consumer.model.City;
import br.com.crinnger.consumer.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TestConsumer {

    @KafkaListener(topics = ConsumerKafkaConfig.TOPIC1, groupId = "group-1")
  //  public void listen(String message,
  //                     @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
  //                     @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
  //                     @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition)
    public void listen(ConsumerRecord<String,String> record)
    {
        //log.info("Thread: {}", Thread.currentThread().getId());
        log.info("Topic1 {}, offet:{}, Key: {}, Particion: {}, msg: {}",
                record.topic(),record.offset(),record.key(),record.partition(),record.value());
    }

    // concurrency defini quantas thread serao aberta para realizar o processo simutaneoo
    @KafkaListener(topics = ConsumerKafkaConfig.TOPICPERSON,groupId = "group-2",
            containerFactory = "personKafkaListenerContainerFactory"
    )
    public void listenPerson(ConsumerRecord<Integer, Person> record){
        log.info("listen Topic {}, offet:{}, Key: {}, Particion: {}, msg: {}",
                record.topic(),record.offset(),record.key().toString(),record.partition(),record.value());
    }

    // concurrency defini quantas thread serao aberta para realizar o processo simutaneoo
    @KafkaListener(topics = ConsumerKafkaConfig.TOPICPERSON,groupId = "group-3",
            containerFactory = "genericKafkaListenerContainerFactory"
    )
    public void listenGeneric(Person person){
        log.info("listen PERSON msg: Nome {} Age{}",
                person.getName(),person.getAge());
    }


    // concurrency defini quantas thread serao aberta para realizar o processo simutaneoo
    @KafkaListener(topics = ConsumerKafkaConfig.TOPICCITY,groupId = "group-4",
            containerFactory = "genericKafkaListenerContainerFactory"
    )
    public void listenGenericCity(City record){
        log.info("listen CITY  msg: Nome{} uf{}",
                record.getNome(),record.getUf());
    }

}
