package br.com.crinnger.consumer.config;

import br.com.crinnger.consumer.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;

// enbla kafka e somente para consumidora
@EnableKafka
@Configuration
@Slf4j
public class ConsumerKafkaConfig {

    public static final String TOPIC1="topic-1";
    public static final String TOPICPERSON="json-person";
    public static final String TOPICCITY ="json-city";

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public ConsumerFactory<String,String> consumerFactory(){
        var configs = new HashMap<String,Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String,String> kafkaListenerContainerFactory(){
        var factory=new ConcurrentKafkaListenerContainerFactory<String,String>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    // nessa abrodagem necessita colocar a dependencia do jackon
    @Bean
    public ConsumerFactory<Integer, Person> personConsumerFactory(){
        var configs = new HashMap<String,Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        var jsonDeserializer = new JsonDeserializer<Person>(Person.class)
                .trustedPackages("*")
                .forKeys();
        return new DefaultKafkaConsumerFactory<Integer,Person>(configs,new IntegerDeserializer(), jsonDeserializer);
        //return new DefaultKafkaConsumerFactory<Integer,Person>(configs);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer,Person> personKafkaListenerContainerFactory(){
        var factory=new ConcurrentKafkaListenerContainerFactory<Integer,Person>();
        factory.setConsumerFactory(personConsumerFactory());
        factory.setRecordInterceptor(adultInterceptor());
        return factory;
    }

    private RecordInterceptor<Integer, Person> adultInterceptor() {
        return consumerRecord -> {
            log.info("record: {}", consumerRecord);
            return consumerRecord.value().getAge()>=20 ? consumerRecord:null;
        };
    }


    @Bean
    public ConsumerFactory genericConsumerFactory(){
        var configs = new HashMap<String,Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // vai realizar a leitura dos topics retornan uma lista deles para processar, no listener tem que receber uma LIST
        //factory.setBatchListener(true);
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory genericKafkaListenerContainerFactory(){
        var factory=new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(genericConsumerFactory());
        factory.setMessageConverter(new JsonMessageConverter());
        // vai realizar a leitura dos topics retornan uma lista deles para processar, no listener tem que receber uma LIST
        //factory.setMessageConverter(new BatchMessagingMessageConverter(new JsonMessageConverter()));
        //factory.setBatchListener(true);
        return factory;
    }


}
