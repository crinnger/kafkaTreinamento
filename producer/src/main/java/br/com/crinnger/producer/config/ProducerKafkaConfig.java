package br.com.crinnger.producer.config;

import br.com.crinnger.producer.model.Person;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

//@Configuration
public class ProducerKafkaConfig {

    public static final String TOPIC1="topic-1";
    public static final String TOPICPERSON="json-person";
    public static final String TOPICCITY ="json-city";

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public ProducerFactory<String,String> stringProducerFactory(){
        var configs = new HashMap<String,Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    public KafkaTemplate<String,String> stringKafkaTemplate(){
        return new KafkaTemplate<>(stringProducerFactory());
    }

    @Bean
    public ProducerFactory<Integer, Person> personProducerFactory(){
        var configs = new HashMap<String,Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        //return new DefaultKafkaProducerFactory<>(configs,new IntegerSerializer(),new JsonSerializer<>());
        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    public KafkaTemplate<Integer,Person> kafkaTemplatePerson(){
        return new KafkaTemplate<Integer,Person>(personProducerFactory());
    }

    // Producer generico para os tipos mas com as mesma classe de serializacao
    @Bean
    public ProducerFactory genericProducerFactory(){
        var configs = new HashMap<String,Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        //return new DefaultKafkaProducerFactory<>(configs,new IntegerSerializer(),new JsonSerializer<>());
        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    public KafkaTemplate genericKafkaTemplate(){
        return new KafkaTemplate(genericProducerFactory());
    }

    // obrigatorio se quiser criar topico pela aplicacao
    @Bean
    public KafkaAdmin kafkaAdmin(){
        var configs = new HashMap<String,Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic newTopic(){
        return new NewTopic( TOPIC1,10, Short.parseShort("1"));
    }

    @Bean
    public NewTopic newTopicPerson(){
        return TopicBuilder.name(TOPICPERSON)
                .partitions(10)
                .build();
    }

    @Bean
    public NewTopic newTopicCity(){
        return TopicBuilder.name(TOPICCITY)
                .partitions(2)
                .build();
    }
}
