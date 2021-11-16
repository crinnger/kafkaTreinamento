package br.com.crinnger.producer.config;

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
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

// abordagem para quando tem varias topics sendo produzidos de tipos diferentes e formas diferentes de serializacao

@Configuration
public class ProducerKafkaConfigGenerico {

    public static final String TOPIC1="topic-1";
    public static final String TOPICPERSON="json-person";
    public static final String TOPICCITY ="json-city";

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public RoutingKafkaTemplate routingKafkaTemplate(GenericApplicationContext context,
                                                     ProducerFactory producerFactory){
        var jsonProducerFactory = jsonProducerFactory();
        context.registerBean(DefaultKafkaProducerFactory.class, "jsonPF", jsonProducerFactory);
        Map<Pattern,ProducerFactory<Object,Object>> map = new LinkedHashMap<>();
        map.put(Pattern.compile("topic" +".*") , stringProducerFactory());
        map.put(Pattern.compile("json-.*"), jsonProducerFactory());
        return new RoutingKafkaTemplate(map);
    }


    public ProducerFactory jsonProducerFactory(){
        var configs = new HashMap<String,Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        //return new DefaultKafkaProducerFactory<>(configs,new IntegerSerializer(),new JsonSerializer<>());
        return new DefaultKafkaProducerFactory<>(configs);
    }


    public ProducerFactory stringProducerFactory(){
        var configs = new HashMap<String,Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configs);
    }

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
