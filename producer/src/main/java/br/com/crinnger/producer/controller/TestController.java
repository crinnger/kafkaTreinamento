package br.com.crinnger.producer.controller;

import br.com.crinnger.producer.config.ProducerKafkaConfig;
import br.com.crinnger.producer.model.City;
import br.com.crinnger.producer.model.Person;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.stream.IntStream;

@RestController
@RequiredArgsConstructor
public class TestController {

    //private final KafkaTemplate<String,String> kafkaTemplate;
    //private final KafkaTemplate<Integer, Person> kafkaTemplatePerson;
    //private final KafkaTemplate<Integer, Object> genericKafkaTemplate;
    private final RoutingKafkaTemplate routingKafkaTemplate;

    @GetMapping("send")
    public ResponseEntity<?> send(){
        IntStream.range(1,50)
                .boxed()
             //  .forEach(n -> kafkaTemplate.send(ProducerKafkaConfig.TOPIC1, "Num=" + n ,"Numero: " + n + " Envio em " + LocalDateTime.now()));
                .forEach(n -> routingKafkaTemplate.send(ProducerKafkaConfig.TOPIC1, "Num=" + n ,"Numero: " + n + " Envio em " + LocalDateTime.now()));
        return ResponseEntity.ok().build();
    }

    @GetMapping("sendPerson")
    public ResponseEntity<?> sendPerson(){
        IntStream.range(1,30)
                .boxed()
                .forEach(n ->{
                    //kafkaTemplatePerson.send(ProducerKafkaConfig.TOPICPERSON,  n ,
                    routingKafkaTemplate.send(ProducerKafkaConfig.TOPICPERSON,  n ,
                            Person.builder().name("Teste " + n).age(n).build());
                } );
        return ResponseEntity.ok().build();
    }

    @GetMapping("sendGeneric")
    public ResponseEntity<?> sendGeneric(){
        //genericKafkaTemplate.send(ProducerKafkaConfig.TOPICPERSON,  1 ,
        //                    Person.builder().name("Person Teste " + 1).age(1).build());
        //genericKafkaTemplate.send(ProducerKafkaConfig.TOPICCITY,  1 ,
        //        City.builder().nome("City Teste " + 1).uf("am").build());

        routingKafkaTemplate.send(ProducerKafkaConfig.TOPICPERSON,  2 ,
                Person.builder().name("Person Routing " + 2).age(1).build());
        routingKafkaTemplate.send(ProducerKafkaConfig.TOPICCITY,  2 ,
                City.builder().nome("City Routing " + 2).uf("am").build());
        routingKafkaTemplate.send(ProducerKafkaConfig.TOPIC1,  "Chave" ,
                "msg routing 3");

        return ResponseEntity.ok().build();
    }

}
