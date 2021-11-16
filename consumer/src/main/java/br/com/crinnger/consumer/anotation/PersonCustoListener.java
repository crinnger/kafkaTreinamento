package br.com.crinnger.consumer.anotation;

import br.com.crinnger.consumer.config.ConsumerKafkaConfig;
import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@KafkaListener
public @interface PersonCustoListener {
    @AliasFor(annotation = KafkaListener.class, attribute = "groupID")
    String groupId() default "";

    @AliasFor(annotation = KafkaListener.class, attribute = "topics")
    String[] topics() default ConsumerKafkaConfig.TOPICPERSON;

    @AliasFor(annotation = KafkaListener.class, attribute = "containerFactory")
    String containerFactory() default "personKafkaListenerContainerFactory";
}
