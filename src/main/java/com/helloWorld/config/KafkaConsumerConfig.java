package com.helloWorld.config;

import com.helloWorld.dto.Student;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

  @Bean
  public ConsumerFactory<String, Student> studentConsumerFactory() {
    Map<String, Object> config = new HashMap<>();

    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "student-group");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final JsonDeserializer<Student> jsonDeserializer = new JsonDeserializer<>(Student.class);

    jsonDeserializer.setRemoveTypeHeaders(false);
    jsonDeserializer.addTrustedPackages("*");
    jsonDeserializer.setUseTypeMapperForKey(true);

    final ErrorHandlingDeserializer<Student> errorHandlingValueDeserializer =
        new ErrorHandlingDeserializer<>(jsonDeserializer);

    final ErrorHandlingDeserializer<String> errorHandlingKeyDeserializer =
        new ErrorHandlingDeserializer<>(new StringDeserializer());

    return new DefaultKafkaConsumerFactory<>(
        config, errorHandlingKeyDeserializer, errorHandlingValueDeserializer);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, Student> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, Student> userFactory =
        new ConcurrentKafkaListenerContainerFactory<>();
    userFactory.setConsumerFactory(studentConsumerFactory());
    return userFactory;
  }
}
