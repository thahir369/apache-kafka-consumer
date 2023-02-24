package com.halloWorld.config;

import com.halloWorld.dto.Student;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

  @Value("${spring.kafka.consumer.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.consumer.group-id}")
  private String consumerGroupId;

  @Bean
  public ConsumerFactory<String, Student> studentConsumerFactory() {
    Map<String, Object> config = new HashMap<>();

    config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    // to read data from beginning from a topic
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

    // below code is useful when error occurred while consuming data from topic
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
    ConcurrentKafkaListenerContainerFactory<String, Student> studentFactory =
        new ConcurrentKafkaListenerContainerFactory<>();
    studentFactory.setConsumerFactory(studentConsumerFactory());

    // to provide manual acknowledgment after consuming data from topic
    studentFactory
        .getContainerProperties()
        .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return studentFactory;
  }
}
