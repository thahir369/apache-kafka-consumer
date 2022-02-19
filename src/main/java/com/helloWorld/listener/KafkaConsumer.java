package com.helloWorld.listener;

import com.helloWorld.dto.Student;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumer {

  @KafkaListener(
      topics = "${spring.kafka.topic}",
      containerFactory = "kafkaListenerContainerFactory")
  public void getUserData(Student data, Acknowledgment acknowledgment) {
    log.info("Data received :: " + data);
  }
}
