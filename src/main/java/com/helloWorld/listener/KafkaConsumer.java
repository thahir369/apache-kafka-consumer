package com.helloWorld.listener;

import com.helloWorld.dto.Student;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumer {

  @KafkaListener(
      groupId = "student-group",
      topics = "student-topic",
      containerFactory = "kafkaListenerContainerFactory")
  public void getUserData(Student data) {
    log.info("Data received :: " + data);
  }
}
