package com.example.kyc;

import java.util.Map;

import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.JavaDelegate;
import org.cibseven.bpm.model.bpmn.instance.MessageEventDefinition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * An ein Message-End- oder Throw-Event hängen: camunda:delegateExpression="${kafkaPublisher}".
 * Das Topic kommt aus kafka-bpm.outbound anhand des Message-Namens des Events.
 */
@Component("kafkaPublisher")
public class KafkaPublisher implements JavaDelegate {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final JsonMapper jsonMapper;
  private final Map<String, String> topicByMessage;

  public KafkaPublisher(KafkaTemplate<String, String> kafkaTemplate, JsonMapper jsonMapper, KafkaBpmProperties properties) {
    this.kafkaTemplate = kafkaTemplate;
    this.jsonMapper = jsonMapper;
    this.topicByMessage = properties.topicByMessage();
  }

  @Override
  public void execute(DelegateExecution execution) {
    String message = execution.getBpmnModelElementInstance()
        .getChildElementsByType(MessageEventDefinition.class).iterator().next()
        .getMessage().getName();
    String topic = topicByMessage.get(message);
    if (topic == null) {
      throw new IllegalStateException("Kein Topic in kafka-bpm.outbound für Message '" + message + "'");
    }

    String payload = jsonMapper.writeValueAsString(execution.getVariables());
    // join(): schlägt das Senden fehl, rollt die Engine zurück und der Task bleibt offen
    kafkaTemplate.send(topic, execution.getProcessBusinessKey(), payload).join();
  }
}
