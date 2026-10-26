package com.example.kyc;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.cibseven.bpm.engine.RuntimeService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/** Hört auf alle Topics aus kafka-bpm.inbound und korreliert die zugeordnete BPMN-Message. */
@Component
public class KafkaInboundListener {

  private final RuntimeService runtimeService;
  private final JsonMapper jsonMapper;
  private final Map<String, String> messageByTopic;

  public KafkaInboundListener(RuntimeService runtimeService, JsonMapper jsonMapper, KafkaBpmProperties properties) {
    this.runtimeService = runtimeService;
    this.jsonMapper = jsonMapper;
    this.messageByTopic = properties.messageByTopic();
  }

  public String[] getTopics() {
    return messageByTopic.keySet().toArray(String[]::new);
  }

  @KafkaListener(topics = "#{__listener.topics}")
  public void onMessage(ConsumerRecord<String, String> record) {
    // shortcut: nur flaches JSON (Strings, Zahlen, Booleans); verschachtelte Objekte bräuchten Spin/JSON-Variablen
    Map<String, Object> variables = jsonMapper.readValue(record.value(), new TypeReference<>() {});

    // correlate() bedient Message-Start-Events (Key wird Business Key) und wartende Zwischenereignisse (Key filtert)
    runtimeService.createMessageCorrelation(messageByTopic.get(record.topic()))
        .processInstanceBusinessKey(record.key())
        .setVariables(variables)
        .correlate();
  }
}
