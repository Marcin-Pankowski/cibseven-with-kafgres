package com.example.kyc;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Zuordnung Kafka-Topic <-> BPMN-Message aus application.yaml (kafka-bpm.*). */
@ConfigurationProperties("kafka-bpm")
public record KafkaBpmProperties(List<Route> inbound, List<Route> outbound) {

  public record Route(String topic, String message) {}

  public KafkaBpmProperties {
    inbound = inbound == null ? List.of() : inbound;
    outbound = outbound == null ? List.of() : outbound;
  }

  /** Topic -> Message; doppelte Topics führen beim Start zu einem Fehler. */
  public Map<String, String> messageByTopic() {
    return inbound.stream().collect(Collectors.toMap(Route::topic, Route::message));
  }

  /** Message -> Topic; doppelte Messages führen beim Start zu einem Fehler. */
  public Map<String, String> topicByMessage() {
    return outbound.stream().collect(Collectors.toMap(Route::message, Route::topic));
  }
}
