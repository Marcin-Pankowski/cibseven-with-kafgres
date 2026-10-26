# CIB seven with Kafgres

Beispiel: Ein BPMN-Prozess in **CIB seven 2.2** wird über eine Kafka-Nachricht gestartet, wartet auf einen User Task
und meldet sein Ergebnis wieder per Kafka. Der Kafka-Broker ist **[Kafgres](https://github.com/RayElg/kafgres)**,
eine PostgreSQL-Erweiterung: Dieselbe Postgres-Instanz ist Prozessdatenbank und Kafka-Broker.

```
kyc.approval.requested ──▶ Message-Start ──▶ User Task „KYC prüfen“ ──▶ Gateway ──▶ kyc.approval.approved
                                                                                └─▶ kyc.approval.rejected
```

| Komponente | Version |
|---|---|
| [CIB seven](https://cibseven.org) | 2.2.0 (Spring-Boot-4-Starter, Webapps + REST) |
| Spring Boot / Spring Kafka | 4.0.6 |
| Kafgres | 0.3.1 (PostgreSQL 16) |
| Java | 21 |

## Starten

Voraussetzungen: Docker, Java 21, Maven.

```bash
docker compose up -d --build --wait   # erster Build dauert einige Minuten (Kafgres wird aus dem Quellcode gebaut)
mvn spring-boot:run
```

| Was | Wo |
|---|---|
| CIB seven Webclient / Tasklist | http://localhost:8080/ bzw. http://localhost:8080/camunda/app/tasklist/ (Login `demo` / `demo`) |
| Kafka-Topics und Nachrichten (Redpanda Console) | http://localhost:8090/ |
| Kafka | `localhost:9092` |
| PostgreSQL | `localhost:5432` (Engine-DB `cibseven`, User `postgres` / `postgres`) |

## Ausprobieren

```bash
./send-kyc.sh                            # eine Anfrage mit Zufallsdaten
./send-kyc.sh 10                         # zehn Anfragen mit Zufallsdaten
./send-kyc.sh KYC-4711 "Max Mustermann"  # eine Anfrage mit eigenen Werten
```

Das Skript produziert per SQL (`kafgres_produce`) auf `kyc.approval.requested`: Kafka-Key = Business Key,
JSON-Value = Prozessvariablen. Den Task „KYC prüfen“ in der Tasklist abschließen, dann erscheint das Ergebnis
mit allen Prozessvariablen als JSON auf `kyc.approval.approved` oder `kyc.approval.rejected`
(in der Redpanda Console sichtbar).

## Topics mit BPMN-Messages verbinden

Ohne Code, in `src/main/resources/application.yaml`:

```yaml
kafka-bpm:
  inbound:                          # Topic → korreliert BPMN-Message (Start-Event oder wartendes Zwischenereignis)
    - topic: kyc.approval.requested
      message: KycApprovalRequested
  outbound:                         # BPMN-Message → Topic
    - message: KycApproved
      topic: kyc.approval.approved
    - message: KycRejected
      topic: kyc.approval.rejected
```

Im Modell bekommt ein Message-End- oder Throw-Event die Message und `camunda:delegateExpression="${kafkaPublisher}"`.
Schlägt das Senden fehl, rollt die Engine die Transaktion zurück und der Task bleibt offen.

- [KafkaInboundListener.java](src/main/java/com/example/kyc/KafkaInboundListener.java) – ein Listener für alle `inbound`-Topics
- [KafkaPublisher.java](src/main/java/com/example/kyc/KafkaPublisher.java) – JavaDelegate, Topic anhand des Message-Namens
- [kyc-approval.bpmn](src/main/resources/kyc-approval.bpmn) – der Prozess

Neue Topics: in [kafgres/init-topics.sql](kafgres/init-topics.sql) (nur beim ersten Start) oder
`SELECT kafgres_create_topic('name', 1);` – Kafgres legt Topics standardmäßig auch automatisch an.

## Hinweise

- **Kafgres ist sehr jung** (Version 0.3, seit September 2026). Für lokale Entwicklung und kleine Installationen
  sehr angenehm; für großen Produktivbetrieb würde ich noch abwarten.
- Kafgres hat kein KRaft-Quorum. Werkzeuge, die das abfragen, scheitern – z. B. **Kafbat Kafka UI** zeigt den Cluster
  als OFFLINE. Redpanda Console funktioniert.
- Kafgres meldet `127.0.0.1:9092` als Broker-Adresse. Clients müssen daher auf dem Host oder im Netzwerk des
  Kafgres-Containers laufen (siehe `network_mode` der Redpanda Console im `docker-compose.yml`).
- Eingehendes JSON wird nur flach in Prozessvariablen übernommen (Strings, Zahlen, Booleans).
- Das JWT-Secret in `application.yaml` ist ein **öffentliches Demo-Secret**. Für alles außer lokalen Tests per
  Umgebungsvariable `CIBSEVEN_WEBCLIENT_AUTHENTICATION_JWTSECRET` überschreiben.
- Alle Zugangsdaten (`demo`/`demo`, `postgres`/`postgres`, Postgres mit `trust`-Auth aus dem Kafgres-Image)
  sind nur für lokale Demos gedacht; Ports sind nur an `127.0.0.1` gebunden.
