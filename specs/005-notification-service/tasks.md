# Tarefas 005 — Notificações

Base de caminhos: `notification-service/src/{main,test}/java/br/com/motorfraude/notification/`

## Fase 1 — Setup
- [x] T501 Criar o módulo `notification-service` (Spring Boot, virtual threads) + teste ArchUnit
- [x] T502 [P] Simuladores WireMock no compose (`antifraud-legacy-mock`, `push-provider-mock`, `email-provider-mock`, `sms-provider-mock`, `contacts-mock`), com *mappings* de sucesso e cenários de falha (503, lentidão) — `infra/mocks/`
- [x] T503 [P] Adicionar o serviço ao compose (porta 8085) e ao Prometheus

## Fase 2 — Domínio e casos de uso (H1, H2, H3, H5)
- [x] T504 **Testes** de `ChannelPolicy` e `SuppressionPolicy` — `domain/*Test.java`
- [x] T505 Implementar `Alert`, `Delivery`, `Channel`, `DeliveryStatus`, `ChannelPolicy` e `SuppressionPolicy`
- [x] T506 Portas `AlertRepository`, `DeliveryRepository`, `NotificationChannel`, `ContactDirectory` e `RetryScheduler`, com fakes
- [x] T507 **Testes** de `RegisterAlertUseCase` (idempotente; entregas por severidade; supressão) e de `DispatchDeliveryUseCase` (SENT não reenvia; falha → retry; esgotou → DLQ; PUSH HIGH esgotado → SMS; contato ausente → FAILED sem retry)
- [x] T508 Implementar os dois casos de uso

## Fase 3 — Persistência
- [x] T509 Flyway `V1__schema.sql` (plan §4) + **teste de integração** (Testcontainers Postgres) do `ON CONFLICT DO NOTHING` e da consulta de supressão
- [x] T510 Implementar os repositórios JDBC

## Fase 4 — Canais (H1, H2, H4)
- [x] T511 **Testes** (WireMock) do `AntifraudWebhookChannel`: HMAC-SHA256 válido, `Idempotency-Key`, timeout
- [x] T512 Implementar `AntifraudWebhookChannel`
- [x] T513 [P] **Testes** + implementação de `PushChannel`, `EmailChannel` e `SmsChannel` (mesma interface; `Idempotency-Key = alertId:channel`)
- [x] T514 [P] **Testes** + implementação do `HttpContactDirectory`
- [x] T515 Configuração Resilience4j por canal (timeout, retry, circuit breaker, bulkhead) + **teste**: circuit abre com 503 e fecha depois

## Fase 5 — Consumo e retry (MVP de ponta a ponta)
- [x] T516 **Teste de integração:** alerta no tópico → linha em `alerts` → webhook chamado (H1) e push/e-mail em HIGH (H2) — `AlertFlowIT.java`
- [x] T517 Implementar `AlertListener` (`read_committed`, ack depois do commit, despacho em virtual threads)
- [x] T518 **Teste:** falha de canal → mensagem em `alerts.retry.1m` → reprocesso no vencimento → `.5m` → `.30m` → `alerts.dlq` (tempos reduzidos no teste)
- [x] T519 Implementar `KafkaRetryScheduler` e `RetryListener` (`nack` até `dueAt`)
- [x] T520 Implementar o reconciliador de entregas `PENDING` antigas + teste

**Checkpoint:** alertas do motor chegam ao antifraude e ao cliente.

## Fase 6 — H3/H4 provas e H6 API
- [x] T521 **Teste SC-002:** reentregar 1.000 alertas → 0 chamadas duplicadas nos simuladores (WireMock `verify`)
- [x] T522 **Teste SC-003:** push em 503 por 5 min (tempo reduzido) → 100% dos webhooks entregues
- [x] T523 **Testes** + implementação de `GET /v1/alerts` (cursor) e `POST /v1/alerts/{id}/feedback`

## Fase 7 — Observabilidade
- [ ] T524 Métricas do plan §5 + painel no Grafana (entregas por canal e resultado, estado dos circuit breakers, DLQ, latência alerta→entrega)

## Dependências
Fase 1 → Fase 2 → Fase 3 → Fase 4 → Fase 5 → (Fases 6 e 7).
Para testar antes do motor existir, publicar `FraudAlert` de exemplo direto no tópico (usar os exemplos de `contracts`).
