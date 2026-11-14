# Tarefas 001 — Receiver

Base de caminhos: `receiver/src/{main,test}/java/br/com/motorfraude/receiver/`

## Fase 1 — Setup
- [x] T101 Criar o módulo `receiver` (Spring Boot, dependências de `contracts` e `platform-commons`; Actuator e Micrometer Tracing com ponte OpenTelemetry para o `traceparent` HTTP), `application.yml` com valores locais, `logback-spring.xml` incluindo o `logback-base.xml` do commons e o teste ArchUnit usando `CleanArchitectureRules`
- [x] T102 [P] Adicionar o serviço `receiver` ao `docker-compose.yml` (porta 8081, depende de kafka e redis) e ao `prometheus.yml` — o alvo já estava no `prometheus.yml` (000); o serviço entra no perfil `app` do compose (`docker compose --profile app up -d --build receiver`, depois de `./mvnw -pl receiver -am package -DskipTests`), para o `docker compose up -d` da infraestrutura continuar sem build (SC-001 da 000)

## Fase 2 — Domínio e caso de uso (sem infraestrutura)
- [x] T103 **Teste** das invariantes de `TransactionCommand` (valor ≤ 0, moeda ≠ BRL, `occurredAt` no futuro) — `domain/TransactionCommandTest.java`
- [x] T104 Implementar `TransactionCommand` e `ReceiveResult` (sealed) — `domain/`
- [x] T105 Criar as portas `DuplicateGuard` (`tryReserve`/`confirm`/`release`), `CommandPublisher` e `RateLimiter`, com fakes em memória nos testes — `application/port/out/`, `test/.../fakes/`
- [x] T106 **Testes** do `ReceiveTransactionUseCase`: aceito (com `confirm`), duplicata (`DONE`), em andamento (`PENDING` → `InFlight`), rate limit, Redis indisponível (fail-open, sem `confirm`), falha de publicação com `release` só quando `RESERVED` e sem `confirm` — `application/ReceiveTransactionUseCaseTest.java`
- [x] T107 Implementar `ReceiveTransactionUseCase`. Ao concluir, **remover** `receiver/src/test/resources/archunit.properties` (relaxamento temporário da T101: as regras ArchUnit voltam a exigir classes em `domain` e `application`)

## Fase 3 — H1 + H2: entrada HTTP e publicação (MVP)
- [x] T108 **Testes** MockMvc: 202 ACCEPTED, 400 por schema, 400 por `Idempotency-Key` divergente, 413, 401 para `X-Client-Id` ausente/desconhecido (sem criar label de métrica) — `adapters/in/rest/TransactionControllerTest.java`
- [x] T109 Implementar `RequestValidator` (JSON Schema) e `TransactionController` com Problem Details
- [x] T110 **Teste de integração** com Testcontainers Kafka: o comando chega ao tópico com chave e headers corretos, e o valor passa no teste de contrato `ContractAssertions.assertProduces("evaluate-transaction.v1", …)` (000 plan §3.3) — `adapters/out/kafka/KafkaCommandPublisherIT.java`
- [x] T111 Implementar `KafkaCommandPublisher` (`acks=all`, idempotente, `send().get(timeout)`) e `KafkaConfig`
- [x] T112 Propagar `traceparent` e gravar `receivedAt`. O Micrometer Tracing já lê (ou gera) o `traceparent` da requisição HTTP; o publicador grava esse contexto no header Kafka com `KafkaTracePropagation` e o `receivedAt` com `ClockProvider`

**Checkpoint:** o receiver aceita e publica. Já dá para demonstrar o passo 1 de ponta a ponta.

## Fase 4 — H3 + H5: duplicatas e falha do Kafka
- [x] T113 **Teste de integração** com Testcontainers Redis: reserva grava `PENDING` com TTL 5 s; segunda chamada antes do `confirm` devolve `IN_FLIGHT`; depois do `confirm` (`DONE`, TTL 600) devolve `ALREADY_SEEN`; `PENDING` expira em 5 s e a reserva volta a funcionar; timeout devolve `UNAVAILABLE` — `adapters/out/redis/RedisDuplicateGuardIT.java`
- [x] T114 Implementar `RedisDuplicateGuard` (e **remover** o `DuplicateGuard` provisório de `TemporaryPortsConfig`) (timeout de 5 ms por comando; `SET PENDING NX EX 5` + `GET`; `confirm` com `SET DONE EX 600`; `release` com `DEL`). `PENDING` TTL > `delivery.timeout.ms`
- [x] T115 **Teste de aceite** H3/H5: reenvio depois do ack → DUPLICATE; reenvio durante a publicação → 503 IN_FLIGHT (nunca DUPLICATE); Redis parado → fail-open; Kafka parado → 503 e retry aceito depois que o Kafka volta — `AcceptanceIT.java`
- [x] T116 **Teste de concorrência:** 50 POSTs simultâneos com o mesmo `transactionId` → 1 publicação; os demais recebem `IN_FLIGHT` ou `DUPLICATE`, nenhum `ACCEPTED`

## Fase 5 — H4: rate limit
- [x] T117 **Teste** do `Bucket4jRateLimiter` (excede → false; origens isoladas) e do filtro: 429 com `Retry-After` antes de validar o corpo (corpo inválido de origem acima do limite → 429, não 400) — limite baixo (5 req/s) nos testes
- [x] T118 Implementar `Bucket4jRateLimiter` (e **remover** o `RateLimiter` provisório de `TemporaryPortsConfig`; o filtro passa a limitar antes do corpo, então tirar a chamada duplicada de `tryAcquire` do caso de uso ou do filtro) + `RateLimitConfig` (limite via `RECEIVER_RATE_LIMIT_PER_SECOND`, padrão 20000; lista `receiver.clients`) + filtro de origem/rate limit antes da leitura do corpo + resposta 429 com `Retry-After`

## Fase 6 — Observabilidade e acabamento
- [x] T119 `ReceiverMetrics` com as métricas do plan §5 (inclui `in_flight` e `confirm_failures`) + teste de que os contadores aumentam
- [x] T120 Readiness: health indicator do produtor Kafka (sem o Redis)
- [x] T121 Painel do receiver no dashboard Grafana (RPS por status, duplicatas, fail-open, p99 da publicação)
- [ ] T122 (requer a Fase 5 da 000, o gerador de carga) Rodar `load-generator --tps 8000 --duration 120s` e registrar no README do serviço o p99 e o TPS alcançados (SC-001)

## Dependências
T101 → Fase 2 → Fase 3 → (Fases 4 e 5 em paralelo) → Fase 6.
