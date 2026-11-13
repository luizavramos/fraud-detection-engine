# Tarefas 001 — Receiver

Base de caminhos: `receiver/src/{main,test}/java/br/com/motorfraude/receiver/`

## Fase 1 — Setup
- [ ] T101 Criar o módulo `receiver` (Spring Boot, dependências de `contracts` e `platform-commons`; Actuator e Micrometer Tracing com ponte OpenTelemetry para o `traceparent` HTTP), `application.yml` com valores locais, `logback-spring.xml` incluindo o `logback-base.xml` do commons e o teste ArchUnit usando `CleanArchitectureRules`
- [ ] T102 [P] Adicionar o serviço `receiver` ao `docker-compose.yml` (porta 8081, depende de kafka e redis) e ao `prometheus.yml`

## Fase 2 — Domínio e caso de uso (sem infraestrutura)
- [ ] T103 **Teste** das invariantes de `TransactionCommand` (valor ≤ 0, moeda ≠ BRL, `occurredAt` no futuro) — `domain/TransactionCommandTest.java`
- [ ] T104 Implementar `TransactionCommand` e `ReceiveResult` (sealed) — `domain/`
- [ ] T105 Criar as portas `DuplicateGuard`, `CommandPublisher` e `RateLimiter`, com fakes em memória nos testes — `application/port/out/`, `test/.../fakes/`
- [ ] T106 **Testes** do `ReceiveTransactionUseCase`: aceito, duplicata, rate limit, Redis indisponível (fail-open), falha de publicação com `release` só quando `RESERVED` — `application/ReceiveTransactionUseCaseTest.java`
- [ ] T107 Implementar `ReceiveTransactionUseCase`

## Fase 3 — H1 + H2: entrada HTTP e publicação (MVP)
- [ ] T108 **Testes** MockMvc: 202 ACCEPTED, 400 por schema, 400 por `Idempotency-Key` divergente, 413 — `adapters/in/rest/TransactionControllerTest.java`
- [ ] T109 Implementar `RequestValidator` (JSON Schema) e `TransactionController` com Problem Details
- [ ] T110 **Teste de integração** com Testcontainers Kafka: o comando chega ao tópico com chave e headers corretos, e o valor passa no teste de contrato `ContractAssertions.assertProduces("evaluate-transaction.v1", …)` (000 plan §3.3) — `adapters/out/kafka/KafkaCommandPublisherIT.java`
- [ ] T111 Implementar `KafkaCommandPublisher` (`acks=all`, idempotente, `send().get(timeout)`) e `KafkaConfig`
- [ ] T112 Propagar `traceparent` e gravar `receivedAt`. O Micrometer Tracing já lê (ou gera) o `traceparent` da requisição HTTP; o publicador grava esse contexto no header Kafka com `KafkaTracePropagation` e o `receivedAt` com `ClockProvider`

**Checkpoint:** o receiver aceita e publica. Já dá para demonstrar o passo 1 de ponta a ponta.

## Fase 4 — H3 + H5: duplicatas e falha do Kafka
- [ ] T113 **Teste de integração** com Testcontainers Redis: `SET NX` reserva, segunda chamada devolve `ALREADY_SEEN`, timeout devolve `UNAVAILABLE` — `adapters/out/redis/RedisDuplicateGuardIT.java`
- [ ] T114 Implementar `RedisDuplicateGuard` (timeout de 5 ms, `release` com `DEL`)
- [ ] T115 **Teste de aceite** H3/H5: reenvio → DUPLICATE; Redis parado → fail-open; Kafka parado → 503 e retry aceito depois que o Kafka volta — `AcceptanceIT.java`
- [ ] T116 **Teste de concorrência:** 50 POSTs simultâneos com o mesmo `transactionId` → 1 publicação

## Fase 5 — H4: rate limit
- [ ] T117 **Teste** do `Bucket4jRateLimiter` (excede → false; origens isoladas)
- [ ] T118 Implementar `Bucket4jRateLimiter` + `RateLimitConfig` (limite via variável de ambiente) + resposta 429 com `Retry-After`

## Fase 6 — Observabilidade e acabamento
- [ ] T119 `ReceiverMetrics` com as métricas do plan §5 + teste de que os contadores aumentam
- [ ] T120 Readiness: health indicator do produtor Kafka (sem o Redis)
- [ ] T121 Painel do receiver no dashboard Grafana (RPS por status, duplicatas, fail-open, p99 da publicação)
- [ ] T122 (requer a Fase 5 da 000, o gerador de carga) Rodar `load-generator --tps 8000 --duration 120s` e registrar no README do serviço o p99 e o TPS alcançados (SC-001)

## Dependências
T101 → Fase 2 → Fase 3 → (Fases 4 e 5 em paralelo) → Fase 6.
