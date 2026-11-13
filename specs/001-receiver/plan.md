# Plano 001 — Receiver

## 1. Resumo
Serviço HTTP sem estado próprio. Ordem do processamento:
1. valida o contrato;
2. aplica rate limit;
3. reserva o `transactionId` no Redis;
4. publica no Kafka e responde `202`.

A garantia de processamento único **não** está aqui: está no motor (004). O Redis é só um escudo de carga (ADR-10).

## 2. Contexto técnico
| Item | Valor |
|---|---|
| Framework | Spring Boot 4.0.x, Spring MVC com **virtual threads** (`spring.threads.virtual.enabled=true`) |
| Kafka | `KafkaProducer` nativo: `enable.idempotence=true`, `acks=all`, `linger.ms=5`, `compression.type=zstd`, `delivery.timeout.ms=2000` |
| Redis | Lettuce, `commandTimeout=5ms` |
| Rate limit | Bucket4j em memória, um bucket por `X-Client-Id` |
| Validação | `Schemas.validate` (módulo `contracts`) + invariantes de domínio |
| Porta | 8081 |

**Por que MVC + virtual threads e não WebFlux:** o código fica imperativo e simples de testar, e as chamadas bloqueantes (aguardar o ack do Kafka) não ocupam threads de plataforma. O throughput é suficiente para 8k–25k TPS com 2–4 instâncias.

## 3. Clean Architecture
```
receiver/src/main/java/br/com/motorfraude/receiver/
├── domain/
│   ├── TransactionCommand.java        # record + invariantes (valor > 0, BRL, occurredAt ≤ now+5min)
│   └── ReceiveResult.java             # sealed: Accepted, Duplicate, Rejected, RateLimited, Unavailable
├── application/
│   ├── ReceiveTransactionUseCase.java
│   └── port/out/
│       ├── DuplicateGuard.java        # Reservation tryReserve(String txId); void release(String txId)
│       ├── CommandPublisher.java      # void publish(TransactionCommand, TraceContext) throws PublishFailed
│       └── RateLimiter.java           # boolean tryAcquire(String clientId)
├── adapters/
│   ├── in/rest/TransactionController.java     # HTTP ↔ caso de uso; Problem Details
│   ├── in/rest/RequestValidator.java          # JSON Schema
│   ├── out/redis/RedisDuplicateGuard.java     # SET NX EX 600; timeout → UNAVAILABLE
│   ├── out/kafka/KafkaCommandPublisher.java   # send().get(timeout)
│   └── out/ratelimit/Bucket4jRateLimiter.java
└── infrastructure/
    ├── ReceiverApplication.java
    ├── config/{KafkaConfig,RedisConfig,RateLimitConfig}.java
    └── observability/ReceiverMetrics.java
```

### Fluxo do caso de uso
```
execute(cmd, clientId, trace):
  if !rateLimiter.tryAcquire(clientId)          -> RateLimited
  r = duplicateGuard.tryReserve(cmd.transactionId)
  if r == ALREADY_SEEN                          -> Duplicate
  // r == RESERVED ou UNAVAILABLE (fail-open)
  try publisher.publish(cmd, trace)             -> Accepted
  catch PublishFailed:
     if r == RESERVED: duplicateGuard.release(txId)   (melhor esforço)
     -> Unavailable
```
A validação de contrato acontece no adapter de entrada, antes do caso de uso. O domínio aplica as invariantes ao construir `TransactionCommand`.

## 4. API
`POST /v1/transactions:evaluate`
- **Headers:** `Content-Type: application/json`, `Idempotency-Key` (obrigatório), `X-Client-Id` (obrigatório), `traceparent` (opcional)
- **Respostas:**

| Status | Corpo | Quando |
|---|---|---|
| 202 | `{"transactionId","status":"ACCEPTED"}` | Publicado |
| 202 | `{"transactionId","status":"DUPLICATE"}` | Já visto no Redis |
| 400 | Problem Details com `errors[]` | Contrato ou invariante inválido |
| 413 | Problem Details | Corpo > 16 KB |
| 429 | Problem Details + `Retry-After: 1` | Rate limit |
| 503 | Problem Details + `Retry-After: 1` | Kafka indisponível |

**Mensagem Kafka:** tópico `transaction-commands.v1`, chave `accountId`, valor = corpo normalizado, headers `traceparent`, `receivedAt`, `schemaVersion=1`, `commandType=EvaluateTransaction`.

## 5. Observabilidade
| Métrica | Tipo | Labels |
|---|---|---|
| `receiver_requests_total` | counter | `status`, `client` |
| `receiver_duplicates_blocked_total` | counter | — |
| `receiver_redis_failopen_total` | counter | `reason` (timeout/erro) |
| `receiver_publish_seconds` | histogram | — |
| `receiver_redis_release_failures_total` | counter | — |

**Readiness** falha se o produtor Kafka não tiver metadata do tópico. O Redis **não** entra na readiness, porque o receiver é fail-open.

## 6. Testes
| Nível | O que cobre |
|---|---|
| Unitário (caso de uso, portas em memória) | as 5 saídas do `ReceiveResult`; `release` só quando `RESERVED` |
| Unitário (domínio) | invariantes de `TransactionCommand` |
| Adapter REST (MockMvc) | status e Problem Details de cada cenário |
| Integração (Testcontainers Kafka + Redis) | H1, H3; Redis parado → fail-open; Kafka parado → 503 + chave apagada |
| Concorrência | 50 requisições simultâneas com o mesmo `transactionId` → exatamente 1 publicada |
| ArchUnit | regra de dependência |

## 7. Constitution Check
- **I:** ✅ camadas e ArchUnit.
- **II:** ✅ testes antes, por tarefa.
- **III:** ✅ `Idempotency-Key` + Redis + dedup no motor.
- **IV:** ✅ o Redis é a única dependência auxiliar no caminho, e é fail-open.
- **VI:** ✅ só tokens no corpo, schema barra campos extras.
- **VII:** ✅ métricas e trace.

## 8. Riscos e trade-offs
- **Rate limit por instância:** a soma varia com o número de pods. Aceito para o case.
- **`SET NX` antes de publicar:** se o processo morrer entre o `SET` e a publicação, a chave fica presa por 10 min e um retry legítimo é barrado como `DUPLICATE`. Mitigação: TTL curto + métrica. Alternativa rejeitada: publicar antes e marcar depois (abre janela para duplicatas). Ver risco registrado na spec técnica §10.
