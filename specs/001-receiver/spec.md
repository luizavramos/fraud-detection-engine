# Spec 001 — Receiver

**Status:** pronta para implementação · **Prioridade:** P1 · **Depende de:** 000
**Passos no desenho:** 1, 1.1, 1.2, 1.3, 1.4

## Contexto
Os sistemas de pagamento do banco chamam por HTTP. O receiver é a porta de entrada. Ele precisa:
- responder rápido e nunca fazer a origem esperar pela detecção;
- barrar lixo, excesso de chamadas e retries óbvios antes que cheguem ao núcleo;
- garantir que tudo o que ele aceita esteja gravado no Kafka.

## Histórias de usuário

### H1 — Aceitar uma transação válida (P1)
Como sistema de pagamento, quero enviar uma transação e receber a confirmação de que ela será avaliada, para seguir meu fluxo sem esperar a detecção.

**Teste independente:** um POST válido devolve `202`, e o comando aparece em `transaction-commands.v1` com chave `accountId` e headers `traceparent`, `receivedAt` e `commandType=EvaluateTransaction`.

**Cenários de aceite**
1. **Given** uma transação válida nunca vista, **When** faço `POST /v1/transactions:evaluate`, **Then** recebo `202` com corpo `{"transactionId": "...", "status": "ACCEPTED"}`, e o comando está no tópico.
2. **Given** o Kafka confirmou a gravação (`acks=all`), **When** o receiver responde, **Then** a resposta só sai depois dessa confirmação.

### H2 — Rejeitar contrato inválido (P1)
Como dono do núcleo de detecção, quero que transações malformadas sejam barradas na entrada, para que lixo nunca chegue ao Kafka.

**Cenários de aceite**
1. **Given** um corpo sem `transactionId`, **When** faço POST, **Then** recebo `400` em formato *Problem Details* (RFC 9457), apontando o campo, e nada é publicado.
2. **Given** `amount.value` ≤ 0 ou decimal, **When** faço POST, **Then** recebo `400`.
3. **Given** o header `Idempotency-Key` diferente do `transactionId` do corpo, **When** faço POST, **Then** recebo `400`.

### H3 — Barrar retries óbvios (P1)
Como dono do núcleo, quero que reenvios da mesma transação não gerem carga extra, para proteger o motor em rajadas de retry.

**Cenários de aceite**
1. **Given** a transação `T1` já aceita há menos de 10 min, **When** a origem reenvia `T1`, **Then** recebo `202` com `status: "DUPLICATE"`, e nada é publicado de novo.
2. **Given** o Redis está fora do ar, **When** envio `T1` duas vezes, **Then** as duas são aceitas e publicadas (*fail-open*), e a métrica `receiver_redis_failopen_total` aumenta. O motor resolve a duplicata.

### H4 — Limitar excesso de chamadas por origem (P2)
Como responsável por SRE, quero limitar a taxa por sistema de origem, para que um cliente com defeito não derrube a entrada.

**Cenários de aceite**
1. **Given** a origem `pix-core` com limite de N req/s, **When** ela excede N, **Then** recebe `429` com header `Retry-After`, e as outras origens não são afetadas.

### H5 — Não aceitar o que não foi gravado (P1)
Como sistema de pagamento, quero saber quando a transação **não** foi aceita, para tentar de novo.

**Cenários de aceite**
1. **Given** o Kafka está indisponível, **When** faço POST de `T2`, **Then** recebo `503` com `Retry-After`, e a chave `T2` é removida do Redis.
2. **Given** o cenário anterior, **When** a origem reenvia `T2` depois que o Kafka volta, **Then** recebo `202 ACCEPTED`. O retry não é barrado como duplicata.

## Casos de borda
- **Duas requisições simultâneas com o mesmo `transactionId`:** só uma ganha o `SET NX`. A outra recebe `DUPLICATE`.
- **Redis lento (acima de 5 ms):** o receiver trata como indisponível, deixa passar e registra a métrica.
- **Corpo acima de 16 KB:** `413`.

## Requisitos funcionais
- **FR-001:** DEVE expor `POST /v1/transactions:evaluate`, aceitando `application/json` no schema `evaluate-transaction.v1`.
- **FR-002:** DEVE validar o corpo contra o JSON Schema e as regras de domínio (valor > 0, moeda `BRL`, `occurredAt` até 5 min no futuro).
- **FR-003:** DEVE exigir o header `Idempotency-Key` igual ao `transactionId`.
- **FR-004:** DEVE checar duplicata com `SET tx:{transactionId} 1 NX EX 600` no Redis, com timeout de 5 ms; se falhar ou estourar o tempo, segue (*fail-open*).
- **FR-005:** DEVE publicar em `transaction-commands.v1` com chave `accountId`, `acks=all`, produtor idempotente, e só responder depois da confirmação.
- **FR-006:** se a publicação falhar, DEVE apagar a chave do Redis (melhor esforço) e responder `503`.
- **FR-007:** DEVE aplicar rate limit por origem, identificada pelo header `X-Client-Id` local (em produção, pelo certificado mTLS `[DOC]`).
- **FR-008:** DEVE gerar ou propagar `traceparent` e gravar `receivedAt` (instante de chegada) no header Kafka.
- **FR-009:** DEVE expor métricas: requisições por status e origem, duplicatas barradas, fail-open, latência da publicação.

## Entidades
`EvaluateTransaction` (contrato 000), `ReceiveResult` (ACCEPTED, DUPLICATE, REJECTED, RATE_LIMITED, UNAVAILABLE).

## Critérios de sucesso
- **SC-001:** p99 do receiver (requisição → resposta) ≤ 30 ms a 8.000 TPS, com até 2 instâncias locais.
- **SC-002:** 0 transações aceitas (`202 ACCEPTED`) que não estejam no tópico, num teste com o Kafka sendo derrubado durante a carga.
- **SC-003:** 100% dos reenvios dentro de 10 min com o Redis no ar respondem `DUPLICATE` sem publicar.

## Premissas
- Autenticação local por `X-Client-Id`. mTLS fica como `[DOC]`.
- Rate limit **por instância**, em memória (Bucket4j). O limite global aproximado é o limite por instância × número de instâncias. Rate limit distribuído é evolução `[DOC]`.
- [NEEDS CLARIFICATION] **Limite por origem:** *padrão adotado: 20.000 req/s por instância, configurável por variável de ambiente.*
