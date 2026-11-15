# Plano 005 — Notificações

## 1. Resumo
Serviço Spring Boot, com I/O em **virtual threads**. O fluxo por alerta:
1. grava o alerta e as entregas por canal no Postgres (idempotente);
2. confirma o offset;
3. dispara cada canal de forma isolada.

Uma falha de canal vira um agendamento num tópico de retry. Resolve o problema de **efeito externo sem duplicata** com:
- chave única no banco;
- `Idempotency-Key` nos provedores;
- risco residual documentado.

## 2. Contexto técnico
| Item | Valor |
|---|---|
| Framework | Spring Boot 4.0.x, Spring Kafka, Spring MVC, Spring Data JDBC |
| Concorrência | `spring.threads.virtual.enabled=true`; envio por canal num executor de virtual threads |
| Resiliência | Resilience4j: `TimeLimiter` 2 s, `Retry` 3× (backoff exponencial + jitter), `CircuitBreaker` por canal, `Bulkhead` por canal |
| Banco | PostgreSQL 16, `alerts_db`, Flyway; `alerts` particionada por dia |
| Porta | 8085 |
| Simuladores | **Um** WireMock no compose (`provider-mocks`, porta 18090) com cinco simuladores lógicos por prefixo de caminho: `/antifraud-legacy`, `/push-provider`, `/email-provider`, `/sms-provider` e `/contacts`; falha por simulador com `infra/mocks/mock-control.sh` |

## 3. Clean Architecture
```
notification-service/src/main/java/br/com/motorfraude/notification/
├── domain/
│   ├── Alert.java, Delivery.java, Channel.java (ANTIFRAUD_WEBHOOK, PUSH, EMAIL, SMS)
│   ├── DeliveryStatus.java (PENDING, SENT, FAILED, SUPPRESSED)
│   ├── ChannelPolicy.java        # quais canais para cada severidade; SMS como reserva do PUSH
│   └── SuppressionPolicy.java    # janela por conta para canais do cliente
├── application/
│   ├── usecase/RegisterAlertUseCase.java     # grava alerta + entregas (idempotente), aplica supressão
│   ├── usecase/DispatchDeliveryUseCase.java  # envia uma entrega por um canal; decide SENT / retry / DLQ / reserva
│   ├── usecase/RegisterFeedbackUseCase.java
│   ├── usecase/ListAlertsUseCase.java
│   └── gateway/ AlertRepository, DeliveryRepository, NotificationChannel (strategy por canal),
│                 ContactDirectory, RetryScheduler, Clock
├── adapters/
│   ├── controller/kafka/AlertListener.java    # fraud-alerts.v1 → Register + Dispatch
│   ├── controller/kafka/RetryListener.java    # alerts.retry.* → espera o vencimento → Dispatch
│   ├── controller/rest/AlertController.java   # GET /v1/alerts, POST feedback
│   ├── gateway/postgres/…
│   ├── gateway/channels/{AntifraudWebhookChannel (HMAC), PushChannel, EmailChannel, SmsChannel}.java
│   ├── gateway/contacts/HttpContactDirectory.java
│   └── gateway/kafka/KafkaRetryScheduler.java
└── infrastructure/ config (Resilience4j, Kafka, segurança simples), métricas
```

### 3.1 Fluxo
```
AlertListener.onMessage(alert):
   RegisterAlertUseCase(alert)          # TX: insert alerts ON CONFLICT DO NOTHING
                                        #     insert deliveries por canal ON CONFLICT DO NOTHING
                                        #     supressão: canais do cliente → SUPPRESSED se houve HIGH da conta na janela
   ack()                                # offset só depois do commit no banco
   para cada entrega PENDING: executor.submit(DispatchDeliveryUseCase(delivery))

DispatchDeliveryUseCase(d):
   se d.status != PENDING → retorna (idempotente)
   contato = ContactDirectory.find(accountId)   (canais do cliente)
   resultado = channel.send(payload, idempotencyKey = alertId:channel)   # Resilience4j
   OK            → status SENT
   falha         → attempts+1; RetryScheduler.schedule(d, próximo atraso: 1m, 5m, 30m)
   esgotou       → status FAILED; publica alerts.dlq; se PUSH & HIGH → cria entrega SMS PENDING e despacha
```
**Por que confirmar o offset antes de enviar:** o alerta já está durável no banco, com entregas `PENDING`. Se o processo cair, um **reconciliador** (agendado a cada 1 min) reenvia as entregas `PENDING` com mais de 2 min. Assim o consumo do tópico não fica refém da lentidão de provedores.

### 3.2 Retry não bloqueante
A mensagem de retry é `{alertId, channel, attempt, dueAt}`, com chave `alertId`. O `RetryListener` de cada tópico, se `dueAt` ainda não chegou, faz `nack(dueAt - now)`, o que pausa só aquela partição. Quando vence, chama o `DispatchDeliveryUseCase`.

## 4. Modelo de dados (`V1__schema.sql`)
Igual à spec técnica §4.4: `alerts` (PK `alert_id`, `transaction_id` UNIQUE, particionada por `detected_at`), `notification_delivery` (PK `alert_id, channel`; `status`, `attempts`, `last_error`, `provider_msg_id`, `updated_at`), `alert_feedback`.
Índices: `alerts(account_token, detected_at desc)` para a consulta do analista e para a supressão; `notification_delivery(status, updated_at)` para o reconciliador.

## 5. Observabilidade
| Métrica | Labels |
|---|---|
| `notification_deliveries_total` | `channel`, `result` (sent/failed/suppressed/retry) |
| `notification_alert_to_delivery_seconds` (histogram) | `channel` |
| `resilience4j_circuitbreaker_state` | `name` (canal) |
| `notification_dlq_total` | `channel` |
| `notification_pending_reconciled_total` | — |
| `alerts_feedback_total` | `verdict` |

## 6. Testes
| Nível | O que cobre |
|---|---|
| Domínio | `ChannelPolicy` (MEDIUM só antifraude; HIGH antifraude+push+e-mail), `SuppressionPolicy` |
| Casos de uso (fakes) | idempotência do registro; `Dispatch` não reenvia `SENT`; sequência retry → DLQ → SMS; contato ausente → FAILED sem retry |
| Adapters de canal (WireMock) | HMAC correto, `Idempotency-Key`, timeout, circuit breaker abre e fecha |
| Integração (Testcontainers Kafka + Postgres + WireMock) | H1–H5 de ponta a ponta; reentrega de 1.000 alertas → 0 chamadas duplicadas (SC-002) |
| Caos | push-provider-mock em 503 por 5 min → webhook do antifraude segue (SC-003) |
| ArchUnit | regra de dependência |

## 7. Constitution Check
- **I/II:** ✅.
- **III:** ✅ PK por canal + `Idempotency-Key`.
- **IV:** ✅ fora do caminho quente do motor.
- **VI:** ✅ contato buscado sob demanda, não persistido; logs mascarados.
- **VII:** ✅.

## 8. Riscos
- **Provedor sem idempotência:** possibilidade (baixa) de aviso duplicado. Documentado + reconciliação diária `[DOC]`.
- **`nack` com espera longa (30 min) segura a partição do tópico de retry:** aceitável, porque cada tópico de retry contém só mensagens com o mesmo atraso.

## 9. Decisões da implementação (Fase 1)
- **Pacotes pela constituição:** `adapters/controller/{kafka,rest}` e `adapters/gateway/{postgres,channels,contacts,kafka}` no lugar de `in/` e `out/`.
- **Um WireMock, cinco simuladores lógicos:** cinco JVMs de WireMock não cabem no Docker local de 4 GB junto com o restante da pilha. Os simuladores são separados por prefixo de caminho, e a falha (503 ou lentidão de 5 s) liga e desliga **por simulador** com um estub de prioridade 1 criado pela API administrativa; o teste de fumaça (`infra/mocks/smoke.sh`) confere o isolamento (push em 503, e-mail e antifraude seguem aceitando). Nas Fases 4 e 6 os testes de canal usam WireMock próprio de teste (Testcontainers ou o do JUnit), não este.
- **Contrato dos simuladores** (os adaptadores da Fase 4 seguem estes caminhos): `POST /antifraud-legacy/webhooks/fraud-alerts` (devolve `caseId`), `POST /push-provider/v1/push`, `POST /email-provider/v1/email`, `POST /sms-provider/v1/sms` (devolvem `messageId` e ecoam o `Idempotency-Key`), `GET /contacts/v1/contacts/{accountToken}` (200 com `pushToken`, `email` e `phone` sintéticos; 404 para tokens `acc_tok_nocontact*`, o cenário `NO_CONTACT`).
- **Dependências por fase:** o módulo começa só com web, actuator e tracing; JDBC, Flyway, Kafka e Resilience4j entram junto com as fases que os usam (um starter de JDBC sem banco derrubaria o teste de subida).

## 10. Decisões da implementação (Fase 2)
- **Sem classe `Alert` de domínio:** o caso de uso e os adaptadores usam o `FraudAlert` do módulo `contracts` (como o motor faz com o `Decision`); uma cópia só repassaria os mesmos campos.
- **Supressão ancorada na entrega, não no alerta:** o H5 fala em "alerta HIGH **entregue**". A consulta da supressão (`DeliveryRepository.latestCustomerNotification`) olha as entregas ao cliente em PENDING ou SENT de outros alertas HIGH da mesma conta na janela de ±10 min. Um HIGH anterior cujas entregas falharam (FAILED) ou foram suprimidas não suprime o próximo. Efeito: num ataque contínuo o cliente é avisado uma vez, e de novo só quando passarem 10 min desde o último aviso notificado (e não "uma vez e silêncio até 10 min sem alerta"). O antifraude nunca é suprimido. A reentrega do mesmo alerta não reavalia a supressão.
- **Retry em duas camadas:** os 3 retries imediatos (backoff com jitter) ficam dentro do adaptador de canal (Resilience4j, Fase 4). O caso de uso conta **rodadas**: a 1ª falha agenda 1 min, a 2ª 5 min, a 3ª 30 min, a 4ª esgota (FAILED, `alerts.dlq`, e SMS de reserva se era PUSH num alerta HIGH). A entrega continua PENDING entre as rodadas, com `attempts` e `lastError`.
- **Falhas que não repetem:** contato ausente (`NO_CONTACT`) e recusa definitiva do provedor (4xx) vão direto para FAILED, sem retry e sem DLQ; serviço de contatos fora do ar é falha **com** retry, não `NO_CONTACT`.
- **Idempotência no registro:** `alerts` recusa o mesmo `alertId` ou o mesmo `transactionId`; registrar de novo devolve só as entregas ainda PENDING, então uma reentrega do Kafka depois de uma queda antes do despacho retoma o que faltou sem reenviar o que já saiu.

## 11. Decisões da implementação (Fase 3)
- **Chaves de `alerts` por partição:** no Postgres, `PRIMARY KEY` e `UNIQUE` de tabela particionada precisam incluir a chave de partição, então a spec técnica (§4.4: `alert_id PK` e `transaction_id UNIQUE` diretos) não é implementável como escrita. `alerts` usa `PRIMARY KEY (alert_id, detected_at)` e `UNIQUE (transaction_id, detected_at)`, o que barra a reentrega do mesmo alerta (mesma mensagem, mesmo `detected_at`). **A idempotência do efeito não depende disso:** vem da PK `(alert_id, channel)` de `notification_delivery`, que não é particionada; um alerta gravado duas vezes com `detected_at` diferentes não geraria uma segunda entrega.
- **Partição do dia sob demanda:** a função `ensure_alert_partition(day)` (com `pg_advisory_xact_lock`, segura contra duas instâncias) cria a partição UTC do dia; o repositório a chama na primeira gravação de cada dia e guarda os dias já garantidos em memória. Não há partição `DEFAULT` (ela impediria criar a partição do dia depois). A retenção é um `DROP` de partição (`[DOC]`, fora desta fase).
- **Sem chave estrangeira** de `notification_delivery` e `alert_feedback` para `alerts` (não existe FK para tabela particionada sem a chave de partição). A consulta da supressão filtra `alerts` por conta, severidade e janela de `detected_at` (poda de partições, usa o índice `(account_token, detected_at desc)`) e confere a entrega com `EXISTS` na PK.
- **Migração na subida:** o Spring Boot 4 não autoconfigura o Flyway só com o `flyway-core`; um bean `Flyway` com `initMethod = "migrate"` (mesmo padrão do `rule-management`) roda a migração, e os repositórios dependem dele. A prontidão do serviço inclui o banco (`readinessState,db`).
- **Teste de subida virou integração:** `NotificationStartupIT` sobe o serviço com Postgres real (Testcontainers), porque o serviço agora exige o banco.
- **Exceção ao "sem Jackson 2":** só para o Flyway, como no `rule-management` (Enforcer com a mesma exceção e um teste de ArchUnit garantindo que o nosso código só usa o Jackson 3).

## 12. Decisões da implementação (Fase 4)
- **`Idempotency-Key` do webhook:** o cenário H1.2 dizia `alertId` e o FR-005 dizia `alertId:canal` para todo provedor. Valeu o FR-005 (uma regra única, e é a chave da entrega no banco); o H1.2 foi corrigido.
- **Canais com o cliente HTTP do JDK:** `AntifraudWebhookChannel`, `PushChannel`, `EmailChannel` e `SmsChannel` usam `java.net.http.HttpClient` (sem biblioteca extra, funciona bem com virtual threads). A assinatura é `X-Signature` = HMAC-SHA256 do corpo, em hexadecimal minúsculo, com o segredo vindo de variável de ambiente.
- **Mapeamento HTTP → resultado:** 2xx é sucesso (o id do provedor vem de `caseId` ou `messageId`, com a chave de idempotência de reserva); 408, 429 e 5xx, timeout e erro de rede são falha **com** nova tentativa; os demais 4xx são recusa definitiva.
- **Mensagem ao cliente genérica:** push, e-mail e SMS não levam token de conta, regras, pontuação nem valores; só o texto fixo e o `alertId`. Sem contato, ou sem o dado do canal, é recusa definitiva e o provedor nem é chamado.
- **Timeout de 2 s por requisição, não `TimeLimiter`:** o `TimeLimiter` do Resilience4j exige envolver a chamada num `Future` e deixa a requisição HTTP rodando depois do estouro; o timeout da própria requisição cancela de fato. O efeito é o mesmo para o plano (2 s) e é configurável.
- **Resilience4j por canal (`ResilientChannel`):** `Retry` (3 tentativas imediatas, backoff exponencial com jitter), `CircuitBreaker` (janela de 20, 50 %, 30 s aberto, 3 chamadas em meia-abertura) e `Bulkhead` (50 simultâneas), na ordem Retry → Bulkhead → CircuitBreaker. Só falhas **transitórias** contam para o circuito: recusas definitivas (4xx) não abrem. Circuito aberto ou bulkhead cheio viram falha com nova tentativa sem chamar o provedor e sem repetir na hora. Cada canal tem a sua instância, então o circuito do push aberto não afeta o antifraude nem o e-mail (testado).
- **Diretório de contatos:** `HttpContactDirectory` consulta `GET {base}/v1/contacts/{accountId}` a cada envio, sem cache (FR-008). 404 é "sem contato"; 5xx, resposta quebrada, timeout e conexão recusada são `UnavailableException`, que o caso de uso trata como falha **com** nova tentativa.
- **Testes de canal usam WireMock próprio** (`wiremock-standalone`, porta dinâmica), não o `provider-mocks` do compose.

## 13. Decisões da implementação (Fase 5)
- **`kafka-clients` simples em vez de Spring Kafka:** `AlertListener` e `RetryListener` viraram `AlertConsumerLoop` e `RetryConsumerLoop`, laços de `poll` com `pause`/`resume` por partição e commit manual. É o que permite "commit só depois de gravar" e "não consumir antes do `dueAt`" sem `nack` com sleep, e os laços testam com `MockConsumer`.
- **Registro transacional:** `RegisterAlertUseCase` grava o alerta e as entregas na mesma transação (`UnitOfWork`). O offset só é confirmado depois do commit, e o despacho roda depois, em virtual threads. Falha de banco: `seek` de volta, pausa e backoff, sem perder a mensagem.
- **Alerta inválido:** validado contra `fraud-alert.v1`; o que não passa vai bruto para `alerts.dlq` e a partição segue.
- **Retry por tópicos com atraso injetável:** `alerts.retry.1m/.5m/.30m` carregam `RetryMessage(alertId, channel, attempt, dueAt)`; o consumidor segura a partição até o vencimento. Os atrasos vêm da configuração (no teste, 300/600/900 ms). Esgotados, a entrega vai a `FAILED`, a mensagem à `alerts.dlq` e, em HIGH, o SMS entra como reserva do push.
- **Reconciliador respeitando o backoff:** uma entrega `PENDING` só é reprocessada quando `updated_at + atraso da tentativa + tolerância` já passou, para não furar o retry que está esperando num tópico.
- **Ajustes de compose (feitos, ainda não exercitados com a pilha de pé):** `KAFKA_BOOTSTRAP_SERVERS`, URLs dos provedores em `provider-mocks:8080`, `CONTACTS_URL` e `depends_on` do `provider-mocks`.
- **Verificação:** `AlertFlowIT` (8 testes, Kafka e Postgres reais e WireMock próprio) cobre H1, H2, H3, H4, H5, cliente sem contato, mensagem inválida e o reconciliador. Ainda não foi rodado no compose completo (SC-002 e SC-003 são a Fase 6).

## 14. Decisões da implementação (Fase 6)
- **`GET /v1/alerts`:** ordena por `detected_at` e `alert_id` decrescentes e pagina por **cursor de keyset** (`(detected_at, alert_id) < cursor`), não por `OFFSET`: o custo não cresce com a página e não há repetição nem salto quando entram alertas novos. O cursor é opaco (base64url de microssegundos + `alertId`; o Postgres guarda microssegundos, então o ponto de corte é exato). O caso de uso pede `limit + 1` para saber se há próxima página; página cheia exata não devolve cursor. `limit` padrão 50, de 1 a 200; cursor adulterado, severidade ou data inválidas dão 400 em Problem Details. A listagem devolve um resumo (sem o `payload`) com os ids das regras.
- **`POST /v1/alerts/{id}/feedback`:** 201; veredito `CONFIRMED_FRAUD` ou `FALSE_POSITIVE`; alerta inexistente é 404; o analista vem do cabeçalho `X-Analyst` (sem autenticação nesta entrega, como o resto da API; em branco vira `desconhecido`). O analista pode mudar de ideia: cada veredito é uma linha nova em `alert_feedback`, sem sobrescrever.
- **Métrica de falso positivo por regra:** `alerts_feedback_total{verdict, rule}`, um incremento por regra que disparou o alerta. A tabela do §5 só previa o rótulo `verdict`; o `rule` foi acrescentado porque a H6.2 pede "por regra" (cardinalidade baixa: poucas regras).
- **SC-002 e SC-003 provadas em Testcontainers, não na pilha do compose:** `ReliabilityIT` com Kafka e Postgres reais e WireMock próprio. SC-002: 1.000 alertas (500 HIGH, 500 MEDIUM, contas distintas para a supressão não interferir) publicados duas vezes; nenhuma `Idempotency-Key` aparece mais de uma vez nos provedores. SC-003: push em 503, 200 HIGH; 100% dos webhooks `SENT`, o e-mail segue, push vai a `FAILED` e o SMS de reserva sai para todos. O "5 minutos" da spec foi reduzido (atrasos de retry de 300 a 900 ms), como o T522 já previa. Não foi rodado contra `provider-mocks` no compose.
- **Isolamento de teste:** o circuit breaker do push guarda as falhas do SC-003; sem `@DirtiesContext` o circuito aberto vazava para o teste seguinte (382 de 500 pushes falharam). O teste do 503 agora recria o contexto.

## 15. Decisões da implementação (Fase 7)
- **Porta `DeliveryMetrics`** (application/gateway) com métodos `default` vazios e a constante `NONE`: os casos de uso não conhecem o Micrometer, e o adaptador `MicrometerDeliveryMetrics` implementa. Resultados: `sent` (com a latência), `retry`, `failed` (recusa definitiva, contato ausente ou tentativas esgotadas, esta também conta na DLQ) e `suppressed` (no registro do alerta).
- **`notification_alert_to_delivery_seconds`** mede de `detectedAt` do alerta até o provedor aceitar, com buckets fixos de 100 ms a 60 s (percentis calculados no Prometheus, que dá para agregar entre instâncias). Inclui fila no Kafka, banco e retries: entrega que precisou de retry aparece como cauda.
- **`notification_dlq_total{channel}`** tem um valor por canal e `INVALID_ALERT` para a mensagem fora do contrato `fraud-alert.v1` (que antes ia à DLQ sem deixar rastro em métrica). Todas as séries de entrega e DLQ nascem em zero, para o painel não ter buraco antes do primeiro alerta.
- **`resilience4j_circuitbreaker_state{name,state}`:** em vez de trazer o módulo `resilience4j-micrometer` (que exige um `CircuitBreakerRegistry` que não usamos), um gauge por canal e por estado na `NotificationConfig`, com o mesmo nome e o mesmo formato (1 no estado atual, 0 nos outros; estados em minúsculas, como `half_open`), então o painel funciona com qualquer um dos dois.
- **Painel:** linha "Notificações (spec 005)" no `motor-fraude.json` (versão 6): entregas por canal e resultado, p99 e p50 da latência por canal, circuit breakers abertos, DLQ e reconciliadas, e feedback por regra. O Prometheus já raspava `notification-service:8085`.
- **Teste:** `MetricsIT` usa contexto próprio (`@DirtiesContext`), porque os contadores são do processo e acumulavam os dos outros testes. Não foi aberto no Grafana de verdade: o JSON foi validado, e as consultas não foram exercitadas contra um Prometheus.
