# Spec 005 — Notificações (notification service)

**Status:** implementada e verificada com Kafka, Postgres e WireMock em Testcontainers (Fases 1 a 7); ainda não exercitada na pilha do compose (`--profile app` com os `provider-mocks`), e sem medição de carga (etapa futura) · **Prioridade:** P1 · **Depende de:** 000; consome `fraud-alerts.v1` (004)
**Passos no desenho:** 5, 5.1, 5.2, 5.3

## Contexto
O motor publica alertas num tópico. Este serviço os entrega:
- ao **time antifraude**, por webhook assinado ao sistema legado, sempre que houver alerta;
- ao **cliente** (push, e-mail, SMS como reserva), só nos alertas **HIGH**.

Os provedores externos são lentos e instáveis. Por isso este serviço é separado do motor (ADR-4), e a regra principal é: **entregar sem duplicar e sem travar.**

## Histórias de usuário

### H1 — Registrar o alerta e avisar o antifraude (P1)
Como analista antifraude, quero receber todo alerta (MEDIUM e HIGH) no sistema legado, com o motivo, para abrir o caso.

**Teste independente:** um `FraudAlert` MEDIUM publicado no tópico gera uma linha em `alerts` e uma chamada `POST` ao webhook do legado (simulado), com assinatura HMAC válida.

**Cenários de aceite**
1. **Given** um alerta MEDIUM, **When** é consumido, **Then** é gravado em `alerts`, e uma entrega `ANTIFRAUD_WEBHOOK` fica `SENT`.
2. **Given** a chamada ao webhook, **When** o legado recebe, **Then** os headers `X-Signature` (HMAC-SHA256 do corpo, em hexadecimal) e `Idempotency-Key` (= `alertId:ANTIFRAUD_WEBHOOK`, como o FR-005 pede a todo provedor) estão presentes.

### H2 — Avisar o cliente em alerta grave (P1)
Como cliente, quero ser avisado no celular quando houver suspeita grave na minha conta, para agir na hora.

**Cenários de aceite**
1. **Given** um alerta HIGH, **When** é consumido, **Then** são criadas as entregas `ANTIFRAUD_WEBHOOK`, `PUSH` e `EMAIL`, e cada uma é enviada.
2. **Given** um alerta MEDIUM, **When** é consumido, **Then** **não** há entrega ao cliente.
3. **Given** a busca do contato do cliente (token do dispositivo, e-mail), **When** acontece, **Then** é feita no serviço de contatos no momento do envio. Nenhum dado de contato é gravado no banco deste serviço.

### H3 — Nunca avisar duas vezes (P1)
Como cliente, quero receber um único aviso por suspeita, mesmo se a mensagem do alerta chegar duas vezes.

**Cenários de aceite**
1. **Given** o alerta A já entregue por PUSH, **When** A é consumido de novo (reentrega do Kafka), **Then** nenhuma nova chamada ao provedor de push acontece.
2. **Given** o serviço cai depois de chamar o provedor e antes de marcar `SENT`, **When** reprocessa, **Then** chama de novo com o **mesmo** `Idempotency-Key`, e o provedor descarta a repetição. Esse é o risco residual documentado, para provedores sem idempotência.

### H4 — Seguir funcionando com provedor fora do ar (P1)
Como SRE, quero que a queda do provedor de push não trave os alertas do antifraude nem perca avisos.

**Cenários de aceite**
1. **Given** o provedor de push respondendo `503`, **When** chegam alertas HIGH, **Then** o webhook do antifraude e o e-mail continuam sendo entregues normalmente (isolamento por canal).
2. **Given** a falha persiste, **When** o circuit breaker do PUSH abre, **Then** as entregas de PUSH vão para `alerts.retry.1m` → `.5m` → `.30m` → `alerts.dlq`.
3. **Given** um PUSH que esgotou as tentativas num alerta HIGH, **When** vai para a DLQ, **Then** uma entrega `SMS` é criada como reserva.

### H5 — Não encher o cliente de avisos (P2)
Como cliente, não quero 10 notificações por 10 transações suspeitas em 1 minuto.

**Cenários de aceite**
1. **Given** um alerta HIGH entregue para `acc_tok_1`, **When** chega outro HIGH para a mesma conta em até 10 min, **Then** a entrega ao cliente fica `SUPPRESSED`, e o antifraude recebe os dois normalmente.

### H6 — Consulta e feedback do analista (P2)
Como analista, quero listar os alertas e marcar se eram fraude, para calibrar as regras.

**Cenários de aceite**
1. **Given** alertas gravados, **When** faço `GET /v1/alerts?accountId=&severity=&from=`, **Then** recebo uma lista paginada por cursor.
2. **Given** um alerta, **When** faço `POST /v1/alerts/{id}/feedback {"verdict":"FALSE_POSITIVE"}`, **Then** o feedback é gravado e a métrica de falso positivo por regra é atualizada.

## Casos de borda
- **Alerta com versão de schema desconhecida:** vai para a DLQ com motivo, sem travar a partição.
- **Banco de alertas fora do ar:** o consumidor pausa (não confirma o offset). Nada se perde, o lag cresce e o alarme dispara.
- **Contato do cliente não encontrado:** a entrega fica `FAILED` com o motivo `NO_CONTACT`, sem retry.

## Requisitos funcionais
- **FR-001:** DEVE consumir `fraud-alerts.v1` com `isolation.level=read_committed` e confirmar o offset só depois de gravar o alerta e as entregas.
- **FR-002:** DEVE gravar `alerts` (`transaction_id` único) e `notification_delivery` (PK `alert_id, channel`) com `ON CONFLICT DO NOTHING`.
- **FR-003:** DEVE enviar cada canal isoladamente, com timeout de 2 s, retry com backoff exponencial e jitter (3 tentativas imediatas) e circuit breaker por canal (Resilience4j).
- **FR-004:** depois dos retries imediatos, DEVE agendar novas tentativas nos tópicos `alerts.retry.1m` / `.5m` / `.30m` e, por fim, `alerts.dlq`.
- **FR-005:** DEVE enviar `Idempotency-Key = alertId:channel` a todo provedor.
- **FR-006:** DEVE assinar o webhook do antifraude com HMAC-SHA256 (segredo por variável de ambiente; em produção, Secrets Manager `[DOC]`).
- **FR-007:** DEVE aplicar a supressão por conta (H5), com janela configurável (padrão 10 min).
- **FR-008:** NÃO DEVE gravar dado de contato. A busca é feita pela porta `ContactDirectory` a cada envio.
- **FR-009:** DEVE expor `GET /v1/alerts` e `POST /v1/alerts/{id}/feedback`.

## Critérios de sucesso
- **SC-001:** alerta publicado → webhook do antifraude aceito com p95 ≤ 1 s (provedores simulados saudáveis).
- **SC-002:** 0 entregas duplicadas ao provedor num teste de reentrega forçada de 1.000 alertas.
- **SC-003:** com o PUSH fora do ar por 5 min, 100% dos webhooks do antifraude continuam sendo entregues no prazo.

## Premissas
- Provedores reais (SES, End User Messaging) ficam atrás de portas. Localmente, usam **simuladores WireMock** com falha configurável.
- Serviço de contatos simulado (WireMock) que devolve contatos falsos para os tokens.
