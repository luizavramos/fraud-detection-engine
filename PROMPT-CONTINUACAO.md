# Prompt de continuação — Motor de Detecção de Transações Suspeitas

> Copie tudo abaixo da linha e cole como primeira mensagem no novo chat, aberto na pasta `C:\Users\Luiza\Documents\Desafio`.

---

Você vai continuar um trabalho já desenhado e validado. **Não redesenhe a arquitetura**: as decisões abaixo foram discutidas e aprovadas. Se achar que algo deveria mudar, pergunte antes de mudar.

## 1. O que é
Case técnico de processo seletivo (Engenharia de Software Backend, 7 dias + apresentação de 1 h). O desafio é um **motor de detecção de transações suspeitas em tempo real**:
- recebe eventos de transações de serviços internos;
- aplica regras de detecção;
- gera alertas para canais internos (antifraude) e externos (cliente: push, e-mail);
- continua funcionando quando serviços auxiliares caem.

Requisitos não funcionais do PDF:
- 8.000 TPS de média, 25.000 de pico;
- alerta em ≤ 500 ms;
- segurança ponta a ponta e LGPD;
- idempotência (sem duplicar alerta);
- integração com sistemas internos;
- regras configuráveis **sem redeploy** (thresholds dinâmicos, regras compostas);
- monitoramento por SRE.

O PDF também exige **deixar explícito como e quando a IA foi usada**. Mantenha `docs/uso-de-ia.md` atualizado a cada etapa.

## 2. Arquivos do projeto (leia nesta ordem)
1. `specs/constitution.md`: princípios obrigatórios e stack fixa.
2. `specs/README.md`: índice e ordem de implementação.
3. `specs/000-plataforma-e-contratos/{spec,plan,tasks}.md`: **próximo passo**.
4. As demais specs (001–006), quando chegar a vez de cada uma.
5. Referência, se precisar do porquê de alguma decisão:
   - `technical-spec-motor-deteccao-fraude.md` (ADRs 1–13);
   - `business-spec-motor-deteccao-fraude.md`.
6. Desenhos:
   - `arquitetura-aws.drawio` / `arquitetura-aws.html`: AWS, com passos numerados 1–8;
   - `fluxo-bpmn-sequencia.drawio`: página 1 BPMN, página 2 diagrama de sequência.

`preview-drawio.html` é só uma ferramenta de visualização e pode ser ignorado.

## 3. Arquitetura final (decidida)
- **Modelo:** detecção **assíncrona, depois da transação**. Gera alerta, não bloqueia o pagamento. O SLO é **p99 ≤ 500 ms** da chegada no receiver até o alerta publicado no tópico interno. A entrega ao cliente tem SLO próprio.
- **Receiver** (Spring Boot, HTTP `POST /v1/transactions:evaluate`):
  - valida o JSON Schema;
  - aplica rate limit por origem;
  - **Redis como escudo de carga** (`SET NX EX 600`, **fail-open** se cair), não como garantia;
  - publica no Kafka com `acks=all` e responde `202`;
  - outras respostas: `400` / `429` / `503` (no `503`, apaga a chave do Redis).
- **Um tópico de comando** na entrada: `transaction-commands.v1` (`EvaluateTransaction`), chave `accountId`, 6 partições local / 48 em produção. **Não** há tópico de evento separado nem event store.
- **Customer service** (Kafka Streams): lê o CDC (Debezium) da base de clientes, pré-calcula o perfil **sem dados pessoais** e publica em `customer-profile.v1` (compactado). O motor mantém uma cópia local.
- **Transaction service** (Kafka Streams): só faz a **carga inicial de 90 dias** de histórico no go-live (`transaction-history-backfill.v1`). Fica fora do caminho quente.
- **Motor de detecção** (Kafka Streams puro, sem Spring, Processor API, `exactly_once_v2`, RocksDB): é o **orquestrador e único dono da decisão**. Não usa SAGA nem scatter-gather, e não faz nenhuma chamada síncrona por transação. Por transação, executa:
  1. dedup de 24 h;
  2. perfil da cópia local + blocklist (GlobalKTable);
  3. histórico calculado **no próprio fluxo**: buckets de 60×1 min, 24×1 h e 90×1 dia (`count/sum/max`); últimos 50 beneficiários e 10 dispositivos;
  4. regras CEL;
  5. score: MÉDIA ≥ 40, ALTA ≥ 70; `alertId = UUIDv5(transactionId)`;
  6. publica `fraud-decisions.v1` (todas as decisões) e `fraud-alerts.v1` (alertas MÉDIA e ALTA).
- **Gestão de regras** (Spring Boot + Postgres `rules_db`):
  - regras são **dados**, com versões imutáveis;
  - ações: ativar, desligar (kill switch), rollback, modo sombra, scoring;
  - **o motor busca o conjunto ativo por polling a cada 30 s** em `GET /v1/rule-sets/active` (ETag/304), compila em memória, troca de forma atômica e grava snapshot em disco. Sem snapshot nem gestão disponível, a readiness falha;
  - **sem tópico de regras e sem outbox**;
  - **nenhuma transação chama a gestão de regras**;
  - mudar regras **nunca** exige deploy. Só um **dado novo** no catálogo de features exige deploy do motor.
  - o módulo compartilhado **`rules-kernel`** (catálogo de features + compilador e avaliador CEL) é usado pela gestão e pelo motor.
- **Notificações** (Spring Boot + Postgres `alerts_db`):
  - grava alerta e entregas com `ON CONFLICT DO NOTHING` (PK `alert_id, channel`) e envia `Idempotency-Key` aos provedores;
  - resiliência: Resilience4j por canal, retry topics `alerts.retry.1m/5m/30m`, `alerts.dlq`, SMS como reserva do push;
  - supressão de avisos ao cliente por conta (janela de 10 min);
  - destinos: webhook HMAC para o antifraude legado; SES e AWS End User Messaging para o cliente (**não usar Pinpoint**);
  - API do analista com consulta e feedback.
- **Auditoria:** todas as decisões vão para o **S3 (Parquet) via MSK Connect S3 sink**, consultadas pelo **Athena**. **MongoDB foi considerado e rejeitado**, tanto como event store quanto como camada quente de 7 dias, por volume e custo. Pode ser citado na apresentação como alternativa e evolução.
- **AWS (região sa-east-1):**
  - rede: VPC com 2 AZs no desenho, entrada por PrivateLink + NLB interno, ALB interno com OIDC para o analista, NAT com saída filtrada;
  - computação e dados: EKS, MSK, ElastiCache, RDS Multi-AZ, EBS gp3 para o RocksDB;
  - segurança e operação: KMS, ACM Private CA (mTLS), Secrets Manager, CloudTrail, GuardDuty, CloudWatch, Prometheus e Grafana gerenciados, ECR.

## 4. Stack e regras de implementação
- **Java 21**, **Maven multi-módulo com Maven Wrapper** (`pom.xml` pai, um módulo por serviço). Surefire roda os `*Test`, Failsafe roda os `*IT`.
- **Spring Boot 4.0.x** (MVC + virtual threads) no receiver, na gestão de regras e nas notificações. **Kafka Streams puro** no motor, no customer service e no transaction service.
- **Clean Architecture** (`domain` → `application` [casos de uso + portas] → `adapters` → `infrastructure`) com **teste ArchUnit obrigatório**.
- **Testes antes da implementação** em toda tarefa. Ferramentas: JUnit 5, AssertJ, jqwik, `TopologyTestDriver`, Testcontainers, WireMock.
- **Contratos:** JSON + JSON Schema no módulo `contracts`.
- **Dados:** dinheiro em centavos (`long`); tempo em `Instant` UTC, sempre via porta `Clock`.
- **Privacidade:** **somente tokens** em tópicos, bancos e logs; o mascaramento de dados pessoais é testado.
- **Pacote base** `br.com.motorfraude.<servico>`. **Portas:** 8081 receiver, 8082 rule-management, 8083 customer-service, 8084 detection-engine, 8085 notification-service, 8086 transaction-service.
- **Ambiente local:** `docker compose up` com Kafka (KRaft), Kafka Connect + Debezium, Postgres do projeto, Postgres "banco legado" (fonte do CDC, com colunas de dados pessoais sintéticos para provar a minimização), Redis, Prometheus e Grafana. Provedores externos são simulados com WireMock.

## 5. Ordem de implementação
`000 plataforma e contratos` → (`001 receiver`, `002 gestão de regras + rules-kernel`, `003 customer service` em paralelo) → `004 motor` → `005 notificações` → `006 transaction service`.

A **Fase 2 da 002 (`rules-kernel`)** desbloqueia o motor. Cada `tasks.md` lista as tarefas na ordem, com teste antes da implementação. Marque `[x]` ao concluir cada uma.

## 6. Decisões com valor padrão adotado (marcadas `[NEEDS CLARIFICATION]` nas specs)
- JSON + JSON Schema (e não Avro + Schema Registry).
- Gestão de regras: versões, validação, ativar/desligar/rollback, scoring e modo sombra; aprovação por outra pessoa (maker-checker) só se sobrar tempo.
- 8 regras iniciais (R01, R03, R04, R05, R07, R09, R10, R12) com faixas 40/70 (tabela em `specs/002-rule-management/plan.md` §5).
- Modelo de transação genérico (`type`), com regras pensadas para Pix.
- Rate limit de 20.000 req/s por instância.

## 7. Estado atual e próximo passo
- **Pronto:** specs, desenhos e documentação. **Nenhum código foi escrito.** A pasta ainda **não é um repositório git**.
- **Próximo passo:**
  1. inicializar o git e criar uma branch;
  2. implementar a **spec 000** seguindo `specs/000-plataforma-e-contratos/tasks.md` (T001 em diante);
  3. criar `docs/uso-de-ia.md` registrando que as specs e os desenhos foram produzidos com apoio de IA, e o que foi decidido pela candidata ou pelo candidato.
- **Ao final de cada fase:** rode `./mvnw verify`, confirme o ArchUnit verde e resuma o que foi feito.
