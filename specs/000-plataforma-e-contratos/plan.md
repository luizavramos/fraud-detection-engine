# Plano 000 — Plataforma e contratos

## 1. Resumo
Esta unidade cria:
- o esqueleto Maven multi-módulo;
- o ambiente local via Docker Compose;
- os contratos compartilhados (schemas + records);
- o módulo `platform-commons` (observabilidade e privacidade, sem Spring e sem Kafka Streams);
- o gerador de carga (implementação adiada para depois do checkpoint da Fase 3 da 001; ver `tasks.md`).

## 2. Contexto técnico
| Item | Valor |
|---|---|
| Linguagem | Java 21 |
| Build | Maven 3.9+ com Maven Wrapper. `pom.xml` pai (`packaging=pom`) centraliza versões em `<properties>` e `<dependencyManagement>` (importa o BOM do Spring Boot **4.0.x**, que também fixa as versões de Jackson 3, Kafka, Micrometer e Testcontainers 2.x para os módulos sem Spring) e plugins em `<pluginManagement>`. Testcontainers 2.x renomeou os artefatos de módulo (ex.: `testcontainers-postgresql`, `testcontainers-kafka`) |
| Plugins | `maven-compiler-plugin` (release 21), **Surefire** (testes unitários `*Test`), **Failsafe** (integração `*IT`, rodam em `verify`), `jacoco-maven-plugin`, `spring-boot-maven-plugin` (só nos serviços Spring), `maven-shade-plugin` ou `maven-jar-plugin` com classpath (apps Kafka Streams e gerador de carga), `exec-maven-plugin` (rodar o gerador) |
| Validação de schema | `com.networknt:json-schema-validator` (Draft 2020-12) |
| JSON | **Jackson 3** (pacote `tools.jackson`, `JsonMapper` imutável montado por builder; suporte a `java.time` já embutido, sem `JavaTimeModule`). `FAIL_ON_UNKNOWN_PROPERTIES=false` configurado explicitamente nos consumidores (tolerância a campos novos) e datas escritas como ISO-8601. Jackson 2 não entra no projeto: dependência que o puxe é exceção registrada aqui |
| Logs | Logback + `logstash-logback-encoder`. Mascaramento pelo `MaskingJsonGeneratorDecorator` do próprio encoder, com um `ValueMasker` nosso (`PiiValueMasker`): mascara **todo** valor de texto do JSON (mensagem, MDC, argumentos, stack trace), não só a `%msg` |
| Métricas / trace | Serviços Spring: Actuator + Micrometer Tracing com ponte OpenTelemetry (lê o `traceparent` HTTP e põe `traceId` no MDC sozinho). Apps Kafka Streams: Micrometer (Prometheus registry) via `platform-streams`. Em Kafka, o `traceparent` vai e volta pelo `KafkaTracePropagation` do commons |
| Carga | Java 21 `HttpClient` com virtual threads e controle de taxa por token bucket; histograma com HdrHistogram. **Modelo aberto:** cada requisição tem um horário planejado (`t0 + i/tps`) e a latência é medida a partir dele, não do envio real. Isso evita a *coordinated omission* (quando o sistema engasga, o atraso na fila entra no p99 em vez de sumir) |

## 3. Contratos

### 3.1 Tópicos (criados pelo container `kafka-init`)
| Tópico | Chave | Partições (local / prod) | Política | Retenção |
|---|---|---|---|---|
| `transaction-commands.v1` | `accountId` | 6 / 48 | delete | 7 d |
| `transaction-commands.dlq` | `accountId` | 1 / 6 | delete | 14 d |
| `customer-profile.v1` | `accountId` | 6 / 48 | compact | — |
| `transaction-history-backfill.v1` | `accountId` | 6 / 48 | delete | 7 d |
| `blocklist.v1` | `entityToken` | 1 / 6 | compact | — |
| `fraud-decisions.v1` | `accountId` | 6 / 48 | delete | 3 d |
| `fraud-alerts.v1` | `accountId` | 3 / 12 | delete | 7 d |
| `alerts.retry.1m` / `.5m` / `.30m` | `alertId` | 1 / 6 | delete | 14 d |
| `alerts.dlq` | `alertId` | 1 / 6 | delete | 14 d |
| `cdc.bank.customers` / `cdc.bank.customer_devices` / `cdc.bank.transactions` | PK da tabela | 3 / 12 | delete | 7 d |

`customer-profile.v1`, `transaction-history-backfill.v1` e `transaction-commands.v1` **precisam ter o mesmo número de partições** (co-particionamento exigido pelo motor).

**Nome dos tópicos de CDC:** o Debezium nomeia os tópicos como `<topic.prefix>.<schema>.<tabela>`. As tabelas do banco legado ficam no schema Postgres `bank` (não em `public`) e os conectores usam `topic.prefix=cdc`, então os nomes saem `cdc.bank.<tabela>` sem transformação extra. Os tópicos são criados antes pelo `kafka-init`, com as partições acima; o Connect não depende de criação automática.

### 3.2 Schemas (`contracts/src/main/resources/schemas/`)
| Schema | Produtor → consumidor | Campos-chave |
|---|---|---|
| `evaluate-transaction.v1.json` | receiver → motor | `schemaVersion`, `transactionId`, `accountId`, `customerId`, `type`, `amount{value:int64,currency}`, `counterparty{accountToken,bankIspb}`, `channel`, `deviceIdHash`, `occurredAt` |
| `customer-profile.v1.json` | customer-service → motor | `accountId`, `segment` (PF/PJ), `accountOpenedAt`, `lastCredentialChangeAt`, `knownDeviceHashes[]` (≤10), `updatedAt` |
| `transaction-history-record.v1.json` | transaction-service → motor | `transactionId`, `accountId`, `amount`, `counterpartyToken`, `deviceIdHash`, `occurredAt` |
| `blocklist-entry.v1.json` | antifraude legado → motor | `entityToken`, `entityType` (ACCOUNT/DEVICE), `reason`, `listedAt`; valor nulo = remoção |
| `fraud-decision.v1.json` | motor → S3 | `decisionId`, `transactionId`, `accountId`, `decision` (ALERT/NO_ALERT), `score`, `severity`, `triggeredRules[]`, `shadowHits[]`, `features{}`, `ruleSetVersion`, `contextCompleteness`, `eventReceivedAt`, `decidedAt` |
| `fraud-alert.v1.json` | motor → notificações | `alertId` (UUIDv5 de `transactionId`), `transactionId`, `accountId`, `severity` (MEDIUM/HIGH), `score`, `triggeredRules[{ruleId,version,weight,reason}]`, `ruleSetVersion`, `contextCompleteness`, `eventReceivedAt`, `detectedAt`, `traceId` |
| `rule-set.v1.json` | gestão de regras → motor (HTTP) | `version`, `publishedAt`, `scoring{mediumThreshold,highThreshold}`, `rules[{ruleId,version,status,expression,params,paramOverrides,weight,reasonTemplate}]` |

**Valores adotados para os enums de transação** (as specs não os definiam; `[NEEDS CLARIFICATION]` resolvido com este padrão, ver spec 000 "Premissas"): `type` ∈ {`PIX`, `TED`, `BOLETO`, `CARD`}; `channel` ∈ {`MOBILE`, `WEB`, `API`, `BRANCH`}. Acrescentar valor é mudança de contrato: o schema e os consumidores vão primeiro (regra de evolução abaixo). Os identificadores são tokens: `acc_tok_…`, `cus_tok_…`, `dev_…`; `alertId` é UUID versão 5.

**Schemas estritos em todos os níveis:** todo objeto de todo schema (raiz e aninhados) declara `additionalProperties: false`. Assim o schema barra dado pessoal tanto na entrada (um `cpf` enviado ao receiver) quanto na saída (uma coluna `email` do CDC vazando para `customer-profile.v1`).

**Mapas abertos com limites:** `features{}` (decisão), `params` e `paramOverrides` (regras) são mapas por natureza. Neles:
- `propertyNames` com padrão `^[a-z][a-zA-Z0-9_.]{0,63}$`;
- valores só `number`, `boolean` ou `string` com `maxLength: 64` (em `params`/`paramOverrides`, também arrays desses tipos, até 50 itens).

**Quem valida o quê:**

| Onde | Valida? | Por quê |
|---|---|---|
| Receiver, entrada HTTP | Sim, em produção | Fronteira com quem chama: fora do contrato → `400` |
| Produtores internos (receiver, customer-service, transaction-service, motor, gestão de regras) | Sim, nos **testes de contrato** | Toda mensagem produzida nos testes passa por `Schemas.validate`; campo a mais quebra o build |
| Consumidores (motor, notificações) | Não; leitura tolerante (`FAIL_ON_UNKNOWN_PROPERTIES=false`) | Validar por mensagem custa caro a 25k TPS, e a tolerância permite evoluir sem quebrar |
| `blocklist.v1` (produzido pelo antifraude legado) | Não | Produtor fora do nosso controle; o schema documenta, o motor lê de forma tolerante |

**Regra de evolução:** um campo opcional novo entra **primeiro** no schema do `contracts` (e no deploy do receiver/consumidores) e só depois nos produtores. O schema estrito força essa ordem; a leitura tolerante dos consumidores a torna compatível com versões anteriores (princípio V). Remover campo, mudar tipo ou tornar obrigatório continua exigindo `.v2`.

**Inteiros e dinheiro no JSON Schema:** JSON Schema não tem `int64`. Todo campo inteiro de 64 bits (ex.: `amount.value`) é `"type": "integer"` com `minimum`/`maximum` explícitos (`amount.value`: `1` a `9007199254740991`, maior inteiro exato em JSON). Atenção: no Draft 2020-12, `10.0` **é** inteiro válido (número com parte fracionária zero), enquanto `10.5` não é. Os testes cobrem os dois casos; o record Java recebe `long` em ambos.

**Headers Kafka padrão:** `traceparent`, `receivedAt` (ISO-8601), `schemaVersion`, `commandType` (só em comandos).

### 3.3 Records Java (`contracts/src/main/java/br/com/motorfraude/contracts/`)
Um record por schema, com tipos fortes:
- `Money(long value, String currency)`;
- enums para `type`, `channel` e `severity`;
- `Instant` para todo instante.

A classe `Schemas` expõe `validate(String schemaName, JsonNode)` para testes e para os validadores de entrada.

**Teste de contrato do produtor:** `contracts` publica um test-jar com `ContractAssertions.assertProduces(schemaName, Object mensagem)` (serializa com o `JsonCodec` e valida). Os módulos produtores (001, 002, 003, 004, 006) usam esse utilitário nos seus testes de saída.

## 4. Estrutura do repositório
```
motor-fraude/
├── pom.xml                        # pai: <modules>, versões, dependencyManagement, pluginManagement (Java 21, Surefire/Failsafe, Jacoco)
├── mvnw, mvnw.cmd, .mvn/wrapper/  # Maven Wrapper (o avaliador não precisa instalar Maven)
├── docker-compose.yml
├── .env.example
├── infra/
│   ├── kafka-init/create-topics.sh
│   ├── connect/register-connectors.sh     # Debezium (usado por 003 e 006)
│   ├── bank-db/init.sql                    # "banco legado", schema `bank`: customers, customer_devices, transactions (dados sintéticos) + publicações
│   ├── prometheus/prometheus.yml
│   └── grafana/{provisioning/,dashboards/motor-fraude.json}
├── contracts/
├── platform-commons/                 # sem Spring e sem Kafka Streams; deps: logback, logstash-logback-encoder, kafka-clients
│   ├── src/main/java/br/com/motorfraude/commons/
│       ├── logging/PiiPatterns.java          # regex de CPF, e-mail, telefone, cartão
│       ├── logging/PiiValueMasker.java       # ValueMasker do logstash-logback-encoder
│       ├── tracing/TraceParent.java          # lê/gera o header W3C (sem SDK OpenTelemetry)
│       ├── tracing/KafkaTracePropagation.java
│       ├── time/ClockProvider.java
│       └── (src/main/resources) logback-base.xml
│   └── src/test/
│       ├── CleanArchitectureRules.java   # regras ArchUnit reutilizáveis, publicadas como test-jar (maven-jar-plugin goal test-jar)
│                                                    # test-jar não traz dependências transitivas: cada serviço declara archunit-junit5 (scope test) além do test-jar;
│                                                    # a versão do ArchUnit fica no dependencyManagement do pom pai
│       ├── CommonsBoundaryTest.java      # ArchUnit: o código principal do commons não importa org.springframework nem org.apache.kafka.streams
│       └── sampleapp/                    # app Spring Boot mínimo (Spring só em escopo de teste) para o aceite da H3
├── platform-streams/                 # criado na 003 (T301): StreamsAppBootstrap, MiniHttpServer; usado por 003, 004 e 006
├── load-generator/
├── receiver/ … transaction-service/   # um módulo por serviço (specs 001–006)
└── README.md
```

## 5. Docker Compose (serviços)
| Serviço | Imagem | Porta | Healthcheck |
|---|---|---|---|
| kafka | `apache/kafka` (KRaft) | 9092 | `/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` (nessa imagem os scripts ficam em `/opt/kafka/bin` com sufixo `.sh`) |
| kafka-init | `apache/kafka` | — | roda `create-topics.sh` e termina |
| connect | `quay.io/debezium/connect` (a imagem `debezium/connect` do Docker Hub foi descontinuada) | 8083→18083 | `GET /connectors` |
| postgres | `postgres:16` (bancos `rules_db`, `alerts_db`) | 5432 | `pg_isready` |
| bank-db | `postgres:16` com `wal_level=logical` | 5433 | `pg_isready` |
| redis | `redis:7` | 6379 | `redis-cli ping` |
| prometheus | `prom/prometheus` | 9090 | `/-/ready` |
| grafana | `grafana/grafana` | 3000 | `/api/health` |

Os serviços da aplicação entram no compose pelas specs 001–006, cada um com `depends_on: condition: service_healthy`.

**Versões das imagens:** toda imagem usa tag exata (nunca `latest`), fixada na T004, para o ambiente do avaliador ser igual ao testado.

### 5.1 Kafka Connect + Debezium (configuração comum)
Vale para os conectores registrados pela 003 (`customers`, `customer_devices`) e pela 006 (`transactions`):
- **Tópicos internos do Connect:** com um broker só, `CONFIG_STORAGE_REPLICATION_FACTOR`, `OFFSET_STORAGE_REPLICATION_FACTOR` e `STATUS_STORAGE_REPLICATION_FACTOR` = 1 (padrão é 3 e o Connect não sobe).
- **Formato:** `JsonConverter` com `schemas.enable=false` em chave e valor. O tópico recebe o envelope Debezium puro (`before`/`after`/`op`/`source`), sem o bloco `schema`, como as specs 003 e 006 esperam.
- **Nomes:** `topic.prefix=cdc`, tabelas no schema `bank` (ver §3.1).
- **Publicação e slot por conector:** o `init.sql` do banco legado cria duas publicações (`dbz_customers` com `customers` e `customer_devices`; `dbz_transactions` com `transactions`) e cada conector usa `publication.autocreate.mode=disabled`, sua própria `publication.name` e seu próprio `slot.name`. Dois conectores no mesmo slot brigam pelo WAL.
- **Plugin:** `plugin.name=pgoutput` (nativo do Postgres, sem extensão).

## 6. Decisões
- **Um broker local, 6 partições:** reproduz o particionamento sem pesar na máquina do avaliador. Fator de replicação 1 local, 3 em produção (documentado).
- **JSON + JSON Schema em vez de Avro:** menos peças (sem Schema Registry). Avro é evolução `[DOC]`.
- **Gerador de carga em Java, não k6:** reusa os records de `contracts` e o mesmo histograma (HdrHistogram) usado pelos serviços. Com virtual threads, sustenta 25k TPS de uma máquina. Mede em modelo aberto (latência a partir do horário planejado), para o p99 reportado não esconder fila.

- **Recorte da plataforma em dois módulos, nenhum com Spring:** `platform-commons` serve a todos; `platform-streams` só aos apps Kafka Streams. Assim o receiver não herda Kafka Streams e o motor não herda Spring. O que o Spring Boot já faz (tracing HTTP, Actuator) não é reimplementado.

## 7. Constitution Check
| Princípio | Atende? | Como |
|---|---|---|
| I Clean Architecture | ✅ | `CleanArchitectureRules` é a base dos testes ArchUnit de cada serviço |
| V Contratos versionados | ✅ | Schemas `*.v1.json`, tópicos `.v1` |
| VI LGPD | ✅ | `PiiValueMasker` (todo o JSON do log) + schemas com `additionalProperties:false` em todos os níveis, mapas abertos com limites e testes de contrato nos produtores |
| VII Observabilidade | ✅ | `platform-commons` + Prometheus/Grafana no compose |
| VIII Dinheiro/tempo | ✅ | `Money(long)`, `Instant`, `ClockProvider` |

## 8. Riscos
- **Compatibilidade com Jackson 3:** nem toda biblioteca já migrou. As críticas aqui são `com.networknt:json-schema-validator` e `logstash-logback-encoder`. Mitigação: a T002 compila um esqueleto com todas as dependências da 000 e roda `mvn dependency:tree` procurando `com.fasterxml.jackson.core:jackson-databind` (Jackson 2). Se uma biblioteca crítica não tiver versão para Jackson 3, a decisão volta para discussão antes de seguir.
- **Fim do suporte da 4.0 (31/12/2026):** a atualização para 4.1 é de versão menor e fica `[DOC]` como próximo passo.
- **Debezium em ambiente local:** é a parte mais frágil do compose. Mitigação: `register-connectors.sh` com retry e um caminho alternativo, um "CDC simulado" (`infra/bank-db/simulate-cdc.sh`) que publica os mesmos eventos direto no tópico.
- **25k TPS numa máquina só:** pode não ser alcançável no notebook. O teste de 25k fica documentado como execução em máquina maior; 8k é a meta local.
