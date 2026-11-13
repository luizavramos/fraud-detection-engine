# Tarefas 000 — Plataforma e contratos

Formato: `[ID] [P?] [História] Descrição — arquivo(s)`. [P] = pode rodar em paralelo com as outras [P] da mesma fase.

## Fase 1 — Esqueleto
- [x] T001 [H1] Criar o `pom.xml` pai (`packaging=pom`, Java 21, `<dependencyManagement>` importando o BOM do Spring Boot 4.0.x (último patch; já traz Jackson 3 e Testcontainers 2.x), `<pluginManagement>` com Surefire, Failsafe e Jacoco) e o Maven Wrapper (`mvnw`, `mvnw.cmd`, `.mvn/wrapper/`) — raiz
- [x] T002 [P] [H1] Criar módulos vazios `contracts` e `platform-commons` (o `load-generator` nasce na Fase 5, que foi adiada), cada um com um teste "smoke" que passa e declarado em `<modules>`, já com as dependências que a 000 vai usar. Conferir com `dependency:tree` que nenhuma traz Jackson 2 (`com.fasterxml.jackson.core:jackson-databind`) — plan §8 — `*/pom.xml`
- [x] T003 [P] [H1] Criar `.gitignore`, `.env.example` e `README.md` com a seção "Como rodar" (placeholder) — raiz

## Fase 2 — Ambiente local (H1)
- [x] T004 [H1] Escrever `docker-compose.yml` com kafka, postgres, bank-db, redis, connect (`quay.io/debezium/connect`, fator de replicação 1 nos tópicos internos, `JsonConverter` sem schema — plan §5.1), prometheus e grafana, com healthchecks e tags de imagem exatas — `docker-compose.yml`
- [x] T005 [P] [H1] Script de criação de tópicos conforme plan §3.1 — `infra/kafka-init/create-topics.sh`
- [x] T006 [P] [H1] `init.sql` do Postgres do projeto (cria `rules_db` e `alerts_db`) — `infra/postgres/init.sql`
- [x] T007 [P] [H1] `init.sql` do banco legado, com tabelas `customers`, `customer_devices` e `transactions` (colunas em 003 plan §3 e 006 plan §3, **incluindo as colunas de dado pessoal sintético** que provam a minimização), 10 mil clientes sintéticos tokenizados. Tabelas no schema `bank` e duas publicações lógicas, `dbz_customers` (`customers`, `customer_devices`) e `dbz_transactions` (`transactions`) — plan §5.1 — `infra/bank-db/init.sql`
- [x] T008 [P] [H1] Provisionamento do Grafana (datasource + dashboard vazio "Motor de fraude") e `prometheus.yml` com os alvos 8081–8086 — `infra/grafana/`, `infra/prometheus/`
- [x] T009 [H1] Teste de aceite H1: script `infra/smoke.sh` que sobe o compose, espera os healthchecks e confere os tópicos com `kafka-topics --describe`

## Fase 3 — Contratos (H2)
- [x] T010 [H2] Escrever **testes** de validação com exemplos válidos/inválidos para cada schema (falham: os schemas ainda não existem). Incluir os casos de inteiro: `amount.value` = `10.5` falha, `10.0` passa (Draft 2020-12), `0` e negativos falham, acima de `2^53-1` falha. Casos de schema estrito: `cpf` na raiz e dentro de `counterparty` falham; campo a mais em `customer-profile` (ex.: `email`) falha; `features{}` com chave fora do padrão ou texto acima de 64 caracteres falha — plan §3.2 — `contracts/src/test/java/.../SchemasTest.java`, `contracts/src/test/resources/examples/`
- [x] T011 [P] [H2] Schemas `evaluate-transaction.v1.json` e `customer-profile.v1.json` — `contracts/src/main/resources/schemas/`
- [x] T012 [P] [H2] Schemas `transaction-history-record.v1.json` e `blocklist-entry.v1.json`
- [x] T013 [P] [H2] Schemas `fraud-decision.v1.json`, `fraud-alert.v1.json` e `rule-set.v1.json`
- [x] T014 [H2] Records Java + `Schemas.validate(...)` + `JsonCodec` (`JsonMapper` do Jackson 3 configurado) — `contracts/src/main/java/br/com/motorfraude/contracts/`
- [x] T015 [H2] Teste de ida e volta (record → JSON → validação → record) para cada contrato, e `ContractAssertions.assertProduces(...)` publicado no test-jar de `contracts` para os testes de contrato dos produtores (plan §3.3)

## Fase 4 — platform-commons (H3)
- [ ] T016 [H3] **Teste** do mascaramento (CPF, e-mail, telefone, cartão) na mensagem, no MDC, em argumentos estruturados e em mensagem de exceção — `platform-commons/src/test/.../PiiValueMaskerTest.java`
- [ ] T017 [H3] `PiiPatterns` + `PiiValueMasker` (ligado pelo `MaskingJsonGeneratorDecorator`) + `logback-base.xml` JSON reutilizável — `platform-commons/src/main/...`
- [ ] T018 [P] [H3] `TraceParent` (lê/gera o header W3C) + `KafkaTracePropagation` (injeta e extrai `traceparent` em headers Kafka), com teste
- [ ] T019 [P] [H3] `ClockProvider` (porta `Clock` injetável) com teste
- [ ] T020 [H3] `CleanArchitectureRules` (ArchUnit) publicada como **test-jar** do `platform-commons` (os serviços a importam com `<type>test-jar</type>` e `<scope>test</scope>` **e** declaram `archunit-junit5` em scope test, porque test-jar não traz dependências transitivas), com um teste que prova que ela pega uma violação plantada. No mesmo passo, `CommonsBoundaryTest`: o código principal do commons não importa `org.springframework` nem `org.apache.kafka.streams`
- [ ] T020a [H3] **Teste de aceite** H3: app Spring Boot mínimo em `platform-commons/src/test/.../sampleapp/` (Spring Boot, Actuator e Micrometer Tracing + OTel só em escopo de teste) usando o `logback-base.xml`. Prova: log JSON com CPF mascarado (cenário 1), requisição com `traceparent` → o log tem o mesmo `traceId` (cenário 2) e `/actuator/prometheus` responde — `H3AcceptanceTest.java`

## Fase 5 — Gerador de carga (H4) — **adiada**
> Executar **depois do checkpoint da Fase 3 da 001** (o receiver já aceita e publica) e **antes da T122 da 001**, que é o primeiro uso do gerador. Também é pré-requisito de T428/T429 da 004. Motivo: o gerador só pode ser validado de ponta a ponta com o receiver no ar, e adiá-lo libera mais cedo o caminho até o motor.

- [ ] T020b [H4] Criar o módulo `load-generator` (depende de `contracts`) com teste "smoke" e declarado em `<modules>`
- [ ] T021 [H4] **Testes** do controle de taxa (token bucket), do sorteio de cenários (proporções ±1%) e da medição em modelo aberto: com um alvo simulado que trava 1 s, a latência registrada inclui o atraso de fila das requisições planejadas nesse intervalo (sem *coordinated omission*) — `load-generator/src/test/...`
- [ ] T022 [H4] Gerador de transações sintéticas, com cenários de fraude: sequestro de conta, rajada, pulverização e beneficiário em lista — `load-generator/src/main/.../scenarios/`
- [ ] T023 [H4] Cliente HTTP com virtual threads, controle de taxa, duplicatas e header `x-synthetic-scenario` — `load-generator/src/main/.../LoadRunner.java`
- [ ] T024 [H4] Relatório final: TPS alcançado, respostas por status, latência HTTP p50/p99 (HdrHistogram, medida a partir do horário planejado de cada requisição) — `load-generator/src/main/.../Report.java`
- [ ] T025 [H4] CLI (`--tps`, `--duration`, `--fraud-ratio`, `--duplicate-ratio`, `--target`) e execução com `./mvnw -pl load-generator exec:java -Dexec.args="--tps 8000 --duration 60s"`

## Fase 6 — Fechamento
- [ ] T026 Completar o `README.md` (subir e testar) e conferir o SC-001 do zero numa pasta limpa. A seção "carga" do README entra junto com a Fase 5 (T025)

## Dependências
Fase 1 → (Fases 2, 3, 4 em paralelo) → Fase 6. A 000 fecha sem a Fase 5.
Fase 5 (usa `contracts`): depois do checkpoint da Fase 3 da 001 e antes da T122 da 001.
Pronto para liberar 001–003 depois de: T004–T009, T014 e T017.
`platform-streams` não é desta spec: nasce na 003 (T301).
