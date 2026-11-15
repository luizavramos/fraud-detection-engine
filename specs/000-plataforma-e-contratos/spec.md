# Spec 000 — Plataforma e contratos

**Status:** implementada (testes e `verify` verdes; ver `docs/uso-de-ia.md`) · **Prioridade:** fundação (bloqueia todas as outras)

## Contexto
Antes de qualquer serviço, o projeto precisa de quatro coisas:
- um repositório que compila;
- um ambiente local que sobe com um comando;
- contratos de mensagens que todos os serviços compartilham;
- uma forma de gerar carga para provar os 8.000/25.000 TPS do case.

Esta spec cobre esse chão comum.

## Histórias de usuário

### H1 — Subir o ambiente com um comando (P1)
Como avaliador do case, quero rodar `docker compose up` e ter toda a infraestrutura pronta, para executar a solução sem configurar nada à mão.

**Teste independente:** num ambiente limpo, `docker compose up -d` deixa todos os containers *healthy* em até 3 minutos, e os tópicos Kafka existem.

**Cenários de aceite**
1. **Given** uma máquina só com Docker, **When** rodo `docker compose up -d`, **Then** Kafka, Postgres, Redis, Kafka Connect, Prometheus e Grafana ficam *healthy*.
2. **Given** o Kafka no ar, **When** o container de inicialização termina, **Then** todos os tópicos da tabela de contratos existem, com as partições e a política (delete/compact) definidas.
3. **Given** o ambiente no ar, **When** abro o Grafana em `localhost:3000`, **Then** o datasource Prometheus já está configurado e o dashboard "Motor de fraude" aparece (vazio até os serviços subirem).

### H2 — Contratos compartilhados e validados (P1)
Como desenvolvedor de qualquer serviço, quero schemas JSON versionados e classes Java geradas a partir deles, para que produtores e consumidores não divirjam.

**Teste independente:** os testes do módulo `contracts` validam exemplos válidos e inválidos de cada schema.

**Cenários de aceite**
1. **Given** o exemplo oficial de `EvaluateTransaction`, **When** é validado contra o schema, **Then** passa.
2. **Given** um exemplo com `amount.value` decimal (ex.: `10.5`), **When** é validado, **Then** falha. Dinheiro só aceita inteiro em centavos.
3. **Given** um exemplo com um campo `cpf`, **When** é validado, **Then** falha (`additionalProperties: false` nos campos de identificação).

### H3 — Base comum de observabilidade e privacidade (P1)
Como responsável por SRE, quero que todo serviço já nasça com métricas, logs JSON e mascaramento de dados pessoais, para não depender de cada time lembrar.

**Teste independente:** um app Spring Boot mínimo, que existe só nos testes do `platform-commons` (Spring em escopo de teste), usa o `logback-base.xml` do módulo, expõe `/actuator/prometheus`, e seus logs saem em JSON, mascarados.

**Cenários de aceite**
1. **Given** um log com o texto `CPF 123.456.789-09`, **When** é escrito, **Then** sai como `CPF ***.***.***-**`.
2. **Given** uma requisição com header `traceparent`, **When** o serviço loga, **Then** o log contém o mesmo `traceId`.

### H4 — Gerar carga sintética com fraudes conhecidas (P2)
> **Execução adiada:** implementada depois do checkpoint da Fase 3 da 001 (receiver no ar) e antes da T122 da 001. Ver `tasks.md`, Fase 5.

Como candidato que precisa provar os números do case, quero um gerador de carga com taxa configurável e cenários de fraude embutidos, para medir vazão e latência e demonstrar alertas.

**Teste independente:** `load-generator --tps 8000 --duration 60s` envia 480 mil transações e imprime um resumo com a taxa alcançada.

**Cenários de aceite**
1. **Given** `--tps 8000`, **When** roda por 60 s, **Then** a taxa média medida fica entre 7.600 e 8.400 TPS (±5%).
2. **Given** `--fraud-ratio 0.005`, **When** roda, **Then** ~0,5% das transações seguem um dos cenários de fraude (ex.: sequestro de conta, rajada), marcadas no header `x-synthetic-scenario` para conferência.
3. **Given** `--duplicate-ratio 0.01`, **When** roda, **Then** ~1% das requisições reenvia um `transactionId` já enviado.

## Casos de borda
- Porta ocupada na máquina do avaliador: as portas ficam configuráveis por `.env`.
- Máquina com pouca memória: o compose usa limites modestos (Kafka 1 GB, Postgres 512 MB). Os 8.000 e 25.000 TPS ficam como etapa futura, na nuvem (não medidos nesta entrega); a escala medida é a de referência local, de 300 TPS.

## Requisitos funcionais
- **FR-001:** o repositório DEVE ser um projeto Maven multi-módulo, compilável com `./mvnw verify` (testes unitários e de integração).
- **FR-002:** DEVE existir `docker-compose.yml` com Kafka (KRaft, um broker), Kafka Connect com Debezium, Postgres (projeto), Postgres "banco legado" (fonte do CDC), Redis, Prometheus e Grafana.
- **FR-003:** um container de inicialização DEVE criar todos os tópicos da tabela de contratos (plan §3).
- **FR-004:** o módulo `contracts` DEVE conter os JSON Schemas e os records Java de todos os contratos, com exemplos válidos e inválidos.
- **FR-005:** o módulo `platform-commons` DEVE fornecer: logs JSON com mascaramento de dados pessoais, leitura/geração do `traceparent` e sua propagação em headers Kafka, a porta `Clock` e regras ArchUnit reutilizáveis. Ele **NÃO DEVE** depender de Spring nem de Kafka Streams (verificado por ArchUnit). Nos serviços Spring, a propagação HTTP do `traceparent`, o `traceId` no log e as métricas padrão vêm do Spring Boot (Micrometer Tracing + ponte OpenTelemetry, Actuator), sem código próprio.
- **FR-005a:** a base comum dos apps Kafka Streams (`StreamsAppBootstrap`, `MiniHttpServer`) fica num módulo separado, `platform-streams`, criado na spec 003 (T301).
- **FR-006:** o módulo `load-generator` DEVE enviar transações por HTTP ao receiver com taxa, duração, proporção de fraude e de duplicatas configuráveis.
- **FR-007:** o `README.md` da raiz DEVE explicar como subir, testar e rodar a carga em até 5 comandos.

## Entidades principais
`EvaluateTransaction`, `CustomerProfile`, `TransactionHistoryRecord`, `BlocklistEntry`, `FraudDecision`, `FraudAlert`, `RuleSet`. Os detalhes estão em plan §3.

## Critérios de sucesso
- **SC-001:** do clone ao ambiente no ar em ≤ 5 comandos e ≤ 5 minutos.
- **SC-002:** 100% dos contratos com schema, record Java e ao menos um exemplo válido e um inválido testados.
- **SC-003:** o gerador sustenta 8.000 TPS por 5 minutos numa máquina de 8 vCPU **(não verificado nesta entrega: medição na nuvem é etapa futura; localmente a escala de referência é 300 TPS, ver `docs/resultados-carga.md`)**.

## Premissas
- O avaliador tem Docker e JDK 21.
- Ambiente local com um broker Kafka e 6 partições por tópico. Produção usa 48 partições e 3 brokers (documentado, não reproduzido localmente).
- [NEEDS CLARIFICATION] **Formato:** JSON + JSON Schema (simplificação declarada) ou Avro + Schema Registry? *Padrão adotado: JSON + JSON Schema.*
