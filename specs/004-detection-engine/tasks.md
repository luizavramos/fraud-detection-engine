# Tarefas 004 — Motor de detecção

Base de caminhos: `detection-engine/src/{main,test}/java/br/com/motorfraude/engine/`

## Fase 1 — Setup
- [ ] T401 Criar o módulo `detection-engine` (depende de `contracts`, `platform-commons`, `platform-streams` e `rules-kernel`) + teste ArchUnit
- [ ] T402 [P] Adicionar ao compose (porta 8084, volume `/data`, `RULES_URL`, `RULES_POLL_INTERVAL`) e ao Prometheus

## Fase 2 — Estruturas de dados (H3)
- [ ] T403 **Testes property-based** de `BucketRing` (janela = força bruta; rotação; eventos fora de ordem até 2 min) — `domain/features/BucketRingProperties.java`
- [ ] T404 Implementar `BucketRing`
- [ ] T405 **Testes** de `RecentSet` (limite N, LRU, novidade) — `domain/features/RecentSetTest.java`
- [ ] T406 Implementar `RecentSet`
- [ ] T407 **Testes** de `AccountFeatures` + `FeatureSnapshot`: cada feature do catálogo (count1m…count30d, sum1h/24h, avgAmount30d, maxAmount90d, isNewBeneficiary/isNewDevice lidos antes do registro, hourOfDay em America/Sao_Paulo) — `domain/features/*Test.java`
- [ ] T408 Implementar `AccountFeatures` e `FeatureSnapshot`
- [ ] T409 **Testes** do serde binário de `AccountFeatures` (ida e volta property-based; tamanho ≤ 5 KB numa conta cheia, SC-004)
- [ ] T410 Implementar `AccountFeaturesSerde` (varints, header de versão)

## Fase 3 — Decisão (H1, H4)
- [ ] T411 **Testes** de `Decision`/`Alert`: severidade pelas faixas, `alertId` UUIDv5 estável, PARTIAL quando falta o perfil — `domain/DecisionTest.java`
- [ ] T412 Implementar `Decision`, `Alert`, `Severity`, `ContextCompleteness`
- [ ] T413 Portas `DedupStore`, `FeatureStore`, `ProfileReader`, `BlocklistReader` e `RuleSetProvider`, com fakes em memória
- [ ] T414 **Testes** do `EvaluateTransactionUseCase`: sem regra → NO_ALERT; 55 pts → MEDIUM; ≥70 → HIGH; duplicata; perfil ausente; blocklist; SHADOW não soma — usando o `CompiledRuleSet` real do kernel com as 8 regras iniciais
- [ ] T415 Implementar `EvaluateTransactionUseCase` (plan §5)

**Checkpoint:** toda a lógica do motor testada sem Kafka.

## Fase 4 — Topologia (H1, H2, H4, H7, H8, H9)
- [ ] T416 **Testes `TopologyTestDriver`:** decisão em `fraud-decisions.v1`, alerta só ≥ MEDIUM em `fraud-alerts.v1`, dedup, perfil da KTable local, blocklist global com tombstone, backfill sem saída, DLQ; toda decisão e todo alerta emitidos passam em `ContractAssertions.assertProduces` (`fraud-decision.v1`, `fraud-alert.v1`; inclui os limites de `features{}`) — `infrastructure/TopologyFactoryTest.java`
- [ ] T417 Implementar os adapters de store (`StateStoreDedupStore`, `StateStoreFeatureStore`, `KTableProfileReader`, `GlobalBlocklistReader`)
- [ ] T418 Implementar `EvaluateProcessor` (lê `receivedAt` e `traceparent`; encaminha `Outcome`) e `BackfillProcessor` (também consulta e marca o `seen-tx`, para que um registro de backfill repetido não conte duas vezes nos buckets)
- [ ] T419 Implementar o punctuator de limpeza do `seen-tx` (24 h) + teste
- [ ] T420 Implementar o `DlqDeserializationHandler` + teste
- [ ] T421 Implementar `TopologyFactory` e `DetectionEngineApp` (usa `StreamsAppBootstrap`, EOS v2, `commit.interval.ms=100`, static membership)

## Fase 5 — Regras a quente (H5)
- [ ] T422 **Testes** (WireMock) do `HttpRuleSetProvider`: 200 troca a versão; 304 mantém; regra inválida mantém a anterior e incrementa a métrica; erro de rede mantém; snapshot gravado; inicialização com snapshot; sem nada → `ready=false`
- [ ] T423 Implementar `HttpRuleSetProvider` + `RuleSetSnapshotFile` (escrita atômica) e ligar a readiness ao início da topologia
- [ ] T424 **Teste de aceite** H5 com a gestão de regras real (Testcontainers ou compose): criar regra nova, mudar threshold, desligar e fazer rollback pela API → decisões refletem cada mudança em ≤ 35 s, **sem deploy nem restart do motor** (o teste confere que o uptime e o id do processo não mudaram)

## Fase 6 — Observabilidade
- [ ] T425 Métricas do plan §7 (incluindo o histograma fim a fim a partir de `receivedAt`) + teste de que são registradas
- [ ] T426 Dashboard do motor no Grafana (p99 × SLO, severidade, alertas por regra × média das últimas horas, lag por partição, versão do conjunto por instância)
- [ ] T427 Seed da blocklist para demonstração: `infra/seed/blocklist.sh`

## Fase 7 — Prova dos requisitos não funcionais (H6, SC-001/002)
- [ ] T428 **Consumidor-auditor** (`tools/auditor`): lê os `transactionId` aceitos (log do `load-generator`) e `fraud-decisions.v1`, e reporta faltantes e duplicados
- [ ] T429 **Teste de carga:** `load-generator --tps 8000 --duration 300s` + extração do p99 fim a fim do Prometheus → resultado registrado em `docs/resultados-carga.md`
- [ ] T430 **Teste de caos:** durante a carga, `docker kill` numa instância do motor (rodando 2) → auditor com 0 faltantes e 0 duplicados → registrado em `docs/resultados-caos.md`

## Dependências
Fase 1 → Fase 2 → Fase 3 (precisa do kernel: 002 Fase 2) → Fase 4 → Fase 5 (precisa de 002 Fase 5 para o T424) → Fase 6 → Fase 7 (precisa de 001 no ar).
Antes de 003 e 006 existirem, os testes publicam perfis e backfill direto nos tópicos.
