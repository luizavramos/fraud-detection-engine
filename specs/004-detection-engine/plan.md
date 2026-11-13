# Plano 004 — Motor de detecção

## 1. Resumo
Aplicação **Kafka Streams** (sem Spring) com a **Processor API**. O processamento de cada transação acontece numa única transação EOS:
1. dedup;
2. leitura do perfil (cópia local) e da blocklist (tabela global);
3. atualização dos agregados da conta;
4. avaliação das regras em memória;
5. emissão da decisão e do alerta.

Toda a lógica fica no caso de uso `EvaluateTransactionUseCase`, que é testável sem Kafka. A topologia só adapta.

Decisões de referência: ADR-2 (Kafka Streams), ADR-3 (perfil local), ADR-7 (idempotência), ADR-12 (histórico no fluxo), ADR-13 (regras por polling).

## 2. Contexto técnico
| Item | Valor |
|---|---|
| Runtime | Java 21, Kafka Streams, ZGC geracional (`-XX:+UseZGC -XX:+ZGenerational`) |
| Garantia | `exactly_once_v2`, `commit.interval.ms=100` |
| Membership | `group.instance.id=${HOSTNAME}` (static membership), `num.standby.replicas=1` em prod (0 local) |
| Estado | RocksDB; em produção, volume EBS gp3 |
| Regras | `rules-kernel` (002) + `HttpRuleSetProvider` (polling com ETag) |
| HTTP | `MiniHttpServer` do módulo `platform-streams` (003/T301): `/health/live`, `/health/ready`, `/metrics` |
| Porta | 8084 |

## 3. Topologia
```
transaction-commands.v1 ──► [EvaluateProcessor] ──┬──► fraud-decisions.v1   (todas)
        (dedup, features, perfil, regras)         └──► fraud-alerts.v1      (MEDIUM/HIGH)
             stores: seen-tx, account-features
             lê:     customer-profile-store (KTable, mesmo sub-topology, co-particionada)
                     blocklist-store (GlobalKTable)

transaction-history-backfill.v1 ──► [BackfillProcessor] ── escreve em account-features (sem saída)

customer-profile.v1 ──► table("customer-profile-store")
blocklist.v1        ──► globalTable("blocklist-store")
```
- `seen-tx`: `KeyValueStore<transactionId, epochMillis>`. A limpeza de chaves com mais de 24 h é feita por um *punctuator* (wall clock, a cada 5 min, varredura incremental).
- `account-features`: `KeyValueStore<accountId, AccountFeatures>`, com serde binário compacto e versionado.
- **Desserialização com erro:** `DlqDeserializationHandler` envia o registro bruto + motivo para `transaction-commands.dlq` e continua.

## 4. Domínio (estruturas de dados — "algoritmos" do case)
```
domain/
├── TransactionCommand.java
├── features/
│   ├── BucketRing.java          # anel de N buckets {count,sum,max} com rotação preguiçosa por índice de tempo
│   ├── AccountFeatures.java     # minutes(60×1min), hours(24×1h), days(90×1d), beneficiaries(RecentSet 50), devices(RecentSet 10)
│   ├── RecentSet.java           # LRU limitado: token → lastSeen; O(1) amortizado
│   └── FeatureSnapshot.java     # valores calculados p/ o catálogo (count5m, sum1h, avgAmount30d, isNewBeneficiary…)
├── CustomerProfile.java
├── Decision.java                # score, severity, hits, shadowHits, snapshot, ruleSetVersion, completeness
├── Alert.java                   # alertId = UUIDv5(NAMESPACE, transactionId)
└── Severity.java, ContextCompleteness.java
```
**`BucketRing`**
- **Estrutura:** arrays paralelos `long[] count, sum, max` + `long[] slotEpoch`.
- **Atualização:** `idx = epochUnit % N`; se `slotEpoch[idx] != epochUnit`, zera o slot antes de somar. O(1).
- **Consulta de janela:** soma os slots com `slotEpoch` dentro da janela. O(N).
- **Ordem do cálculo:** a feature é calculada **depois** de incluir a transação atual (spec H3.1). `isNewBeneficiary` e `isNewDevice` são lidos **antes** de registrar a transação.
- **Tamanho:** (60+24+90) × 32 bytes ≈ 5,6 KB bruto; com o serde compacto (varints, slots vazios omitidos), a meta é ≤ 5 KB (SC-004).

## 5. Caso de uso
```java
public final class EvaluateTransactionUseCase {
  // portas: DedupStore, FeatureStore, ProfileReader, BlocklistReader, RuleSetProvider, Clock
  public Outcome execute(TransactionCommand cmd, Instant receivedAt) {
    if (dedup.seen(cmd.transactionId())) return Outcome.duplicate();
    dedup.mark(cmd.transactionId(), clock.instant());

    var features = featureStore.load(cmd.accountId());          // ou vazio
    var before = features.noveltyFor(cmd);                       // isNewBeneficiary/isNewDevice
    features.record(cmd);                                        // inclui a transação atual
    featureStore.save(cmd.accountId(), features);

    var profile = profiles.find(cmd.accountId());                // Optional
    var lists = blocklist.flagsFor(cmd);
    var snapshot = FeatureSnapshot.of(cmd, features, before, profile, lists, ZONE_SP);
    var ruleSet = ruleSets.current();                            // CompiledRuleSet em memória
    var eval = ruleSet.evaluate(snapshot.toContext());

    var decision = Decision.from(cmd, eval, snapshot, ruleSet.version(),
                                 profile.isPresent() ? FULL : PARTIAL, receivedAt, clock.instant());
    return Outcome.decided(decision, decision.toAlertIfAny());
  }
}
```
O `EvaluateProcessor` (adapter) converte o registro em `TransactionCommand`, lê `receivedAt` do header, chama o caso de uso e encaminha `Outcome` para os dois sinks. As portas de store são implementadas por adapters que envolvem os state stores do `ProcessorContext`.

## 6. Regras por polling (`adapters/out/rules/HttpRuleSetProvider`)
- **Agendamento:** `ScheduledExecutorService` a cada `RULES_POLL_INTERVAL` (padrão 30 s).
- **Requisição:** `GET {RULES_URL}/v1/rule-sets/active` com `If-None-Match`, timeout de 2 s.
- **`200`:** compila com `RuleCompiler.compileAll`. Se OK: `AtomicReference.set`, grava `/data/ruleset.json` (escrita atômica: arquivo temporário + rename) e atualiza o gauge de versão. Se falhar: mantém o atual e incrementa a métrica de falha.
- **`304`:** nada muda.
- **Erro de rede:** mantém o atual e registra a idade do conjunto.
- **Inicialização:** tenta o polling; se falhar, carrega o snapshot; se não houver nenhum dos dois, `ready=false`. A topologia **só inicia** quando `ready=true`.

## 7. Observabilidade
| Métrica | Tipo | Labels |
|---|---|---|
| `engine_e2e_latency_seconds` | histogram (SLO 0,5 s) | — |
| `engine_decisions_total` | counter | `severity` (NONE/MEDIUM/HIGH) |
| `engine_rule_hits_total` | counter | `rule_id`, `mode` (active/shadow) |
| `engine_duplicates_dropped_total` | counter | — |
| `engine_context_partial_total` | counter | — |
| `engine_ruleset_version` | gauge | — |
| `engine_ruleset_age_seconds` | gauge | — |
| `engine_ruleset_load_failures_total` | counter | `reason` |
| `engine_late_events_total`, `engine_dlq_total` | counter | — |
| métricas nativas do Kafka Streams (lag, rebalances, commit) | via `KafkaStreamsMetrics` | — |

O dashboard Grafana do motor mostra: p99 fim a fim × SLO, decisões por severidade, **alertas por regra comparados com a média das últimas horas** (para detectar falso positivo súbito), lag por partição, versão do conjunto por instância e duplicatas.

## 8. Testes
| Nível | O que cobre |
|---|---|
| Property-based (jqwik) | `BucketRing`: para qualquer sequência de eventos, a soma da janela é igual à força bruta; `RecentSet` nunca excede N |
| Domínio | `FeatureSnapshot` (cada feature do catálogo), `Decision`/`Alert` (UUIDv5 estável, severidade) |
| Caso de uso (portas em memória) | H1–H4 e H7 com `CompiledRuleSet` de teste |
| `TopologyTestDriver` | dedup, saídas nos tópicos certos, perfil ausente → PARTIAL, backfill sem saída, DLQ, blocklist |
| `HttpRuleSetProvider` (WireMock) | 200/304/erro, regra inválida mantém a anterior, snapshot em disco, readiness |
| Integração (Testcontainers Kafka) | EOS real: reprocessamento após crash simulado não duplica |
| Carga | `load-generator` 8k TPS × 5 min: p99 fim a fim medido |
| Caos | kill -9 de uma instância durante a carga + **consumidor-auditor** que confere uma decisão por `transactionId` |
| ArchUnit | regra de dependência |

## 9. Constitution Check
- **I:** ✅ caso de uso puro; topologia como adapter.
- **II:** ✅.
- **III:** ✅ dedup no store + EOS + `alertId` determinístico.
- **IV:** ✅ FR-010; o polling de regras roda fora do caminho da transação.
- **V:** ✅.
- **VI:** ✅ só tokens no estado e nas saídas.
- **VII:** ✅ §7.
- **VIII:** ✅ centavos `long`, `Clock`.

## 10. Riscos
- **Serde do `AccountFeatures` mal feito degrada a latência:** medido no teste de carga. Fallback: JSON (mais simples e mais lento).
- **Rebalance durante deploy afeta o p99.9:** static membership + rolling deploy; aceito e documentado.
- **Partição quente (conta PJ gigante):** métrica de lag por partição; estratégia dedicada é `[DOC]`.
