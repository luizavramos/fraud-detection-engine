# Plano 004 — Motor de detecção

## 1. Resumo
Aplicação **Kafka Streams** (sem Spring) com a **Processor API**. O processamento de cada transação acontece numa única transação EOS:
1. dedup;
2. leitura do perfil (cópia local) e da blocklist (tabela global);
3. atualização dos agregados da conta;
4. avaliação das regras em memória;
5. emissão da decisão e do alerta.

Toda a lógica fica no caso de uso `EvaluateTransactionUseCase`, que é testável sem Kafka. A topologia só adapta.

Decisões de referência: ADR-2 (Kafka Streams), ADR-3 (perfil local), ADR-7 (idempotência), ADR-12 (histórico no fluxo), ADR-13 (regras por evento, via outbox).

## 2. Contexto técnico
| Item | Valor |
|---|---|
| Runtime | Java 21, Kafka Streams, ZGC geracional (`-XX:+UseZGC -XX:+ZGenerational`) |
| Garantia | `exactly_once_v2`, `commit.interval.ms=100` |
| Membership | `group.instance.id=${HOSTNAME}` (static membership), `num.standby.replicas=1` em prod (0 local) |
| Estado | RocksDB; em produção, volume EBS gp3 |
| Regras | `rules-kernel` (002) + `KafkaRuleSetProvider` (consome `rule-sets.v1`) |
| HTTP | `MiniHttpServer` do módulo `platform-streams` (003/T301): `/actuator/health/liveness`, `/actuator/health/readiness` e `/actuator/prometheus`, os mesmos caminhos dos serviços Spring (constituição VII) |
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

## 6. Regras por evento (`adapters/gateway/rules/KafkaRuleSetProvider`)
- **Fonte:** o tópico `rule-sets.v1` (uma partição, compactado, chave fixa `active`). Cada evento é o conjunto completo no formato `rule-set.v1`, publicado pela gestão via outbox e Debezium (ADR-13).
- **Consumo:** um consumidor simples, fora do Kafka Streams e fora do caminho da transação. Lê o último evento ao subir e continua ouvindo.
- **Evento novo:** se a versão for maior que a atual, compila com `RuleCompiler.compileAll`. Se OK: `AtomicReference.set` e atualiza o gauge de versão. Se falhar: mantém o atual e incrementa a métrica de falha. Versão menor ou igual: ignora (é um reenvio).
- **Kafka fora do ar:** mantém o conjunto atual e registra a idade dele.
- **Inicialização:** lê o último evento do tópico; se não houver nenhum, `ready=false`. A topologia **só inicia** quando `ready=true`. Não há snapshot em disco: o tópico compactado já é o snapshot.

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
| `KafkaRuleSetProvider` (Kafka real) | evento novo troca a versão, versão repetida ou menor ignora, regra inválida mantém a anterior, sem evento a readiness falha |
| Integração (Testcontainers Kafka) | EOS real: reprocessamento após crash simulado não duplica |
| Carga | `load-generator` 8k TPS × 5 min: p99 fim a fim medido |
| Caos | kill -9 de uma instância durante a carga + **consumidor-auditor** que confere uma decisão por `transactionId` |
| ArchUnit | regra de dependência |

## 9. Constitution Check
- **I:** ✅ caso de uso puro; topologia como adapter.
- **II:** ✅.
- **III:** ✅ dedup no store + EOS + `alertId` determinístico.
- **IV:** ✅ FR-010; o consumo de regras roda fora do caminho da transação.
- **V:** ✅.
- **VI:** ✅ só tokens no estado e nas saídas.
- **VII:** ✅ §7.
- **VIII:** ✅ centavos `long`, `Clock`.

## 10. Riscos
- **Serde do `AccountFeatures` mal feito degrada a latência:** medido no teste de carga. Fallback: JSON (mais simples e mais lento).
- **Rebalance durante deploy afeta o p99.9:** static membership + rolling deploy; aceito e documentado.
- **Partição quente (conta PJ gigante):** métrica de lag por partição; estratégia dedicada é `[DOC]`.

## 4A. Decisões da implementação (Fase 2)
- **Referência de valor anterior à transação:** `avgAmount30d` e `maxAmount90d` são calculados **antes** de registrar a transação atual (como `isNewBeneficiary`/`isNewDevice`); contagens e somas incluem a atual (H3.1). É a leitura que fecha com o cenário H3.2 (média de R$ 200 e chegando R$ 2.000 dá `avgAmount30d = 20000`); incluir a atual inflaria a própria referência. Sem histórico a média e o máximo são 0, então as regras de desvio precisam testar `> 0`.
- **Janelas alinhadas aos buckets:** `count1m` é o minuto corrente; `count5m`, `count1h` (60 minutos), `count24h` (24 horas) e `count30d` (30 dias) são contadas em buckets de calendário que terminam no instante da própria transação (`occurredAt`), então uma "janela de 24 h" cobre de 23 a 24 horas. Dias em UTC; `tx.hourOfDay` em America/Sao_Paulo.
- **Evento velho demais:** o `BucketRing` recusa o evento cujo slot já foi reaproveitado por um instante mais novo (não apaga dado novo); cada anel decide sozinho, então um evento de 3 h atrás sai das janelas de minutos mas entra nas de hora e dia (é o que o backfill precisa).
- **Sem dispositivo informado** (ex.: canal BRANCH), `isNewDevice` é falso e nada é registrado.
- **Serde:** varints com zigzag, buckets em deltas contra o mais recente, versão no primeiro byte; fica em `adapters/gateway/kafka` para o domínio não conhecer o formato.
- **jqwik:** o surefire do módulo inclui `**/*Properties.java`.

## 5A. Decisões da implementação (Fase 3)
- **Perfil ausente:** o `EvaluationContext` exige todas as features do catálogo, então sem perfil entram valores neutros: `profile.segment = "UNKNOWN"` (nenhuma sobreposição por segmento casa), `profile.accountAgeDays = -1` e `profile.hoursSinceCredentialChange = -1`. As regras iniciais testam `hoursSinceCredentialChange >= 0`, então R01 e R12 não disparam; uma regra nova que use `profile.*` precisa tratar o `-1` como "desconhecido" (vale também para perfil sem troca de credencial registrada). A decisão sai com `contextCompleteness = PARTIAL`.
- **Tipos:** `Severity` e `ContextCompleteness` vêm do módulo `contracts` (não foram duplicados no domínio); `Decision` do motor monta direto o `FraudDecision` e o `FraudAlert`. `EvaluateTransactionUseCase.execute(tx, receivedAt, traceId)` recebe também o trace (o plano só citava `receivedAt`): se vier ausente ou inválido, o `traceId` do alerta é derivado do `transactionId`, estável.
- **Identificadores:** `alertId = UUIDv5(NAMESPACE, transactionId)` (como o schema pede) e `decisionId = UUIDv5(NAMESPACE, "decision:" + transactionId)`; reprocessar a mesma transação gera os mesmos ids.
- **Conjunto de regras:** o caso de uso consulta o provedor antes de marcar o dedup; sem conjunto lança `IllegalStateException` e nada é marcado nem gravado (a topologia só inicia com o conjunto carregado, T423).
- **Fakes:** o `InMemoryFeatureStore` passa pelo serde binário a cada `load`/`save`, para o teste pegar um `save` esquecido.
- **Camadas:** com `application` populada, o `archunit.properties` temporário saiu.

## 3A. Decisões da implementação (Fase 4)
- **Atraso de evento:** `occurredAt` mais de 2 min atrás do `receivedAt` entra no bucket do horário de chegada e o `Outcome` marca `late` (a métrica `engine_late_events_total` usa isso na Fase 6); dentro de 2 min vale o `occurredAt`. A lacuna estava no caso de borda da spec e no plano, mas nenhuma tarefa a cobria.
- **Leitura inicial:** os cinco tópicos de entrada começam do **earliest** quando o grupo é novo (o padrão do Streams é `latest`, que perderia perfis, blocklist e backfill já publicados, e deixaria transações sem decisão, contra H6); o dedup protege contra o reprocessamento.
- **Perfil e blocklist:** o perfil é um `KTable` cujo store é ligado ao processor (mesmo sub-topology, o que exige `transaction-commands.v1`, `customer-profile.v1` e `transaction-history-backfill.v1` com o mesmo número de partições; o `smoke.sh` já confere); a blocklist é uma `GlobalKTable` lida pelo nome do store.
- **DLQ:** o Kafka Streams 4.1 ainda não tem DLQ embutida para erro de desserialização; o `DlqDeserializationHandler` publica com um produtor próprio (não transacional) em `transaction-commands.dlq`, com o motivo e a origem (tópico, partição, offset) nos cabeçalhos; se a DLQ falhar, o handler manda falhar o app em vez de descartar. Vale para todos os tópicos de entrada. Os serdes validam os campos obrigatórios, então JSON válido mas incompleto também vai para a DLQ.
- **Limpeza do `seen-tx`:** punctuator de relógio de parede a cada 5 min, varredura incremental (50 mil chaves por rodada, com cursor) e retenção de 24 h.
- **`platform-streams`:** `StreamsAppBootstrap.create(settings, propriedadesExtras, topologia)` aceita propriedades do app (handler de DLQ, `commit.interval.ms=100`), recusando as reservadas (`processing.guarantee`, id da aplicação, servidores, estado e `group.instance.id`).
- **`DetectionEngineApp`:** só inicia a topologia quando há conjunto de regras (a prontidão também exige); até o `KafkaRuleSetProvider` (Fase 5) o `main` fica aguardando.

## 6A. Decisões da implementação (Fase 5)
- **Prontidão só no fim do tópico:** ao subir, o provedor registra o offset final de `rule-sets.v1` e só fica pronto (`ready`) depois de ler até ele; sem isso uma instância nova ficava pronta ao aplicar o primeiro evento e decidia por um instante com uma versão antiga. Foi pego pelo teste com Kafka real. Tópico vazio: `ready` só depois que chegar o primeiro evento.
- **Estrutura:** `RuleSetLoader` (puro: valida contra `rule-set.v1`, ignora versão menor ou igual, compila com o kernel, troca de forma atômica; falhas por motivo `invalid`, `compile` ou `kafka`) e `KafkaRuleSetProvider` (consumidor sem grupo, partição 0, lê do início; em thread própria; falha do Kafka mantém o conjunto e tenta de novo; na partida, se o Kafka estiver fora, repete a inicialização). `RuleSetMetrics` é a porta; o adaptador Micrometer entra na Fase 6.
- **`pollOnce` e `initialize` públicos** para os testes dirigirem o consumidor sem thread (com `MockConsumer`).
- **Spec H5 corrigida:** o cenário 3 falava em "conjunto salvo em disco" (herança da versão por polling); com o ADR-13 por evento o tópico compactado é o snapshot, e a meta de ponta a ponta é ≤ 35 s.

## 7A. Decisões da implementação (Fase 6)
- **Porta e adaptador:** `EngineMetrics` (decisão, duplicata, atraso, DLQ) e `RuleSetMetrics` na camada `application`; `MicrometerEngineMetrics` implementa as duas. Todas as séries nascem em zero (severidades, motivos de falha), para o painel não ficar vazio antes do primeiro evento.
- **Latência fim a fim:** `engine_e2e_latency_seconds` é medida na publicação da decisão, a partir do `receivedAt` (cabeçalho do receiver; sem ele, a hora do registro), com buckets de 5 ms a 5 s e o SLO de 0,5 s como fronteira exata.
- **Métricas extras além do plano:** `engine_rule_errors_total{rule_id}` (regras que lançaram erro na avaliação) e o rótulo `mode` em `engine_rule_hits_total` (ativa ou sombra).
- **Idade do conjunto:** `engine_ruleset_age_seconds` conta desde a `publishedAt` do conjunto em uso (não desde o carregamento), então um conjunto antigo mas estável também mostra idade alta; interpretar junto com a versão.
- **Lag por partição:** em nível INFO o Kafka só expõe `records_lag_max` por consumidor; o lag por partição exige métricas em DEBUG do Kafka Streams (mais custo). O painel usa o máximo por instância e a limitação está na descrição do painel.
- **Registro de métricas compartilhado:** o `main` cria o `PrometheusMeterRegistry` antes de tudo e o passa ao `StreamsAppBootstrap` (novo `create(settings, extras, registro, topologia)`), porque o provedor de regras e a topologia usam o mesmo registro. O handler de DLQ recebe as métricas pelas propriedades.
- **Seed da blocklist:** `infra/seed/blocklist.sh [add|remove]` publica 3 contas e 2 dispositivos sintéticos (e tombstones para remover); o motivo segue o padrão do contrato (`CONFIRMED_FRAUD`, `MULE_ACCOUNT`...).

## 8A. Decisões e achados da implementação (Fase 7)
- **Auditor:** repositório `auditor` (jar executável, sem Spring). Lê `fraud-decisions.v1` e `fraud-alerts.v1` com `read_committed`, compara com o log de transações aceitas do gerador (`--accepted-log`) e reprova se houver faltante, decisão ou alerta duplicado, ou MEDIUM/HIGH sem alerta. Decisões fora do log (um 503 que o Kafka gravou, ou outra rodada) são informativas, mas duplicidade nelas também reprova.
- **Segunda instância para o caos:** `detection-engine-2` no perfil `chaos` do compose (mesmo `application.id`); o Prometheus raspa as duas. O Kafka Streams só entrega tarefas a uma instância nova depois dos *probing rebalances* (padrão 10 min), então o motor fixa `probing.rebalance.interval.ms` em 60 s, o mínimo aceito, para uma instância nova receber tarefas em cerca de 1 min.
- **Limite local de carga:** cerca de 9 mil conexões por rodada (portas efêmeras do Windows); por isso 300 TPS por 20 s e 150 TPS por 45 s. 8.000 e 25.000 TPS não foram medidos nesta entrega (etapa futura, na nuvem).
- **Latência fim a fim a 300 TPS local:** p99 de 230 ms com o motor aquecido (925 ms com o motor frio); detalhes e ressalvas em `docs/resultados-carga.md`.
- **Spec:** SC-003 passou de 10 s para 35 s de ponta a ponta (medido: 13 a 20 s), como já estava no H5 e no T424.
