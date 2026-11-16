# Plano 006 — Transaction service

## 1. Resumo
Aplicação **Kafka Streams** sem estado (map/filter), com `exactly_once_v2`:
```
cdc.bank.transactions ─► filtrar op=r e janela ─► mapear (campos permitidos) ─► transaction-history-backfill.v1
```
Mesma base técnica de 003 e 004 (`StreamsAppBootstrap`, `MiniHttpServer`, do módulo `platform-streams`).

**Por que não um job em lote lendo o banco direto:** o snapshot do Debezium já resolve leitura consistente e retomada, e o motor já sabe consumir tópicos. Um job JDBC exigiria paginação, retomada e acesso de leitura ao banco de produção, que o CDC já entrega.

## 2. Contexto técnico
| Item | Valor |
|---|---|
| Runtime | Java 21, Kafka Streams, EOS v2 |
| Entrada | `cdc.bank.transactions` (conector Debezium com `snapshot.mode=initial`) |
| Saída | `transaction-history-backfill.v1` (mesmo número de partições do motor) |
| Controle de taxa | `max.poll.records` + limitador de taxa no processor (padrão 20 mil/s) |
| Porta | 8086 |

## 3. Tabela do banco legado (000, `infra/bank-db/init.sql`, schema Postgres `bank`)
```sql
transactions(transaction_id PK, account_token, counterparty_token, amount_cents bigint,
             device_hash, occurred_at timestamptz, payer_name, payer_cpf)   -- as duas últimas existem para provar que não vazam
```

## 4. Clean Architecture
```
transaction-service/src/main/java/br/com/motorfraude/transaction/
├── domain/
│   ├── HistoryWindow.java          # cutoverAt, 90 dias; contains(occurredAt)
│   └── HistoryRecord.java
├── application/
│   └── BackfillTransactionHistoryUseCase.java   # (evento CDC já mapeado) → Optional<HistoryRecord>
├── adapters/
│   ├── in/cdc/DebeziumTransactionMapper.java    # campos permitidos; só op=r
│   └── out/kafka/HistoryRecordSerde.java
└── infrastructure/
    ├── TransactionServiceApp.java, TopologyFactory.java
    └── BackfillStatus.java          # contadores + estado (RUNNING/COMPLETED) para /v1/backfill/status
```
**Detectar o fim:** o Debezium publica um evento de fim de snapshot (`snapshot=last`). Quando ele chega, o estado vira `COMPLETED`.

## 5. Testes
| Nível | O que cobre |
|---|---|
| Domínio | `HistoryWindow` (bordas: exatamente 90 dias, `cutoverAt` excluído) |
| Mapper | descarta `payer_name`/`payer_cpf`; ignora `op` ≠ `r` |
| Topologia (`TopologyTestDriver`) | filtro da janela, chave `accountId`, estado `COMPLETED` no último evento |
| Integração (Testcontainers + Debezium) | 10 mil transações → registros corretos no tópico |
| Em processo no motor (SC-001) | `avgAmount30d` de 100 contas de referência, calculado pela topologia real do motor a partir dos registros do serviço, = SQL (o motor não expõe features; ver T611) |
| ArchUnit | regra de dependência |

## 6. Constitution Check
- **I/II:** ✅.
- **III:** ✅ produtor transacional + dedup no motor.
- **IV:** ✅ fora do caminho quente.
- **VI:** ✅ campos permitidos + teste.
- **VII:** ✅ progresso e status.

## 7. Riscos
- **Snapshot grande competindo com o tráfego ao vivo no mesmo cluster:** controle de taxa + execução fora do horário de pico.
- **Reprocessar exige resetar o estado do motor:** procedimento documentado (H3). Não é automatizado no case.

## 8. Decisões da implementação (Fases 1 a 3)
- **Prova do SC-001 (decisão do usuário):** o motor não expõe features fora das regras, e não vamos acrescentar endpoint de diagnóstico a um serviço fechado. A prova é um teste **em processo no `detection-engine`**: a topologia real recebe os registros que o `transaction-service` produz para 100 contas de referência e `avgAmount30d` é comparado com o SQL; no compose só se prova o transporte (tópico). Mesmo assim, "diferença 0" exige definir o SQL com a mesma janela alinhada a buckets do motor (o motor usa a média de referência anterior à transação, ver 004), o que será decidido e documentado na Fase 4.
- **Seed separado do `init.sql`:** `infra/seed/transactions.sh` (e não o `init.sql`), porque o `init.sql` só roda na criação do volume do banco legado, que já existe com os clientes da 003. É determinístico (mesmo `N` e `ANCHOR` dão as mesmas linhas), idempotente e deve rodar **antes** de registrar o conector (o snapshot lê o que já estiver na tabela). 1 milhão de linhas em 120 dias leva ~35 s; com `ANCHOR=2026-11-15T00:00:00Z` ficam 750.082 dentro dos 90 dias e 249.918 fora. O primeiro teste com 20 mil linhas passou e escondeu um estouro de inteiro (`n * 104729` acima de ~20 mil linhas); apareceu ao rodar o tamanho real e foi corrigido com `bigint`.
- **Conector `transactions-cdc`:** slot e publicação próprios (`transactions_cdc_slot`, `dbz_transactions`), `snapshot.mode=initial`. As colunas `payer_name` e `payer_cpf` continuam no CDC de propósito (como em 003); é o mapeador que as descarta, com teste.
- **`CUTOVER_AT` sem valor padrão no serviço:** o compose repassa `${CUTOVER_AT:-}` e o serviço deve **recusar a partida** se vier vazio ou inválido (falha rápida, Fase 3); o `.env.example` traz um valor de exemplo. Um corte errado dobraria ou perderia transações, então não se adivinha.
- **Fixtures reais (Fase 2):** capturadas do Debezium 3.6.3 com uma tabela temporária no banco legado (cópia de `bank.transactions`), um conector descartável e o tópico lido do começo; tudo removido depois (conector, slot, publicação, tabela, tópico). Dois fatos que só a captura mostrou: o **primeiro** evento do snapshot vem com `source.snapshot="first"` (não `"true"`) e o **último** com `"last"`; a remoção (`d`) traz `before` com a chave e valores zerados (`account_token` vazio, `amount_cents` 0, `occurred_at` de 1970), por isso nada é lido dele.
- **Fim do snapshot = `source.snapshot="last"`**, marcado por `TransactionChange.lastOfSnapshot`, que vale mesmo quando esse último registro cai fora da janela (a topologia, na Fase 3, deve contar o fim antes de filtrar).
- **Janela:** início **incluído** (exatamente 90 dias entra), `cutoverAt` **excluído**. O backfill só aceita `op=r`; `c`/`u`/`d` voltam `SkipReason.NOT_SNAPSHOT` (rótulo da métrica `reason`), fora da janela voltam `OUT_OF_WINDOW`.
- **Onde ficou cada peça:** `domain/{HistoryWindow,HistoryRecord}`; `application/usecase/{BackfillTransactionHistoryUseCase,TransactionChange,SkipReason}`; `adapters/controller/cdc/{DebeziumTransactionMapper,CdcFormatException}`. `HistoryRecord` não tem campo de pagador: o dado pessoal nem existe no domínio. `device_hash` nulo é aceito.
- **`archunit.properties` temporário removido:** as camadas já têm classes e a regra de arquitetura passa sem a exceção.
- **`/v1/backfill/status` em porta própria (decisão do usuário):** o `MiniHttpServer` do `platform-streams` é compartilhado e só serve saúde e métricas. O serviço sobe um `HttpServer` do JDK só para o status, em `STATUS_PORT` (padrão 8096; compose publica 8096); saúde e métricas continuam em `HTTP_PORT` (8086). Não toca no `platform-streams` nem nos outros serviços. FR-004 vale, mas em duas portas.
- **`COMPLETED` precisa de silêncio:** `cdc.bank.transactions` tem 3 partições, então `snapshot=last` pode ser processado antes de registros de outra partição. `COMPLETED` = último evento visto **e** `BACKFILL_COMPLETION_QUIET_SECONDS` (padrão 10) sem nenhum evento novo; um retardatário volta para `RUNNING`. É heurística: um atraso maior que a janela no meio da carga a enganaria, e a leitura final de verdade é o lag do consumidor.
- **Estado entre reinícios:** os totais valem desde a partida do processo (ficam na memória; sob exactly-once um reinício no meio pode repetir contagem). Ao completar, o resultado é gravado em `STATE_DIR/backfill-status.json` e relido na partida, porque o evento `last` não chega de novo (offset confirmado); arquivo de outro `cutoverAt` é ignorado. Limite: reinício entre ver o `last` e passar a janela de silêncio fica em `RUNNING`.
- **Limitador (`RateLimiter`, 20 mil/s, balde de 100 ms):** bloqueia a thread do Streams, então o consumo anda na velocidade do limite. Configurável por `BACKFILL_RATE_PER_SECOND`. A taxa foi provada contra relógio falso; contra o motor real fica para a Fase 4.
- **Valor ≤ 0 no legado:** o contrato exige `amount.value ≥ 1`; esses registros são descartados como `invalid_record` em vez de produzir mensagem fora do contrato.
- **Falha rápida:** `CUTOVER_AT` ausente, inválido ou sem fuso derruba o processo antes de abrir porta ou falar com o Kafka (testado e verificado no jar).

## 9. Decisões da implementação (Fase 4) e resultados
- **SQL do SC-001 com a janela alinhada ao motor:** `avgAmount30d` do motor é a soma dos buckets de **dia UTC** em `(ref−30, ref]` dividida pela contagem (divisão inteira), calculada **antes** da transação avaliada. A referência em SQL usa, para a primeira transação ao vivo em `cutoverAt`, `occurred_at ≥ cutoverAt − 29 dias` e `< cutoverAt`, com `div(sum, count)`; `maxAmount90d` usa `≥ cutoverAt − 89 dias`; `count30d` do motor já inclui a transação avaliada (SQL + 1). Todas restritas ao que o backfill publica (`[cutoverAt − 90 d, cutoverAt)`). Um `sum()/count()` direto no Postgres devolve `numeric` com casas decimais: o primeiro IT quebrou por isso.
- **Fixture produzida pelo próprio serviço:** o `TransactionServiceIT` (Postgres, Kafka e Debezium reais, 10 mil linhas, 100 contas de referência com 2 mil delas, mais as 4 bordas) grava, com `-Dfixture.dir`, `reference-history.jsonl` (1.400 registros lidos do tópico) e `reference-expected.json` (SQL). Ficam em `detection-engine/src/test/resources/backfill-reference/`. Regerar só é preciso se a lógica de janela mudar.
- **Resultado do SC-001:** `BackfillReferenceAccuracyTest` (topologia real do motor em `TopologyTestDriver`) compara 3 features × 100 contas e deu **diferença 0**; passou de primeira, então fiz um controle negativo (adulterei uma média esperada em 1 centavo: o teste falhou apontando a conta) e restaurei o arquivo. Não prova o limite de taxa nem tráfego ao vivo.
- **Resultado do SC-002 (IT):** 0 registros fora da janela, 0 repetidos, ids iguais aos do SQL, sem nome nem CPF, status `COMPLETED` com `published` e `out_of_window` iguais ao SQL.
- **Transporte no compose (1 milhão de linhas, Debezium real):** `COMPLETED` com `published=750082` e `out_of_window=249918` (iguais ao SQL); o consumidor `read_committed` do tópico leu 750.082 mensagens, 750.082 `transactionId` distintos e 0 ocorrências de nome ou CPF. Os offsets das partições somam 756.762, a diferença são os marcadores de commit do exactly-once (não conferido mensagem a mensagem). Tempo da carga não foi medido com precisão e não é afirmado.
- **Memória:** o IT sobe Postgres, Kafka e Connect; com a pilha do compose já de pé num Docker de 4 GB o Connect do teste morre com código 137. Pare o compose antes de rodar o IT.
- **Antes da rodada do §10 estava sem verificação:** passos B do H3, painel do Grafana, limite de taxa contra o motor sob carga, janela de silêncio sob carga real.

## 10. Ensaios no compose com o motor de pé (base de 500 mil linhas, Docker limitado a ~2 GB)
Base zerada (`docker compose down -v`), 500.000 transações sintéticas, snapshot do Debezium real, ensaio com `infra/backfill-experiment.sh` e conferência com `infra/backfill-verify.sh`. Para caber em 2 GB, o banco e o Connect ficam desligados durante o ensaio.
- **Carga:** `COMPLETED` em ~220 s, `published=374976` + `out_of_window=125024` = 500.000, 0 inválidos. Uma transição de estado (`RUNNING` → `COMPLETED`), ~10 s depois do último registro, sem reversão. Não houve retardatário: a janela de silêncio só foi vista no caminho feliz.
- **Limitador:** testado com **2 mil/s**, não com o padrão de 20 mil/s. Medido ~1,6–2,2 mil/s no log (janelas de ~9 s) e 2,00 K ops/s no painel do Grafana. O limite de 20 mil/s contra o motor **não foi medido**.
- **Motor sob carga:** acompanhou o backfill a 2 mil/s mais ~250 comandos/s ao vivo, atraso de 0 a ~450 mensagens, sem acumular.
- **H3-B:** `kafka-streams-application-reset` do motor, volume de estado apagado e reinício; o tópico de histórico tinha **duas cópias** do backfill (duas rodadas). Estado reconstruído sem OOM (pico 1.022 MiB com limite de 1 GiB). Conferência de 100 contas novas (101 a 200) contra o SQL, com 72.002 comandos ao vivo do tópico na referência: **0 diferenças**; a deduplicação por `transactionId` segurou as cópias. O passo 3 (conector novo com slot novo) foi exercitado numa rodada anterior.
- **p99 do motor:** ~2,2 s com backfill + ao vivo e ~2,2 s só com tráfego ao vivo (p50 0,52 s). O backfill não piorou a latência neste ambiente; o valor absoluto é do ambiente (Docker de 4 GB, motor com 1 GiB, log em DEBUG) e **não vale como medida de SLO**. A causa do valor alto não foi isolada.
- **Memória:** soma dos picos amostrados a cada 15 s: motor 1.007 MiB, Kafka 776 MiB, transaction-service 233 MiB, Prometheus 88 MiB = ~2,1 GB (picos não simultâneos).
- **Grafana:** painéis 31 e 32 (versão 8 do JSON) renderizados com dados reais: dois picos, `published` 2,00 K ops/s, `out_of_window` 667 ops/s (3:1, igual à base).
- **Defeitos achados no caminho:** (1) o script de ensaio tinha um `)` faltando no produtor ao vivo e escondia o erro: os primeiros ensaios não mandaram tráfego ao vivo; (2) o reset do `transaction-service` falhava em silêncio com o grupo ainda ocupado (agora repete e aborta se falhar); (3) um `transaction-service` morto no meio de uma transação deixou o aborto inacabado e travou uma partição do histórico; o último offset estável parou e o motor não consumia essa partição. Resolvido com `kafka-transactions.sh abort` e perdeu as mensagens daquela transação. Em produção, uma carga com erro deve seguir o H3.
- **Não verificado:** limite de 20 mil/s sob carga; p99 do motor em ambiente representativo; retardatário na janela de silêncio; backfill em escala de nuvem (8.000/25.000 TPS fora do escopo local).
