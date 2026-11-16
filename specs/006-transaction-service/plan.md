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

## 8. Decisões da implementação (Fases 1 e 2)
- **Prova do SC-001 (decisão do usuário):** o motor não expõe features fora das regras, e não vamos acrescentar endpoint de diagnóstico a um serviço fechado. A prova é um teste **em processo no `detection-engine`**: a topologia real recebe os registros que o `transaction-service` produz para 100 contas de referência e `avgAmount30d` é comparado com o SQL; no compose só se prova o transporte (tópico). Mesmo assim, "diferença 0" exige definir o SQL com a mesma janela alinhada a buckets do motor (o motor usa a média de referência anterior à transação, ver 004), o que será decidido e documentado na Fase 4.
- **Seed separado do `init.sql`:** `infra/seed/transactions.sh` (e não o `init.sql`), porque o `init.sql` só roda na criação do volume do banco legado, que já existe com os clientes da 003. É determinístico (mesmo `N` e `ANCHOR` dão as mesmas linhas), idempotente e deve rodar **antes** de registrar o conector (o snapshot lê o que já estiver na tabela). 1 milhão de linhas em 120 dias leva ~35 s; com `ANCHOR=2026-11-15T00:00:00Z` ficam 750.082 dentro dos 90 dias e 249.918 fora. O primeiro teste com 20 mil linhas passou e escondeu um estouro de inteiro (`n * 104729` acima de ~20 mil linhas); apareceu ao rodar o tamanho real e foi corrigido com `bigint`.
- **Conector `transactions-cdc`:** slot e publicação próprios (`transactions_cdc_slot`, `dbz_transactions`), `snapshot.mode=initial`. As colunas `payer_name` e `payer_cpf` continuam no CDC de propósito (como em 003); é o mapeador que as descarta, com teste.
- **`CUTOVER_AT` sem valor padrão no serviço:** o compose repassa `${CUTOVER_AT:-}` e o serviço deve **recusar a partida** se vier vazio ou inválido (falha rápida, Fase 3); o `.env.example` traz um valor de exemplo. Um corte errado dobraria ou perderia transações, então não se adivinha.
- **Fixtures reais (Fase 2):** capturadas do Debezium 3.6.3 com uma tabela temporária no banco legado (cópia de `bank.transactions`), um conector descartável e o tópico lido do começo; tudo removido depois (conector, slot, publicação, tabela, tópico). Dois fatos que só a captura mostrou: o **primeiro** evento do snapshot vem com `source.snapshot="first"` (não `"true"`) e o **último** com `"last"`; a remoção (`d`) traz `before` com a chave e valores zerados (`account_token` vazio, `amount_cents` 0, `occurred_at` de 1970), por isso nada é lido dele.
- **Fim do snapshot = `source.snapshot="last"`**, marcado por `TransactionChange.lastOfSnapshot`, que vale mesmo quando esse último registro cai fora da janela (a topologia, na Fase 3, deve contar o fim antes de filtrar).
- **Janela:** início **incluído** (exatamente 90 dias entra), `cutoverAt` **excluído**. O backfill só aceita `op=r`; `c`/`u`/`d` voltam `SkipReason.NOT_SNAPSHOT` (rótulo da métrica `reason`), fora da janela voltam `OUT_OF_WINDOW`.
- **Onde ficou cada peça:** `domain/{HistoryWindow,HistoryRecord}`; `application/usecase/{BackfillTransactionHistoryUseCase,TransactionChange,SkipReason}`; `adapters/controller/cdc/{DebeziumTransactionMapper,CdcFormatException}`. `HistoryRecord` não tem campo de pagador: o dado pessoal nem existe no domínio. `device_hash` nulo é aceito.
- **`archunit.properties` temporário removido:** as camadas já têm classes e a regra de arquitetura passa sem a exceção.
