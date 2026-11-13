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
| Ponta a ponta com o motor | SC-001: `avgAmount30d` de contas de referência = SQL |
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
