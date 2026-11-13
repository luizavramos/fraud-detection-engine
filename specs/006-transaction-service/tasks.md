# Tarefas 006 — Transaction service

Base de caminhos: `transaction-service/src/{main,test}/java/br/com/motorfraude/transaction/`

## Fase 1 — Setup
- [ ] T601 Criar o módulo `transaction-service` (depende de `contracts`, `platform-commons` e `platform-streams`; usa `StreamsAppBootstrap` do 003/T301) + teste ArchUnit
- [ ] T602 [P] Conector Debezium para `transactions` (`snapshot.mode=initial`, publicação `dbz_transactions`, slot próprio — 000 plan §5.1) em `register-connectors.sh` + serviço no compose (porta 8086, `CUTOVER_AT`)
- [ ] T603 [P] Gerar ~1 milhão de transações sintéticas em 120 dias no `infra/bank-db/init.sql` (ou script de seed separado)

## Fase 2 — Domínio e mapeamento
- [ ] T604 **Testes** de `HistoryWindow` (bordas) — `domain/HistoryWindowTest.java`
- [ ] T605 Implementar `HistoryWindow` e `HistoryRecord`
- [ ] T606 **Testes** do `DebeziumTransactionMapper` (campos permitidos; ignora `op` ≠ `r`), com fixtures reais
- [ ] T607 Implementar o mapper e o `BackfillTransactionHistoryUseCase`

## Fase 3 — Topologia e status
- [ ] T608 **Testes** `TopologyTestDriver`: janela, chave, `COMPLETED` no evento `snapshot=last`; todo registro emitido passa em `ContractAssertions.assertProduces("transaction-history-record.v1", …)`
- [ ] T609 Implementar `TopologyFactory`, `HistoryRecordSerde`, limitador de taxa e `BackfillStatus` + `GET /v1/backfill/status`

## Fase 4 — Prova
- [ ] T610 **Teste de integração** (Testcontainers + Debezium): 10 mil transações → registros corretos e sem dado pessoal (SC-002)
- [ ] T611 **Teste ponta a ponta com o motor** (compose): backfill → `avgAmount30d` de 100 contas de referência igual ao SQL (SC-001) — `tools/backfill-check/`
- [ ] T612 Documentar o procedimento de refazer o backfill (H3) e o `cutoverAt` — `transaction-service/README.md`
- [ ] T613 Métricas de progresso (`backfill_records_published_total`, `backfill_records_skipped_total{reason}`) + painel no Grafana

## Dependências
Fase 1 → Fase 2 → Fase 3 → Fase 4. O T611 precisa do 004 com o `BackfillProcessor` (T418).
