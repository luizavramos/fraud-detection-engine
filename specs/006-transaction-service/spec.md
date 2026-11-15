# Spec 006 — Transaction service

**Status:** em implementação (Fase 1 de 4: módulo, conector, compose e seed) · **Prioridade:** P2 · **Depende de:** 000; alimenta o `BackfillProcessor` do 004
**Passos no desenho:** 2, 2.2

## Contexto
O motor calcula o histórico de cada conta a partir do próprio fluxo (ADR-12). No go-live, esse histórico começa **vazio**: sem os 90 dias anteriores, regras como "valor 5× acima da média de 30 dias" (R04) ficariam cegas.

Este serviço faz a **carga inicial (backfill)**:
1. lê as transações dos últimos 90 dias da base de transações do banco, via snapshot do Debezium;
2. publica no tópico que o motor usa para popular os buckets, sem avaliar regras.

Depois da carga, ele sai do caminho. Nada na detecção depende dele.

## Histórias de usuário

### H1 — Carregar 90 dias de histórico no go-live (P2)
Como responsável pelo go-live, quero popular o histórico das contas antes de ligar as regras de longo prazo, para que elas funcionem desde o primeiro dia.

**Teste independente:** com 90 dias de transações sintéticas no banco legado, rodar o backfill faz o motor ter `features.avgAmount30d` correto para uma conta de referência.

**Cenários de aceite**
1. **Given** transações de 120 dias no banco, **When** o backfill roda, **Then** só as dos últimos 90 dias (em relação ao instante de corte) são publicadas em `transaction-history-backfill.v1`, com chave `accountId`.
2. **Given** o registro publicado, **When** é validado, **Then** segue `transaction-history-record.v1`, sem nenhum dado pessoal.
3. **Given** o backfill terminou, **When** consulto `/v1/backfill/status`, **Then** vejo `COMPLETED`, o total publicado e o instante de corte.

### H2 — Sem sobreposição com o fluxo ao vivo (P2)
Como dono do motor, quero que nenhuma transação seja contada duas vezes: uma pelo backfill e outra pelo receiver.

**Cenários de aceite**
1. **Given** o instante de corte `cutoverAt`, **When** o backfill roda, **Then** só publica transações com `occurredAt < cutoverAt`. O receiver passa a enviar as de `occurredAt ≥ cutoverAt`.
2. **Given** um registro de backfill publicado duas vezes (reinício), **When** o motor consome, **Then** conta uma vez só (o motor marca o `transactionId` no `seen-tx`; ver 004 T418).

### H3 — Refazer a carga (P3)
Como SRE, quero refazer o backfill depois de um erro, de forma controlada.

**Cenários de aceite**
1. **Given** o procedimento documentado (resetar a aplicação do motor e disparar o snapshot de novo), **When** é seguido, **Then** os buckets são reconstruídos sem dupla contagem.

## Casos de borda
- **Volume grande (milhões de linhas):** a publicação é em lote, com controle de taxa, para não competir com o tráfego ao vivo (padrão: 20 mil registros/s).
- **Transação de conta que não existe mais:** é publicada mesmo assim; o motor cria o agregado.

## Requisitos funcionais
- **FR-001:** DEVE ler `cdc.bank.transactions` (snapshot do Debezium, `op=r`) e ignorar eventos que não sejam snapshot quando estiver em modo backfill.
- **FR-002:** DEVE filtrar `occurredAt ∈ [cutoverAt − 90 dias, cutoverAt)`.
- **FR-003:** DEVE publicar `TransactionHistoryRecord` com chave `accountId`, com produtor idempotente e transacional.
- **FR-004:** DEVE expor `GET /v1/backfill/status` (estado, contadores, `cutoverAt`) e as métricas de progresso.
- **FR-005:** NÃO DEVE propagar colunas de dado pessoal (lista de campos permitidos, igual ao 003).
- **[DOC] FR-006:** trazer, no dia a dia, transações de canais que não passam pelo receiver.

## Critérios de sucesso
- **SC-001:** depois do backfill, a `avgAmount30d` de 100 contas de referência no motor bate com o cálculo direto em SQL no banco legado (diferença 0).
- **SC-002:** 0 registros fora da janela ou com dado pessoal.

## Premissas
- `cutoverAt` é configurado (variável de ambiente) e combinado com o início do tráfego no receiver.
- Localmente, o banco legado tem ~1 milhão de transações sintéticas (gerado em 000/T007).
