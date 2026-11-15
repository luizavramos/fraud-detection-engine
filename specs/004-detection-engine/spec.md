# Spec 004 — Motor de detecção

**Status:** implementada e verificada localmente (a 300 TPS); os 8.000 e 25.000 TPS não foram verificados e não serão (sem medição na nuvem) · **Prioridade:** P1 (núcleo) · **Depende de:** 000; usa o `rules-kernel` (002, Fase 2) e os tópicos de 003 e 006
**Passos no desenho:** 3, 3.1, 3.2

## Contexto
É o componente que atende o centro do case:
- avaliar cada transação contra regras configuráveis;
- decidir se ela é suspeita;
- publicar a decisão e, quando for o caso, o alerta.

Tudo isso em até **500 ms (p99)** depois da chegada no receiver, a 8k–25k TPS, **sem duplicar e sem perder** nada, mesmo com falhas.

## Histórias de usuário

### H1 — Avaliar a transação e decidir (P1)
Como time antifraude, quero que cada transação seja avaliada e, se suspeita, vire um alerta com o motivo, para agir rápido e entender por quê.

**Teste independente:** publicar em `transaction-commands.v1` uma transação que dispara R10 (beneficiário em lista) faz aparecer um `FraudAlert` HIGH em `fraud-alerts.v1` e um `FraudDecision` em `fraud-decisions.v1`.

**Cenários de aceite**
1. **Given** uma transação que não dispara nenhuma regra, **When** é avaliada, **Then** sai um `FraudDecision` com `decision=NO_ALERT`, e nada sai em `fraud-alerts.v1`.
2. **Given** uma transação cujas regras somam 55 pontos, **When** é avaliada, **Then** sai um alerta `MEDIUM` com `triggeredRules` (id, versão, peso, motivo) e `ruleSetVersion`.
3. **Given** uma transação que soma ≥ 70, **When** é avaliada, **Then** sai um alerta `HIGH`.
4. **Given** o mesmo `transactionId`, **When** o alerta é gerado, **Then** `alertId = UUIDv5(namespace, transactionId)`, sempre o mesmo.

### H2 — Uma decisão por transação, mesmo com reenvios (P1)
Como cliente, quero receber no máximo um alerta por transação, mesmo se ela chegar duas vezes.

**Cenários de aceite**
1. **Given** a transação `T1` já processada, **When** `T1` chega de novo (ex.: Redis do receiver estava fora), **Then** é descartada sem nova decisão, e `engine_duplicates_dropped_total` aumenta.
2. **Given** `T1` processada há mais de 24 h, **When** chega de novo, **Then** é tratada como nova (limite da janela de dedup, documentado).

### H3 — Calcular o histórico da conta no fluxo (P1)
Como autor de regras, quero contadores confiáveis por conta (minutos, horas, dias), beneficiários e dispositivos recentes, para escrever regras de velocidade e de desvio de comportamento.

**Cenários de aceite**
1. **Given** 6 transações da mesma conta em 4 minutos, **When** a 6ª é avaliada, **Then** `features.count5m = 6` (a transação atual entra na conta).
2. **Given** uma conta com 30 dias de histórico com média de R$ 200, **When** chega uma transação de R$ 2.000, **Then** `features.avgAmount30d = 20000` e R04 dispara.
3. **Given** um beneficiário nunca usado pela conta, **When** a transação é avaliada, **Then** `features.isNewBeneficiary = true`; na próxima para o mesmo beneficiário, `false`.
4. **Given** transações chegando fora de ordem por até 2 min, **When** são avaliadas, **Then** cada uma entra no bucket do seu `occurredAt`.

### H4 — Usar o perfil do cliente sem chamar ninguém (P1)
Como responsável pela resiliência, quero que o motor use uma cópia local do perfil, para continuar funcionando se a base de clientes cair.

**Cenários de aceite**
1. **Given** o perfil da conta está em `customer-profile.v1`, **When** a transação é avaliada, **Then** `profile.*` vem da cópia local, sem nenhuma chamada de rede.
2. **Given** não há perfil para a conta, **When** é avaliada, **Then** as regras que dependem de perfil não disparam, e a decisão sai com `contextCompleteness=PARTIAL`.

### H5 — Regras novas sem redeploy (P1)
Como analista, quero que uma regra ativada na gestão de regras passe a valer no motor sem reiniciar nada.

**Cenários de aceite**
1. **Given** o motor com o conjunto 41, **When** a gestão publica o 42 (evento no `rule-sets.v1`, via outbox e Debezium), **Then** em poucos segundos (meta ≤ 35 s de ponta a ponta, da ativação na API à decisão) as decisões passam a registrar `ruleSetVersion=42`.
2. **Given** o conjunto 42 chega com uma regra que não compila, **When** o motor tenta carregar, **Then** mantém o 41, registra o erro e incrementa `engine_ruleset_load_failures_total`.
3. **Given** a gestão de regras está fora e o motor reinicia, **When** sobe, **Then** lê o último evento do tópico compactado `rule-sets.v1` (que já é o snapshot; não há cópia em disco) e fica pronto.
4. **Given** o tópico `rule-sets.v1` está vazio (ambiente novo), **When** o motor sobe, **Then** a readiness falha e ele não processa transações.

### H6 — Não duplicar nem perder sob falha (P1)
Como responsável pelo case, quero provar que derrubar o motor no meio da carga não gera alerta duplicado nem transação sem decisão.

**Cenários de aceite**
1. **Given** carga de 8k TPS, **When** mato uma instância do motor no meio da carga, **Then** o consumidor-auditor encontra exatamente uma decisão por `transactionId` enviado e aceito.

### H7 — Lista de bloqueio (P2)
Como antifraude legado, quero publicar contas e dispositivos marcados como fraude, para que o motor os use na hora.

**Cenários de aceite**
1. **Given** `acc_tok_9` publicado em `blocklist.v1`, **When** chega uma transação para `acc_tok_9`, **Then** `lists.counterpartyFlagged = true` e R10 dispara.
2. **Given** um tombstone para `acc_tok_9`, **When** chega a próxima transação, **Then** `lists.counterpartyFlagged = false`.

### H8 — Carga inicial do histórico (P2)
Como responsável pelo go-live, quero popular o histórico de 90 dias antes de ligar as regras de longo prazo.

**Cenários de aceite**
1. **Given** registros em `transaction-history-backfill.v1`, **When** o motor os consome, **Then** os buckets diários são populados, e **nenhuma** regra é avaliada, nenhuma decisão é publicada.

### H9 — Mensagem ruim não trava a fila (P2)
**Cenários de aceite**
1. **Given** uma mensagem que não desserializa, **When** chega, **Then** vai para `transaction-commands.dlq` com o motivo, e a partição continua.

## Casos de borda
- **Conta PJ com volume muito alto:** os conjuntos são limitados (50 beneficiários, 10 dispositivos). Ao exceder, `distinctBeneficiaries24h` satura no limite e é marcado.
- **Transação com `occurredAt` mais de 2 min atrasado em relação ao relógio da partição:** entra no bucket do horário de chegada, e a métrica `engine_late_events_total` aumenta.
- **Regra lança erro na avaliação:** conta como não disparada (o kernel trata).
- **Rebalance:** decisões podem atrasar alguns segundos (p99.9). Aceito e monitorado.

## Requisitos funcionais
- **FR-001:** DEVE consumir `transaction-commands.v1` com `exactly_once_v2` e `commit.interval.ms=100`.
- **FR-002:** DEVE deduplicar por `transactionId` num state store com retenção de 24 h, dentro da transação.
- **FR-003:** DEVE manter por conta: buckets de 1 min ×60, 1 h ×24 e 1 dia ×90 (`count`, `sum`, `max`); últimos 50 beneficiários; últimos 10 dispositivos.
- **FR-004:** DEVE materializar `customer-profile.v1` como tabela local co-particionada, e `blocklist.v1` como tabela global.
- **FR-005:** DEVE consumir o tópico `rule-sets.v1` (compactado, chave fixa; cada evento é o conjunto `rule-set.v1` completo), compilar com o `rules-kernel`, trocar de forma atômica, ignorar versão menor ou igual à atual e manter o conjunto anterior se o evento não compilar.
- **FR-006:** DEVE publicar `FraudDecision` para toda transação nova e `FraudAlert` quando a severidade for MEDIUM ou HIGH.
- **FR-007:** DEVE preencher `eventReceivedAt` a partir do header `receivedAt` e medir a latência fim a fim.
- **FR-008:** DEVE consumir `transaction-history-backfill.v1` só para atualizar buckets (H8).
- **FR-009:** DEVE enviar mensagens que não desserializam para `transaction-commands.dlq`.
- **FR-010:** NÃO DEVE fazer chamadas de rede síncronas no processamento de uma transação.

## Entidades
`TransactionCommand`, `AccountFeatures` (buckets + conjuntos), `FeatureSnapshot`, `CustomerProfile`, `Decision`, `Alert`, `Severity`, `ContextCompleteness`.

## Critérios de sucesso
- **SC-001:** latência fim a fim (chegada no receiver → alerta publicado) **p99 ≤ 500 ms** a 8.000 TPS; meta interna ~225 ms. **Não verificado a 8.000 TPS** (sem medição na nuvem); medido a 300 TPS com o motor aquecido: p99 230 ms (`docs/resultados-carga.md`).
- **SC-002:** 0 decisões duplicadas e 0 transações sem decisão no teste de caos (H6).
- **SC-003:** uma regra ativada vale no motor em ≤ 35 s de ponta a ponta (da ativação na API até a decisão, passando pelo outbox, pelo Debezium e pelo consumo). Medido no compose: 13 a 20 s.
- **SC-004:** estado por conta ≤ 5 KB (medido pelo serde).

## Premissas
- A janela de dedup de 24 h e os limites dos conjuntos (50/10) são configuráveis.
- Fuso para `tx.hourOfDay`: America/Sao_Paulo.
