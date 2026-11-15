# Spec 003 — Customer service

**Status:** implementada (testes e `verify` verdes; ver `docs/uso-de-ia.md`) · **Prioridade:** P1 · **Depende de:** 000
**Passos no desenho:** 2, 2.1

## Contexto
O motor precisa de dados do cliente (segmento, idade da conta, troca recente de senha, dispositivos conhecidos), mas **não pode chamar a base de clientes** a cada transação (princípio IV). Este serviço:
1. lê as mudanças da base de clientes do banco por CDC (Debezium);
2. pré-calcula o perfil para antifraude;
3. publica o perfil num tópico compactado, que o motor mantém como cópia local.

Ele também é a fronteira de LGPD: a base do banco tem nome e CPF, e o perfil publicado **não**.

## Histórias de usuário

### H1 — Perfil atualizado quando o cliente muda (P1)
Como motor de detecção, quero receber o perfil atualizado de uma conta pouco depois de ela mudar no banco, para avaliar com dados recentes.

**Teste independente:** `UPDATE customers SET last_credential_change_at = now()` no banco legado faz aparecer em `customer-profile.v1` um perfil com `lastCredentialChangeAt` novo em até 5 s.

**Cenários de aceite**
1. **Given** a conta `acc_tok_1` no banco legado, **When** o cadastro dela muda, **Then** um `CustomerProfile` com a chave `acc_tok_1` é publicado em ≤ 5 s.
2. **Given** a conta é removida no banco, **When** o CDC entrega a remoção, **Then** é publicado um *tombstone* (valor nulo) para `acc_tok_1`.

### H2 — Dispositivos conhecidos sem crescer sem limite (P1)
Como motor, quero a lista de dispositivos conhecidos da conta, limitada, para calcular "dispositivo novo" sem estado ilimitado.

**Cenários de aceite**
1. **Given** uma conta com 15 dispositivos, **When** o perfil é publicado, **Then** `knownDeviceHashes` tem os **10 mais recentes** por `first_seen_at`.
2. **Given** um dispositivo novo cadastrado no banco, **When** o CDC entrega, **Then** o perfil é republicado incluindo-o.

### H3 — Nenhum dado pessoal sai daqui (P1)
Como DPO, quero garantir que nome, CPF, e-mail e telefone não saiam da base do banco para os tópicos.

**Cenários de aceite**
1. **Given** a tabela `customers` tem `full_name`, `cpf`, `email` e `phone`, **When** o perfil é publicado, **Then** ele contém apenas tokens e atributos derivados, e valida contra `customer-profile.v1` (que proíbe campos extras).
2. **Given** um log de erro ao processar um evento CDC, **When** é escrito, **Then** não contém CPF nem e-mail (mascaramento + teste).

### H4 — Reconstruir os perfis do zero (P2)
Como SRE, quero reconstruir todos os perfis a partir do banco, para recuperar de um erro de lógica.

**Cenários de aceite**
1. **Given** o tópico de perfis apagado, **When** reinicio o conector Debezium com snapshot, **Then** todos os perfis são republicados.

## Casos de borda
- **Evento de dispositivo chega antes do evento do cliente:** o perfil só é publicado quando existe o cliente (join interno). O dispositivo fica guardado até o cliente chegar.
- **`last_credential_change_at` nulo:** o perfil vai com o campo ausente, e o motor usa `hoursSinceCredentialChange = -1`.
- **Mesmo evento CDC entregue duas vezes:** a publicação é idempotente (mesmo perfil, mesma chave; o tópico é compactado).

## Requisitos funcionais
- **FR-001:** DEVE consumir `cdc.bank.customers` e `cdc.bank.customer_devices` (formato Debezium com `after`/`before`/`op`).
- **FR-002:** DEVE calcular `CustomerProfile{accountId, segment, accountOpenedAt, lastCredentialChangeAt, knownDeviceHashes[≤10], updatedAt}`, com `updatedAt` = o `ts_ms` mais recente dos eventos do CDC que formaram o perfil (determinístico; não usa o relógio do serviço).
- **FR-003:** DEVE publicar em `customer-profile.v1` com chave `accountId`, e *tombstone* na remoção.
- **FR-004:** NÃO DEVE propagar nenhuma coluna de dado pessoal; a projeção usa lista explícita de campos permitidos.
- **FR-005:** DEVE expor health/readiness e métricas (eventos CDC por tipo, perfis publicados, atraso entre `ts_ms` do CDC e a publicação).

## Critérios de sucesso
- **SC-001:** atraso de mudança no banco → perfil publicado com p99 ≤ 5 s.
- **SC-002:** 0 campos de dado pessoal nos perfis, num teste com todos os 10 mil clientes sintéticos.

## Premissas
- Os tokens (`acc_tok_…`) já existem na base do banco. A tokenização acontece fora deste sistema.
- Localmente, o CDC vem do Debezium sobre o Postgres "banco legado" (000). Se o Debezium falhar, o caminho alternativo é o script de CDC simulado (000, risco).
