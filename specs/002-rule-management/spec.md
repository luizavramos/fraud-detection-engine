# Spec 002 — Gestão de regras (+ módulo `rules-kernel`)

**Status:** pronta para implementação · **Prioridade:** P1 · **Depende de:** 000
**Passos no desenho:** 3.2, 3.3, 6

## Contexto
O case exige regras configuráveis, com thresholds dinâmicos e regras compostas, **sem redeploy**. Este serviço é onde o time antifraude cria, versiona, ativa, desliga e reverte regras. O motor (004) busca o conjunto ativo a cada 30 s (ADR-13).

Esta spec também cria o módulo **`rules-kernel`**, a linguagem de regras compartilhada: modelo, catálogo de features e compilador CEL. Ele é usado aqui (validação) e no motor (avaliação), para que os dois nunca discordem sobre o que é uma regra válida.

**Regra de ouro desta spec:** criar regra, mudar threshold, mudar peso, compor regras, ligar modo sombra, desligar e reverter são **operações de dados pela API**. Nenhuma delas exige build, deploy ou restart de qualquer serviço. Só um **dado novo** no catálogo de features (ex.: uma agregação que ainda não existe) exige deploy do motor. Esse limite é intencional (segurança e latência previsível) e deve ser dito na apresentação.

## Histórias de usuário

### H1 — Criar e validar uma regra (P1)
Como analista antifraude, quero escrever uma regra e saber na hora se ela é válida, para não publicar algo quebrado.

**Teste independente:** `POST /v1/rules` com uma expressão inválida devolve `422` explicando o erro; com uma válida, cria a versão 1 em `DRAFT`.

**Cenários de aceite**
1. **Given** a expressão `features.count5m > params.maxCount`, com `maxCount` em `params`, **When** crio a regra, **Then** recebo `201` e a versão 1 com status `DRAFT`.
2. **Given** a expressão `features.contagem5m > 3` (feature inexistente), **When** crio, **Then** recebo `422` com a mensagem "feature desconhecida: contagem5m".
3. **Given** a expressão `tx.amount > "mil"` (tipo errado), **When** crio, **Then** recebo `422` com erro de tipo.
4. **Given** uma regra com casos de teste (`context` + `expectedHit`), **When** valido em `POST /v1/rules:validate`, **Then** recebo o resultado de cada caso.

### H2 — Ativar, desligar e reverter sem redeploy (P1)
Como analista, quero ativar uma regra e, se ela gerar problema, desligá-la ou voltar ao conjunto anterior em segundos.

**Cenários de aceite**
1. **Given** a regra R em `DRAFT`, **When** faço `POST .../versions/1:activate`, **Then** um novo conjunto (versão N+1) é publicado contendo R `ACTIVE`, e `GET /v1/rule-sets/active` passa a devolver N+1.
2. **Given** R ativa, **When** faço `:disable`, **Then** um conjunto N+2 é publicado sem R.
3. **Given** o conjunto ativo N+2, **When** faço `POST /v1/rule-sets/{N}:rollback`, **Then** é publicado N+3 com o mesmo conteúdo de N. O histórico não é apagado.
4. **Given** qualquer mudança acima, **When** consulto `audit_log`, **Then** há um registro com quem, quando, o antes e o depois.

### H3 — Servir o conjunto ativo de forma barata (P1)
Como motor de detecção, quero perguntar "mudou?" e receber uma resposta barata quando nada mudou, para poder perguntar com frequência.

**Cenários de aceite**
1. **Given** o conjunto ativo é a versão 42, **When** faço `GET /v1/rule-sets/active`, **Then** recebo `200`, o JSON do conjunto (schema `rule-set.v1`) e o header `ETag: "42"`.
2. **Given** envio `If-None-Match: "42"`, **When** nada mudou, **Then** recebo `304` sem corpo.

### H4 — Ajustar thresholds sem mexer na lógica (P1)
Como analista, quero mudar um número (ex.: de 5 para 3 transações em 5 min) sem reescrever a expressão.

**Cenários de aceite**
1. **Given** R com `params.maxCount=5`, **When** crio a versão 2 só com `maxCount=3` e ativo, **Then** o conjunto publicado contém R v2 com o novo parâmetro.
2. **Given** `paramOverrides` para o segmento PJ, **When** o conjunto é publicado, **Then** o override vai junto (o motor aplica conforme o segmento).
3. **Given** `PUT /v1/scoring {"mediumThreshold":40,"highThreshold":70}`, **When** salvo, **Then** um novo conjunto é publicado com as faixas novas.

### H5 — Testar uma regra em modo sombra (P2)
Como analista, quero ligar uma regra só para observar, sem gerar alertas, para medir o impacto antes de ativá-la.

**Cenários de aceite**
1. **Given** R em `DRAFT`, **When** faço `:shadow`, **Then** R entra no conjunto com status `SHADOW`. O motor avalia e registra, mas R não soma pontos.

### H6 — Aprovação por outra pessoa (P3)
Como coordenador, quero que só alguém diferente do autor possa ativar uma regra.

**Cenários de aceite**
1. **Given** R criada por `analista.a`, **When** `analista.a` tenta ativar, **Then** recebe `403`. **When** `coord.b` ativa, **Then** funciona e `approved_by=coord.b`.

## Casos de borda
- **Duas ativações simultâneas:** a versão do conjunto é gerada por sequência no banco, com transação serializável na publicação. Nenhum conjunto se perde.
- **Expressão cara demais:** o custo estimado pelo CEL acima do limite gera `422`.
- **Rollback para versão inexistente:** `404`.

## Requisitos funcionais
- **FR-001:** o `rules-kernel` DEVE definir o catálogo de features (nome e tipo) que as expressões podem usar: `tx.*`, `profile.*`, `features.*`, `lists.*`, `params.*`.
- **FR-002:** o `rules-kernel` DEVE compilar expressões CEL com checagem de tipos contra o catálogo e rejeitar as que excedem o custo máximo.
- **FR-003:** o `rules-kernel` DEVE avaliar um conjunto compilado contra um contexto e devolver as regras disparadas (ACTIVE e SHADOW separadas), com o motivo preenchido pelo `reasonTemplate`.
- **FR-004:** o serviço DEVE versionar regras de forma imutável: mudar o conteúdo cria uma nova versão.
- **FR-005:** cada ativação, desligamento, modo sombra, rollback ou mudança de scoring DEVE publicar um **novo** conjunto com versão crescente.
- **FR-006:** DEVE expor `GET /v1/rule-sets/active` com `ETag` e suporte a `If-None-Match` (`304`).
- **FR-007:** DEVE registrar toda mudança em `audit_log` (append-only).
- **FR-008:** DEVE carregar via migração as **8 regras iniciais** e o scoring 40/70 (plan §5).
- **FR-009:** acesso local por usuários em memória com papéis `RULE_AUTHOR` e `RULE_APPROVER`. OIDC via ALB é `[DOC]`.

## Entidades
`Rule`, `RuleVersion` (imutável), `RuleSet` (versão publicada + itens + scoring), `AuditEntry`, `FeatureCatalog`, `CompiledRuleSet`.

## Critérios de sucesso
- **SC-001:** criar, validar e ativar uma regra em menos de 1 minuto pela API (demo ao vivo), **sem deploy nem restart** de nenhum serviço.
- **SC-002:** `GET /v1/rule-sets/active` com `304` responde em p99 ≤ 10 ms.
- **SC-003:** 100% das expressões das 8 regras iniciais compilam, e há casos de teste que disparam e que não disparam cada uma.

## Premissas e decisões em aberto
- [NEEDS CLARIFICATION] **Escopo:** *padrão adotado: P1 (versões, validação, ativar, desligar, rollback, scoring) + P2 (sombra). P3 (aprovação) implementar se sobrar tempo; senão fica `[DOC]`.*
- [NEEDS CLARIFICATION] **Regras iniciais e pesos:** *padrão adotado: as 8 regras do plan §5, com faixas 40 (MÉDIA) e 70 (ALTA).*
- [NEEDS CLARIFICATION] **Tipos de transação:** só Pix ou também cartão? *Padrão adotado: modelo genérico com `type`; regras iniciais pensadas para Pix.*
