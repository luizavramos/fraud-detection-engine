# Tarefas 002 — Gestão de regras + `rules-kernel`

Caminhos: `rules-kernel/src/...` e `rule-management/src/{main,test}/java/br/com/motorfraude/rules/`

## Fase 1 — Setup e spike
- [x] T201 Spike com cel-java: compilar `features.count5m > params.maxCount` com declarações tipadas e avaliar. Resultado em `rules-kernel/SPIKE.md`: passou; sem estimador de custo no Java, o limite é de nós e profundidade
- [x] T202 [P] Criar os módulos `rules-kernel` (Java puro) e `rule-management` (Spring Boot), com testes ArchUnit — ambos em repositórios próprios
- [x] T203 [P] Adicionar `rule-management` ao compose (porta 8082, depende de postgres) e ao Prometheus

## Fase 2 — `rules-kernel` (bloqueia 004)
- [x] T204 **Testes** de compilação: feature desconhecida, tipo errado, nós ou profundidade acima do limite, expressão válida — `rules-kernel/src/test/.../RuleCompilerTest.java`
- [x] T205 Implementar `FeatureCatalog` (declarações CEL do plan §3.1), a função `max(int,int)` e o `RuleCompiler`
- [x] T206 **Testes** de avaliação: dispara e não dispara, override PJ, SHADOW não soma, erro de regra conta como não disparada, severidade pelas faixas — `CompiledRuleSetTest.java`
- [x] T207 Implementar `CompiledRuleSet.evaluate` e `EvaluationContext` (montado a partir de mapas tipados)
- [x] T208 **Testes** das 8 regras iniciais (plan §5): um caso que dispara e um que não dispara para cada — `InitialRulesTest.java`, com as expressões em `src/test/resources/initial-rules.json`

**Checkpoint:** o kernel está pronto e o motor (004) já pode usá-lo com conjuntos de teste em memória.

## Fase 3 — Domínio e casos de uso (H1, H2, H4)
- [x] T209 **Testes** de transição de status (DRAFT→ACTIVE, DRAFT→SHADOW, ACTIVE→DISABLED, proibidas) — `domain/RuleVersionTest.java`
- [x] T210 Implementar `Rule`, `RuleVersion`, `RuleSet`, `AuditEntry`
- [x] T211 Portas `RuleRepository`, `RuleSetRepository`, `AuditLog` e `RuleValidator`, com fakes em memória
- [x] T212 **Testes** dos casos de uso: criar, nova versão, ativar (conjunto N+1), desligar, rollback (N+k igual a N), scoring, falha de compilação não publica nada — `application/*Test.java`
- [x] T213 Implementar os casos de uso e o `RuleSetPublisher` interno (plan §4.2)

## Fase 4 — Persistência
- [x] T214 Flyway `V1__schema.sql` (plan §4.3) e `V2__seed_rules.sql` (8 regras + scoring 40/70 + conjunto versão 1)
- [x] T215 **Teste de integração** (Testcontainers Postgres): repositórios, publicação concorrente (10 threads ativando ao mesmo tempo → versões sem buraco nem colisão), `audit_log`
- [x] T216 Implementar os repositórios JDBC

## Fase 5 — API (H1–H4)
- [x] T217 **Testes** REST: 201/422 na criação, `:validate` com casos de teste, `:activate`/`:disable`/`:rollback`, `PUT /scoring`, 404, papéis — `adapters/controller/*Test.java`
- [x] T218 Implementar os controllers + Problem Details + Spring Security (usuários em memória `analista`, `coordenador`)
- [x] T219 **Teste** de `GET /v1/rule-sets/active`: 200 com `ETag`; 304 com `If-None-Match`; o corpo valida contra `rule-set.v1`
- [x] T220 Implementar o `RuleSetController` com cache por versão

**Checkpoint:** demo de criar, validar e ativar uma regra pela API (SC-001).

## Fase 6 — H5 sombra e H6 aprovação
- [x] T221 **Teste** + implementação do `:shadow` (entra no conjunto com status SHADOW)
- [x] T222 (P3, opcional) **Teste** + implementação do maker-checker (`403` quando aprovador = autor; CHECK no banco)

## Fase 7 — Observabilidade e documentação
- [x] T223 Métricas `rules_published_total`, `rules_compilation_failures_total` e `rule_set_active_request_seconds` + painel no Grafana
- [x] T224 Coleção de chamadas da demo (`docs/demo-regras.http`): criar regra, validar, ativar em sombra, ativar, desligar, rollback
- [x] T225 `Idempotency-Key` nos POST e PUT da API (plano §4.4): resposta guardada por usuário e chave, repetida em reenvios, 422 se a chave for reaproveitada com outra requisição, 409 enquanto a primeira executa. Acrescentada depois da Fase 5, a pedido do usuário

## Dependências
T201 → T202 → Fase 2 → Fase 3 → Fase 4 → Fase 5 → (Fases 6 e 7).
A Fase 2 libera a 004 assim que terminar.

## Fase 8 — Evento de publicação: outbox + Debezium (ADR-13 revisado)
- [x] T226 **Testes** da outbox: cada publicação (ativar, sombra, desligar, rollback, scoring) grava exatamente um evento, na mesma transação; falha (conjunto que não compila, erro na auditoria) não grava; o payload é o `rule-set.v1` completo e valida contra o schema — `application/RuleSetOutboxTest.java`
- [x] T227 Migração `V5__outbox.sql` (tabela `outbox` + evento inicial com o conjunto ativo), porta `RuleSetOutbox`, adaptador Postgres e ligação no `RuleSetPublisher`
- [x] T228 **Teste de integração** (Postgres): a linha da outbox e o conjunto entram e saem juntos; payload igual ao do `GET /active`; evento inicial presente depois da migração
- [x] T229 Infra: tópico `rule-sets.v1` (1 partição, compactado) em `create-topics.sh`; `wal_level=logical` no Postgres do projeto; conector Debezium (Outbox Event Router) em `infra/connect/` com script de registro
- [x] T230 **Prova com Kafka Connect real**: publicar pela API e ler o evento no tópico `rule-sets.v1`, com chave `active` e corpo válido contra `rule-set.v1`
