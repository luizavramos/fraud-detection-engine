# Plano 002 — Gestão de regras + `rules-kernel`

## 1. Resumo
Esta unidade tem dois módulos:
- **`rules-kernel`:** biblioteca Java pura (sem Spring) com o modelo de regra, o catálogo de features e o compilador/avaliador CEL.
- **`rule-management`:** serviço Spring Boot com Postgres (`rules_db`), API REST, versionamento imutável, publicação de conjuntos e endpoint de leitura com ETag.

## 2. Contexto técnico
| Item | Valor |
|---|---|
| CEL | `dev.cel:cel` (cel-java), compilação com declarações tipadas; sem estimador de custo no Java (spike T201), então o limite é de nós (`maxParseExpressionNodeCount`) e profundidade (`AstDepthLimitValidator`) |
| Serviço | Spring Boot 4.0.x, Spring MVC, Spring Security (usuários em memória), Spring JDBC (`JdbcClient`, SQL explícito para `jsonb` e transação serializável) |
| Banco | PostgreSQL 16, banco `rules_db`, migrações Flyway |
| Porta | 8082 |

## 3. `rules-kernel`

### 3.1 Catálogo de features (o "vocabulário" das regras)
| Variável | Tipo CEL | Origem (no motor) |
|---|---|---|
| `tx.amount` | int (centavos) | evento |
| `tx.type`, `tx.channel` | string | evento |
| `tx.hourOfDay` | int (0–23, fuso America/Sao_Paulo) | evento |
| `tx.counterpartyBank` | string | evento |
| `profile.segment` | string (PF/PJ) | customer-profile |
| `profile.accountAgeDays` | int | customer-profile |
| `profile.hoursSinceCredentialChange` | int (-1 se desconhecido) | customer-profile |
| `features.count1m`, `count5m`, `count1h`, `count24h`, `count30d` | int | buckets do motor |
| `features.sum1h`, `sum24h` | int (centavos) | buckets |
| `features.avgAmount30d`, `maxAmount90d` | int (centavos) | buckets diários |
| `features.isNewBeneficiary`, `isNewDevice` | bool | conjuntos limitados |
| `features.distinctBeneficiaries24h` | int | conjunto limitado |
| `lists.counterpartyFlagged`, `lists.deviceFlagged` | bool | blocklist |
| `params.*` | map(string, dyn) | da própria regra |

### 3.2 API do kernel
```java
public record RuleDefinition(String ruleId, int version, RuleStatus status, String expression,
        Map<String,Object> params, Map<String,Map<String,Object>> paramOverrides,
        int weight, String reasonTemplate) {}
public record RuleSetDefinition(long version, Instant publishedAt, Scoring scoring, List<RuleDefinition> rules) {}
public record Scoring(int mediumThreshold, int highThreshold) {}

public final class RuleCompiler {
    CompilationResult compile(RuleDefinition rule);                 // erros de sintaxe, tipo, complexidade
    CompiledRuleSet compileAll(RuleSetDefinition set);                // falha se qualquer regra falhar
}
public final class CompiledRuleSet {
    long version();
    Evaluation evaluate(EvaluationContext ctx);   // hits ACTIVE, hits SHADOW, score, severidade
}
```
- **`paramOverrides`:** a chave é o segmento (`"PJ"`). Na avaliação, os parâmetros efetivos são `params` sobrescritos pelo override do `profile.segment`.
- **Score:** soma dos `weight` das regras ACTIVE disparadas. A severidade sai do `Scoring`: ≥ high → HIGH; ≥ medium → MEDIUM; senão nenhuma.
- **Falhas:** se uma regra lança erro na avaliação (ex.: divisão por zero), ela conta como não disparada e incrementa um contador de erro. Uma regra nunca derruba a decisão.

## 4. Serviço `rule-management`

### 4.1 Camadas
```
rule-management/src/main/java/br/com/motorfraude/rules/
├── domain/        Rule, RuleVersion, RuleSet, RuleStatus, AuditEntry, regras de transição de status
├── application/
│   ├── usecase/   CreateRule, CreateRuleVersion, ValidateRule, ActivateRule, ShadowRule,
│   │              DisableRule, RollbackRuleSet, UpdateScoring, GetActiveRuleSet
│   └── port/out/  RuleRepository, RuleSetRepository, AuditLog, RuleValidator (implementado com rules-kernel)
├── adapters/
│   ├── in/rest/   RuleController, RuleSetController, ScoringController, ProblemDetails
│   └── out/postgres/  repositórios JDBC
└── infrastructure/ config de segurança, Flyway, métricas
```

### 4.2 Publicar um conjunto (núcleo de H2/H4/H5)
Uma única transação no banco:
1. lê os itens do conjunto ativo atual;
2. aplica a mudança (adiciona, troca ou remove uma versão de regra, ou muda o scoring);
3. **compila tudo** com o `RuleCompiler`; se falhar, aborta com `422`;
4. cria `rule_sets` (N+1) + `rule_set_items` + `audit_log`.

"Ativo" = o `rule_sets` de maior versão. O `GET /active` lê esse conjunto e mantém cache em memória por versão (invalidado ao publicar).

### 4.3 Modelo de dados (Flyway `V1__schema.sql`)
Igual ao ERD da spec técnica §4.4: `rules`, `rule_versions` (imutável, PK `(rule_id, version)`), `rule_sets` (com `medium_threshold`, `high_threshold`), `rule_set_items` (PK `(rule_set_version, rule_id)`) e `audit_log`.

Decisões da implementação (Fase 4):
- `rule_set_items` tem coluna `status` (ACTIVE ou SHADOW): é o status que a versão tinha **naquele** conjunto, e o rollback o restaura.
- A imutabilidade de `rule_versions` é imposta por **gatilho**: só `status` e `approved_by` podem mudar. O `audit_log` recusa `UPDATE` e `DELETE` por gatilho.
- A transação de publicação é `SERIALIZABLE` com nova tentativa em conflito (SQLSTATE 40001); 10 ativações simultâneas geram as versões em sequência.
- **Exceção ao "sem Jackson 2":** o Flyway 11 exige o `ObjectMapper` do Jackson 2 (sem ele: `NoClassDefFoundError`). O Enforcer deste módulo o permite, e um teste ArchUnit garante que o nosso código usa só Jackson 3.
Decisões da implementação (Fase 5):
- **Papéis:** `RULE_AUTHOR` cria, valida, põe em sombra e desliga; `RULE_APPROVER` ativa, reverte e muda o scoring; `RULE_READER` só lê (o motor). Usuários em memória: `analista` (autor), `coordenador` (autor e aprovador) e `motor` (leitor). Senhas locais por variável de ambiente.
- **`GET /v1/rule-sets/active`:** o ETag é a versão do conjunto, e o `304` lê só o número da versão (`select max(version)`). O conteúdo completo fica em cache e é refeito quando a versão muda. O status de cada regra no corpo vem do **item do conjunto**.
- **Custo da senha:** o HTTP Basic confere a senha a cada chamada; com BCrypt de custo 10 o `304` levava ~79 ms (p50). Com custo 4 (usuários locais, senha já em texto na configuração; produção usa OIDC no ALB) o p50 caiu para ~5,7 ms.
- **`Idempotency-Key` nos POSTs: NÃO implementado** (item do §4.4). Exige uma tabela de respostas por chave; ficou fora da fase por decisão de escopo e segue pendente.
- O seed (V2) repete as expressões de `rules-kernel/src/test/resources/initial-rules.json`; o teste confere que as 8 compilam juntas (SC-003).

### 4.4 API
| Método | Rota | Papel | Resposta |
|---|---|---|---|
| POST | `/v1/rules` | AUTHOR | 201 + versão 1 DRAFT · 422 se inválida |
| POST | `/v1/rules/{id}/versions` | AUTHOR | 201 + nova versão DRAFT |
| GET | `/v1/rules`, `/v1/rules/{id}` | qualquer | lista e histórico |
| POST | `/v1/rules:validate` | AUTHOR | resultado da compilação + casos de teste |
| POST | `/v1/rules/{id}/versions/{v}:activate` | APPROVER (P3: ≠ autor) | 200 + conjunto novo |
| POST | `/v1/rules/{id}/versions/{v}:shadow` | AUTHOR | 200 + conjunto novo |
| POST | `/v1/rules/{id}:disable` | AUTHOR | 200 + conjunto novo (kill switch) |
| POST | `/v1/rule-sets/{version}:rollback` | APPROVER | 200 + conjunto novo |
| PUT | `/v1/scoring` | APPROVER | 200 + conjunto novo |
| GET | `/v1/rule-sets/active` | serviço (motor) | 200 + `ETag` · 304 |

Erros em Problem Details. Todos os POSTs aceitam `Idempotency-Key`.

## 5. Regras iniciais (Flyway `V2__seed_rules.sql`)
| ID | Expressão (CEL) | Params | Peso |
|---|---|---|---|
| R01 | `profile.hoursSinceCredentialChange >= 0 && profile.hoursSinceCredentialChange < params.hours && tx.amount > params.minAmount` | hours=24, minAmount=100000 | 35 |
| R03 | `features.isNewBeneficiary && tx.amount > params.minAmount` | minAmount=200000 | 25 |
| R04 | `features.count30d >= params.minHistory && tx.amount > max(params.floor, features.avgAmount30d * params.multiplier)` | minHistory=5, floor=100000, multiplier=5 | 35 |
| R05 | `tx.hourOfDay < params.endHour && tx.amount > params.minAmount` | endHour=6, minAmount=100000 | 15 |
| R07 | `features.count5m > params.maxCount` | maxCount=5 (PJ: 30) | 25 |
| R09 | `features.distinctBeneficiaries24h > params.maxDistinct` | maxDistinct=10 (PJ: 100) | 30 |
| R10 | `lists.counterpartyFlagged` | — | 70 |
| R12 | `profile.hoursSinceCredentialChange >= 0 && profile.hoursSinceCredentialChange < 24 && features.isNewDevice && features.isNewBeneficiary` | — | 30 |

Scoring inicial: MÉDIA ≥ 40, ALTA ≥ 70.
*Nota:* `max()` não é nativo do CEL. O kernel registra uma função `max(int,int)` própria, que tem teste.

## 6. Testes
| Nível | O que cobre |
|---|---|
| Kernel: compilação | feature desconhecida, tipo errado, custo acima do limite, as 8 regras compilam |
| Kernel: avaliação | cada regra dispara e não dispara; overrides PJ; SHADOW não soma; erro de regra não derruba |
| Domínio | transições de status permitidas e proibidas |
| Casos de uso (repositórios em memória) | ativar/desligar/rollback criam conjuntos N+1 corretos; compilação falha → nada publicado |
| Integração (Testcontainers Postgres) | migrações, publicação concorrente sem perder versão, `audit_log` |
| REST | códigos, Problem Details, ETag e 304 |
| ArchUnit | regra de dependência nos dois módulos |

## 7. Constitution Check
- **I:** ✅ `rules-kernel` sem framework; serviço em camadas.
- **II:** ✅.
- **III:** ✅ `Idempotency-Key` nos POSTs; versões imutáveis.
- **IV:** ✅ o motor não depende deste serviço por transação (ADR-13).
- **V:** ✅ `rule-set.v1`.
- **VII:** ✅ métricas: publicações, compilações com falha, latência do `/active`.

## 8. Riscos
- **Curva do cel-java:** mitigação: spike de 2 h no início (T201). Plano B: JSONLogic, com perda da checagem de tipos, registrado como ADR se acontecer.
- **Conjuntos grandes demais para o polling:** irrelevante na escala do case (dezenas de regras).
