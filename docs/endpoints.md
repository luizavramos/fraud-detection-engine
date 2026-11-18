# Endpoints dos serviços

Referência de todos os endpoints HTTP do Motor de Detecção: para que serve cada um, o que enviar e o que volta.

**Como ler este documento.** Os exemplos marcados com ✅ foram **executados contra o serviço no ar** e a resposta é a real.
Os marcados com 📄 vêm do **código e dos contratos** (o serviço não estava de pé para testar) e podem ter pequenas diferenças de formatação.

## Visão geral

| Serviço | Porta | O que expõe | Autenticação | Interface Swagger |
|---|---|---|---|---|
| [receiver](#1-receiver-porta-8081) | 8081 | `POST /v1/transactions:evaluate` (entrada das transações) | Header `X-Client-Id` (lista de origens) | `/swagger-ui.html` |
| [rule-management](#2-rule-management-porta-8082) | 8082 | API de regras, conjuntos de regras e scoring | HTTP Basic, com 3 papéis | `/swagger-ui.html` |
| [customer-service](#3-customer-service-porta-8083) | 8083 | Só operação (saúde e métricas) | Nenhuma | `/swagger-ui.html` |
| [detection-engine](#4-detection-engine-porta-8084) | 8084 | Só operação (saúde e métricas) | Nenhuma | `/swagger-ui.html` |
| [notification-service](#5-notification-service-porta-8085) | 8085 | API do analista: alertas e feedback | **Nenhuma** (ver limitações) | `/swagger-ui.html` |
| [transaction-service](#6-transaction-service-portas-8086-e-8096) | 8086 e 8096 | Saúde e métricas (8086) e status do backfill (8096) | Nenhuma | `/swagger-ui.html` (porta 8086) |

A interface Swagger do customer-service, do detection-engine e do transaction-service carrega os arquivos visuais de um CDN
(cdnjs), então precisa de internet; o JSON do OpenAPI (`/v3/api-docs`) funciona sem isso.

O caminho do fluxo, para situar cada endpoint:

```
origem ──HTTP──▶ receiver ──Kafka──▶ detection-engine ──Kafka──▶ notification-service ──▶ antifraude / push / e-mail
                                          ▲                              ▲
                                  rule-management (tópico          analista (HTTP)
                                  rule-sets.v1)
```

Só o **receiver**, o **rule-management** e o **notification-service** têm API de negócio. Os outros serviços conversam por
Kafka e expõem apenas endpoints de operação.

### Convenções

- **Versão no caminho** (`/v1/...`) e ações como sufixo com dois pontos (`/v1/rules/{id}:disable`).
- **Erros** seguem o formato Problem Details (RFC 9457), com `Content-Type: application/problem+json`. Cada serviço preenche
  um pouco diferente; os exemplos de erro mostram o formato real de cada um.
- **Idempotência.** O receiver exige o header `Idempotency-Key` e o rule-management aceita o mesmo header em todos os `POST`.
- **Dinheiro** é sempre inteiro em **centavos** (`15000` = R$ 150,00). **Datas** em UTC, formato ISO-8601 (`2026-11-17T03:35:38Z`).
- **Nenhum dado pessoal** trafega: contas, clientes e dispositivos são **tokens** (`acc_tok_...`, `cus_tok_...`, `dev_...`).

---

## 1. receiver (porta 8081)

Porta de entrada. Recebe a transação de outro serviço interno, valida o contrato, barra duplicatas e publica no Kafka. Só
responde `202` **depois** que o Kafka confirmou a gravação, então `202` significa "a transação não será perdida".

### `POST /v1/transactions:evaluate`

**Para que serve:** enviar uma transação para ser avaliada pelo motor de detecção. A resposta **não** diz se a transação é
fraude (isso é assíncrono, e sai como decisão e alerta no Kafka); diz só se ela foi aceita.

**Headers**

| Header | Obrigatório | Descrição |
|---|---|---|
| `X-Client-Id` | sim | Origem da chamada. Valores aceitos: `pix-core`, `ted-core`, `boleto-core`, `card-core`. Também é a chave do limite de taxa. |
| `Idempotency-Key` | sim | Tem que ser **igual** ao `transactionId` do corpo. É o que impede processar duas vezes a mesma transação. |
| `Content-Type` | sim | `application/json` |
| `traceparent` | não | Contexto de rastreamento W3C; se vier, o trace continua até a entrega do alerta. |

**Corpo** (JSON Schema `evaluate-transaction.v1`; campos extras são **rejeitados**)

| Campo | Obrigatório | Tipo e regra |
|---|---|---|
| `schemaVersion` | sim | Sempre `1` |
| `transactionId` | sim | Texto de 8 a 64 caracteres: letras, números e `. _ : -` |
| `accountId` | sim | Token da conta: `acc_tok_` + 4 a 64 letras/números |
| `customerId` | sim | Token do cliente: `cus_tok_` + 4 a 64 letras/números |
| `type` | sim | `PIX`, `TED`, `BOLETO` ou `CARD` |
| `amount.value` | sim | Inteiro, mínimo `1`, em centavos |
| `amount.currency` | sim | Só `BRL` |
| `counterparty.accountToken` | sim | Token da conta de destino (`acc_tok_...`) |
| `counterparty.bankIspb` | sim | 8 dígitos (ISPB do banco de destino) |
| `channel` | sim | `MOBILE`, `WEB`, `API` ou `BRANCH` |
| `deviceIdHash` | não | `dev_` + 8 a 64 letras/números |
| `occurredAt` | sim | Instante UTC com `Z` no fim, ex.: `2026-11-17T03:35:38Z` |

**Exemplo de chamada** ✅

```http
POST /v1/transactions:evaluate
X-Client-Id: pix-core
Idempotency-Key: doc1794924430aaaa
Content-Type: application/json

{
  "schemaVersion": 1,
  "transactionId": "doc1794924430aaaa",
  "accountId": "acc_tok_10000001",
  "customerId": "cus_tok_00000001",
  "type": "PIX",
  "amount": { "value": 15000, "currency": "BRL" },
  "counterparty": { "accountToken": "acc_tok_00004711", "bankIspb": "60701190" },
  "channel": "MOBILE",
  "deviceIdHash": "dev_0f3a9c1d5b7e2a64",
  "occurredAt": "2026-11-17T03:35:38Z"
}
```

**Respostas**

| Status | Quando | Corpo |
|---|---|---|
| `202` | Transação aceita e gravada no Kafka | `{"transactionId": "...", "status": "ACCEPTED"}` ✅ |
| `202` | Reenvio de uma transação já aceita | `{"transactionId": "...", "status": "DUPLICATE"}` |
| `400` | Corpo fora do contrato, JSON inválido ou `Idempotency-Key` ausente/diferente do `transactionId` | Problem Details com a lista `errors` ✅ |
| `401` | `X-Client-Id` ausente ou desconhecido | Problem Details ✅ |
| `413` | Corpo maior que o limite configurado | Problem Details 📄 |
| `429` | A origem passou do limite de requisições por segundo (padrão 20.000) | Problem Details + `Retry-After: 1` 📄 |
| `503` | Kafka indisponível ("Serviço indisponível") ou a mesma transação ainda está em processamento ("Transação em andamento") | Problem Details + `Retry-After: 1` 📄 |

Resposta `202` ✅

```json
{"transactionId":"doc1794924430aaaa","status":"ACCEPTED"}
```

Resposta `401` ✅

```json
{"type":"about:blank","title":"Origem não autorizada","status":401,"detail":"X-Client-Id ausente ou desconhecido"}
```

Resposta `400` ✅ (as mensagens de `errors` vêm direto do validador do JSON Schema, em inglês, e não dizem o nome do campo)

```json
{
  "type": "about:blank",
  "title": "Requisição inválida",
  "status": 400,
  "detail": "O corpo ou os headers não respeitam o contrato",
  "errors": [
    "does not match the regex pattern ^[A-Za-z0-9._:-]{8,64}$",
    "must have a minimum value of 1",
    "does not have a value in the enumeration [\"BRL\"]",
    "required property 'accountId' not found"
  ]
}
```

**Detalhe importante sobre duplicatas.** O `DUPLICATE` só aparece quando o Redis responde a tempo (limite de 5 ms). Se o Redis
estiver lento ou fora do ar, o receiver segue sem esse escudo (**fail-open**) e responde `ACCEPTED` de novo; a duplicata é
descartada depois, pelo motor. Foi o que se viu em teste: o reenvio imediato devolveu `ACCEPTED` e, em outro teste, `DUPLICATE`.
O resultado final é o mesmo (a transação é processada uma vez), mas **não use o `status` do `202` para saber se foi duplicata**.

**Como tentar de novo.** `429` e `503` vêm com `Retry-After`: reenvie com o **mesmo** `Idempotency-Key`. É seguro.

### Endpoints de operação (todos os serviços Spring: receiver, rule-management, notification-service)

| Método e caminho | Para que serve | Resposta |
|---|---|---|
| `GET /actuator/health` | Saúde geral | `200` com `{"status":"UP", "components": {...}}` ✅ |
| `GET /actuator/health/liveness` | O processo está vivo (usado para reiniciar) | `200` ou `503` |
| `GET /actuator/health/readiness` | Pronto para receber tráfego | `{"components":{"kafkaProducer":{"status":"UP"},"readinessState":{"status":"UP"}},"status":"UP"}` ✅ |
| `GET /actuator/prometheus` | Métricas em texto, lidas pelo Prometheus | texto `# HELP ... # TYPE ...` |

A prontidão do receiver depende do produtor Kafka; a do rule-management, do banco; a do notification-service, do banco e dos consumidores.

---

## 2. rule-management (porta 8082)

Gestão das regras de detecção **sem redeploy**: cria, valida, testa em modo sombra, ativa, desliga e reverte. Toda mudança
publica um **conjunto de regras** (rule set) novo e versionado. O motor recebe esse conjunto pelo tópico Kafka `rule-sets.v1`
(alimentado por outbox e Debezium), **não** por HTTP; o `GET /v1/rule-sets/active` serve para consultar o que está ativo.

### Autenticação e papéis

HTTP Basic. Os usuários locais (senhas padrão, trocáveis por variável de ambiente) são:

| Usuário | Senha padrão | Papéis | Pode |
|---|---|---|---|
| `analista` | `analista-local` | `RULE_AUTHOR` | criar, validar, pôr em sombra, desligar |
| `coordenador` | `coordenador-local` | `RULE_AUTHOR`, `RULE_APPROVER` | tudo do analista **mais** ativar, reverter e mudar o scoring |
| `motor` | `motor-local` | `RULE_READER` | só ler (usuário de leitura; o motor em si lê as regras pelo Kafka) |

**Quem criou a regra não pode ativá-la** (dupla aprovação): o analista cria, o coordenador ativa. Tentar ativar a própria
regra devolve `403`.

Sem login, qualquer rota devolve `401` ✅:

```json
{"type":"urn:motorfraude:problem:unauthorized","title":"Não autenticado","status":401,"detail":"Informe usuário e senha.","errors":[]}
```

Com login mas sem o papel, `403` ✅:

```json
{"type":"urn:motorfraude:problem:forbidden","title":"Acesso negado","status":403,"detail":"Seu perfil não permite esta operação.","errors":[]}
```

### Tabela de endpoints

| Método e caminho | Papel | Para que serve |
|---|---|---|
| `GET /v1/rules` | leitor, autor ou aprovador | Lista as regras (só id, autor e data) |
| `GET /v1/rules/{ruleId}` | leitor, autor ou aprovador | Histórico completo de versões de uma regra |
| `GET /v1/rule-sets/active` | leitor, autor ou aprovador | Conjunto de regras ativo (consulta; o motor lê o tópico `rule-sets.v1`) |
| `POST /v1/rules:validate` | autor | Testa uma regra sem gravar nada |
| `POST /v1/rules` | autor | Cria uma regra nova (versão 1, em `DRAFT`) |
| `POST /v1/rules/{ruleId}/versions` | autor | Cria uma nova versão de uma regra existente |
| `POST /v1/rules/{ruleId}/versions/{version}:shadow` | autor | Põe a versão em modo sombra |
| `POST /v1/rules/{ruleId}/versions/{version}:activate` | **aprovador** | Ativa a versão (passa a pontuar) |
| `POST /v1/rules/{ruleId}:disable` | autor | Desliga a regra (kill switch por regra) |
| `POST /v1/rule-sets/{version}:rollback` | **aprovador** | Volta o conjunto inteiro para uma versão anterior |
| `PUT /v1/scoring` | **aprovador** | Muda as faixas de severidade (média e alta) |

Qualquer rota que não esteja nesta tabela é negada (`403`).

### Corpo de uma regra (`RuleRequest`)

Usado em `POST /v1/rules`, `POST /v1/rules/{ruleId}/versions` e `POST /v1/rules:validate`.

| Campo | Descrição |
|---|---|
| `ruleId` | Identificador da regra. Só no `POST /v1/rules` (nas versões o id vem na URL). |
| `expression` | Expressão da regra, ex.: `features.count5m > params.maxCount`. É compilada ao salvar. |
| `params` | Parâmetros usados pela expressão, ex.: `{"maxCount": 5}`. Mudar o limite = nova versão só com o parâmetro novo, sem tocar na lógica. |
| `paramOverrides` | Parâmetros diferentes por segmento, ex.: `{"PJ": {"maxCount": 30}}`. |
| `weight` | Pontos somados ao score quando a regra dispara. |
| `reasonTemplate` | Texto do motivo, com variáveis: `Mais de {params.maxCount} transações em 5 min`. |
| `testCases` | Lista de casos de teste: `{"name", "context": {...}, "expectedHit": true/false}`. |

### `POST /v1/rules:validate`

**Para que serve:** conferir uma regra antes de criar. Não grava nada. Compila a expressão e roda os casos de teste.

Chamada ✅

```http
POST /v1/rules:validate
Authorization: Basic analista:analista-local
Content-Type: application/json

{
  "expression": "features.count5m > params.maxCount",
  "params": { "maxCount": 5 },
  "paramOverrides": { "PJ": { "maxCount": 30 } },
  "weight": 25,
  "reasonTemplate": "Mais de {params.maxCount} transacoes em 5 min",
  "testCases": [
    { "name": "6 dispara", "context": { "features.count5m": 6 }, "expectedHit": true },
    { "name": "5 nao dispara", "context": { "features.count5m": 5 }, "expectedHit": false }
  ]
}
```

Resposta `200` ✅

```json
{"valid":true,"errors":[],"cases":[
  {"name":"6 dispara","expectedHit":true,"actualHit":true,"passed":true,"error":null},
  {"name":"5 nao dispara","expectedHit":false,"actualHit":false,"passed":true,"error":null}
]}
```

Com uma feature que não existe, **continua `200`**, mas `valid` vem `false` ✅:

```json
{"valid":false,"errors":["feature desconhecida: contagem5m"],"cases":[]}
```

### `POST /v1/rules` e `POST /v1/rules/{ruleId}/versions`

**Para que serve:** criar a regra (versão 1) ou uma nova versão. Nasce em `DRAFT`: **não afeta nenhuma transação** até alguém ativar.

Resposta `201 Created`, com o header `Location: /v1/rules/{ruleId}/versions/{n}` 📄:

```json
{
  "ruleId": "DEMO1", "version": 1, "status": "DRAFT",
  "expression": "features.count5m > params.maxCount",
  "params": { "maxCount": 5 }, "paramOverrides": { "PJ": { "maxCount": 30 } },
  "weight": 25, "reasonTemplate": "Mais de {params.maxCount} transações em 5 min",
  "author": "analista", "approvedBy": null, "createdAt": "2026-11-17T03:35:38Z"
}
```

Erros: `422` se a expressão não compila ✅, `409` se a regra já existe, `400` se faltar campo ou o JSON for ilegível.

Expressão inválida ✅ (nada é gravado):

```json
{"type":"urn:motorfraude:problem:rule-invalid","title":"Regra inválida","status":422,"detail":"A expressão da regra não compila.","errors":["feature desconhecida: contagem5m"]}
```

Com o header `Idempotency-Key`, repetir o mesmo `POST` devolve **a mesma resposta** (com `Idempotent-Replayed: true`) e não cria de novo.

### `POST .../versions/{version}:shadow`, `:activate` e `POST /v1/rules/{ruleId}:disable`

**Para que servem:** mudar o estado da regra. Sem corpo. Cada um publica um **conjunto de regras novo** e devolve o conjunto (`RuleSetResponse`) 📄:

```json
{
  "version": 12, "publishedAt": "2026-11-17T03:40:00Z", "publishedBy": "coordenador",
  "scoring": { "mediumThreshold": 40, "highThreshold": 70 },
  "rules": [ { "ruleId": "R01", "ruleVersion": 1, "status": "ACTIVE" },
             { "ruleId": "DEMO1", "ruleVersion": 1, "status": "SHADOW" } ]
}
```

| Ação | Efeito |
|---|---|
| `:shadow` | O motor avalia e **registra** a regra, mas ela **não soma pontos**. Serve para testar com tráfego real. |
| `:activate` | A regra passa a somar pontos. A versão anterior sai do conjunto. Exige aprovador diferente do autor. |
| `:disable` | Tira a regra do conjunto (kill switch). |

Erros: `403` se faltar o papel ou se o aprovador for o autor (`self-approval`), `404` se a regra não existir, `409` para
transição inválida de estado, `422` se o conjunto resultante não compilar (nada é publicado).

Aprovador igual ao autor 📄:

```json
{"type":"urn:motorfraude:problem:self-approval","title":"Aprovação por outra pessoa","status":403,"detail":"A ativação precisa ser feita por alguém diferente de quem criou a regra.","errors":["..."]}
```

### `POST /v1/rule-sets/{version}:rollback`

**Para que serve:** kill switch global. Publica de novo o conjunto de uma versão anterior (`{version}` é um número inteiro). Sem
corpo. Responde `200` com o `RuleSetResponse`. Erros: `404` se a versão não existir.

### `PUT /v1/scoring`

**Para que serve:** mudar as faixas de severidade sem tocar em nenhuma regra: score a partir de `mediumThreshold` é severidade média; a partir de `highThreshold`, alta.

```http
PUT /v1/scoring
Authorization: Basic coordenador:coordenador-local

{ "mediumThreshold": 30, "highThreshold": 60 }
```

Os **dois** campos são obrigatórios. Responde `200` com o `RuleSetResponse` (um conjunto novo).

### `GET /v1/rules` ✅

```json
[{"ruleId":"R01","createdBy":"system","createdAt":"2026-11-17T03:35:38.369958Z"},
 {"ruleId":"R03","createdBy":"system","createdAt":"2026-11-17T03:35:38.369958Z"}]
```

### `GET /v1/rules/{ruleId}`

Devolve a regra e **todas as versões**, cada uma com `status`, autor, aprovador e data 📄:

```json
{ "ruleId": "R01", "createdBy": "system", "createdAt": "...", "versions": [ { "ruleId": "R01", "version": 1, "status": "ACTIVE", "...": "..." } ] }
```

Se não existir, `404` ✅:

```json
{"type":"urn:motorfraude:problem:not-found","title":"Não encontrado","status":404,"detail":"regra não encontrada: NAOEXISTE","errors":[]}
```

### `GET /v1/rule-sets/active` ✅

**Para que serve:** consultar o conjunto de regras ativo (a mesma informação que vai ao motor pelo tópico `rule-sets.v1`; o motor
**não** chama este endpoint). Devolve o conjunto ativo com tudo o que o motor usa
(expressão, parâmetros, peso e motivo de cada regra, mais o scoring).

A resposta traz `ETag: "<versão>"`. Quem consulta pode mandar de volta `If-None-Match: "<versão>"` e, se nada mudou, recebe **`304` sem corpo**: barato de repetir.

```json
{
  "version": 1,
  "publishedAt": "2026-11-17T03:35:38.369958Z",
  "scoring": { "mediumThreshold": 40, "highThreshold": 70 },
  "rules": [
    { "ruleId": "R03", "version": 1, "status": "ACTIVE",
      "expression": "features.isNewBeneficiary && tx.amount > params.minAmount",
      "params": { "minAmount": 200000 }, "paramOverrides": {}, "weight": 35,
      "reasonTemplate": "..." }
  ]
}
```

Se nenhum conjunto foi publicado ainda, `404`.

### Erros do rule-management (resumo)

| Status | `type` | Quando |
|---|---|---|
| `400` | `invalid-request` / `unreadable-request` | Campo fora do contrato ou JSON malformado |
| `401` | `unauthorized` | Sem login |
| `403` | `forbidden` / `self-approval` | Sem o papel, ou aprovador igual ao autor |
| `404` | `not-found` | Regra ou versão inexistente |
| `409` | `conflict` | Regra já existe, ou transição de estado inválida |
| `422` | `rule-invalid` / `rule-set-invalid` | Expressão não compila (a regra ou o conjunto) |
| `500` | `internal-error` | Erro inesperado |

Saúde, métricas e Swagger (`/actuator/health`, `/actuator/prometheus`, `/v3/api-docs`, `/swagger-ui.html`) **não exigem login**.

---

## 3. customer-service (porta 8083)

Não tem API de negócio. Lê as mudanças do banco legado de clientes (CDC) e publica o **perfil do cliente** no Kafka, que o
motor usa como contexto (ex.: horas desde a troca de credencial). Expõe só operação 📄:

| Método e caminho | Para que serve |
|---|---|
| `GET /actuator/health` | Saúde geral (`{"status":"UP"}`) |
| `GET /actuator/health/liveness` | O processo está vivo |
| `GET /actuator/health/readiness` | Pronto (estado carregado, stream em execução) |
| `GET /actuator/prometheus` | Métricas Prometheus |

---

## 4. detection-engine (porta 8084)

O coração do sistema, mas **sem API de negócio**: consome `transaction-commands.v1` do Kafka, aplica as regras e publica as
decisões (`fraud-decisions.v1`) e os alertas (`fraud-alerts.v1`). Só expõe operação ✅:

| Método e caminho | Para que serve | Resposta |
|---|---|---|
| `GET /actuator/health` | Saúde geral | `200 {"status":"UP"}`; `503` se `DOWN` |
| `GET /actuator/health/liveness` | O processo está vivo | `200` ou `503` |
| `GET /actuator/health/readiness` | Pronto: estado carregado e stream em execução | `200 {"status":"UP"}` ou `503 OUT_OF_SERVICE` |
| `GET /actuator/prometheus` | Métricas (latência fim a fim, decisões, disparos por regra, lag) | texto Prometheus |

A segunda instância (`detection-engine-2`, usada nos testes de caos) tem as mesmas rotas.

---

## 5. notification-service (porta 8085)

Recebe os alertas do Kafka, entrega ao antifraude, ao cliente (push, e-mail e SMS) e guarda tudo. A API HTTP é a **do
analista**: consultar os alertas e dar feedback.

> Não foi possível chamar este serviço agora (estava parado), então os exemplos abaixo vêm do código 📄.

### `GET /v1/alerts`

**Para que serve:** listar alertas, com filtros e paginação por cursor.

| Parâmetro | Obrigatório | Descrição |
|---|---|---|
| `accountId` | não | Só alertas desta conta (`acc_tok_...`) |
| `severity` | não | `MEDIUM` ou `HIGH` (o tipo também aceita `NONE`, mas alerta só existe para média e alta) |
| `from` | não | Só alertas detectados a partir deste instante (ISO-8601 com data e hora) |
| `limit` | não | Itens por página: padrão `50`, máximo `200` |
| `cursor` | não | O `nextCursor` da página anterior |

Resposta `200`:

```json
{
  "items": [
    {
      "alertId": "7c9e6679-7425-50f0-9f0f-0d5c5a1b8c33",
      "transactionId": "run1794924430-flagged-bbbb",
      "accountId": "acc_tok_10000001",
      "severity": "HIGH",
      "score": 85,
      "ruleSetVersion": 1,
      "contextCompleteness": "FULL",
      "detectedAt": "2026-11-17T03:39:35.164Z",
      "ruleIds": ["R03", "R05"]
    }
  ],
  "nextCursor": "eyJ..."
}
```

`nextCursor` é `null` na última página. Erro: `400` se `limit` estiver fora de 1 a 200 ou se `severity`/`from` forem inválidos.
O significado dos campos: `score` é a soma dos pesos das regras que dispararam; `contextCompleteness` é `PARTIAL` quando o
perfil do cliente não estava disponível; `ruleIds` lista as regras que dispararam.

### `POST /v1/alerts/{alertId}/feedback`

**Para que serve:** o analista diz se o alerta era fraude de verdade. Esse rótulo alimenta as métricas de falso positivo (e o alerta `FeedbackDeFalsoPositivoAlto` do Prometheus).

```http
POST /v1/alerts/7c9e6679-7425-50f0-9f0f-0d5c5a1b8c33/feedback
X-Analyst: maria.silva
Content-Type: application/json

{ "verdict": "FALSE_POSITIVE" }
```

| Item | Descrição |
|---|---|
| `verdict` | **Obrigatório.** `CONFIRMED_FRAUD` ou `FALSE_POSITIVE` |
| `X-Analyst` | Opcional. Nome de quem deu o feedback, gravado junto com ele; se faltar, grava um valor padrão de analista desconhecido |
| Resposta | `201 Created`, **sem corpo** |
| `400` | `verdict` ausente ou valor inválido |
| `404` | `alertId` não existe |

Erro 📄 (formato padrão do Spring):

```json
{"type":"about:blank","title":"Bad Request","status":400,"detail":"verdict é obrigatório: CONFIRMED_FRAUD ou FALSE_POSITIVE","instance":"/v1/alerts/.../feedback"}
```

Saúde (`/actuator/health`, `/actuator/health/readiness`) e métricas (`/actuator/prometheus`) como no receiver.

---

## 6. transaction-service (portas 8086 e 8096)

Faz o **backfill**: lê o histórico de transações do banco e publica no Kafka para o motor começar com o histórico preenchido
(sem isso, as regras de longo prazo não têm base). Não tem API de negócio.

**Porta 8086** (operação): `GET /actuator/health`, `/actuator/health/liveness`, `/actuator/health/readiness` e `/actuator/prometheus`, como no motor, mais `/v3/api-docs` e `/swagger-ui.html`.

**Porta 8096** (status), em servidor próprio:

### `GET /v1/backfill/status` 📄

**Para que serve:** acompanhar a carga de histórico (quantas transações já foram publicadas, quantas descartadas e por quê).

Resposta `200` (`application/json`):

```json
{
  "state": "RUNNING",
  "cutoverAt": "2026-11-17T03:00:00Z",
  "windowStart": "2026-08-19T03:00:00Z",
  "published": 750082,
  "skipped": { "not_snapshot": 0, "out_of_window": 120, "invalid_record": 0 },
  "malformed": 0,
  "completedAt": null
}
```

| Campo | Significado |
|---|---|
| `state` | `RUNNING` enquanto carrega e `COMPLETED` quando termina |
| `cutoverAt` | Instante de corte: o que veio antes é histórico, o que veio depois já é tráfego em tempo real |
| `windowStart` | Início da janela de histórico carregada |
| `published` | Registros publicados no Kafka |
| `skipped` | Registros descartados, por motivo: `not_snapshot` (não é leitura da carga inicial), `out_of_window` (fora dos 90 dias antes do corte) e `invalid_record` (valor zero ou negativo) |
| `malformed` | Registros com formato inválido |
| `completedAt` | Quando terminou (`null` enquanto não terminou) |

Outros métodos devolvem `405` com `Allow: GET`; qualquer outro caminho, `404`. Os números do exemplo são ilustrativos.

---

## Fora do HTTP: tópicos Kafka

Os serviços de detecção conversam por mensageria. Os contratos (JSON Schema) ficam no repositório `contracts`.

| Tópico | De → Para | Conteúdo |
|---|---|---|
| `transaction-commands.v1` | receiver → motor | O mesmo corpo do `POST /v1/transactions:evaluate`, mais headers `commandType`, `traceparent` e `receivedAt` |
| `fraud-decisions.v1` | motor → auditoria | Decisão de cada transação (`NO_ALERT` ou `ALERT`), regras em sombra e as features usadas |
| `fraud-alerts.v1` | motor → notification-service | Alerta: `alertId`, `severity`, `score`, regras disparadas com motivo, `ruleSetVersion`, `contextCompleteness` |

## Limitações a ter em mente

- **O notification-service não tem autenticação** no código local: qualquer um que chegue à porta 8085 lista alertas e dá feedback. O `X-Analyst` é só informativo, não é verificado. Em produção isso precisa de OIDC ou mTLS.
- O rule-management usa **HTTP Basic com usuários em memória**: serve ao ambiente local, não a produção.
- O receiver valida o `X-Client-Id` por **lista fixa**, sem segredo: é identificação, não autenticação forte.
- As mensagens de `errors` do `400` do receiver vêm do validador, em inglês e **sem o nome do campo**; dá para melhorar.
- O OpenAPI gerado automaticamente (`/v3/api-docs`) é **raso**: no rule-management só lista `200`, e no receiver nem descreve o corpo. Este documento é mais completo que o Swagger.
