# Plano 003 — Customer service

## 1. Resumo
Aplicação **Kafka Streams** (sem Spring) que:
- faz o join da tabela de clientes com a lista agregada de dispositivos, as duas vindas do CDC;
- projeta o resultado num perfil sem dados pessoais;
- publica num tópico compactado.

Não tem banco próprio: o estado do join fica no RocksDB do Kafka Streams.

**Por que Kafka Streams e não um consumidor Spring com Postgres:**
- o join entre duas tabelas que mudam (cliente × dispositivos) é exatamente o caso de uso de `KTable`–`KTable`;
- dispensa um banco só para a projeção;
- usa a mesma base técnica do motor (004), o que reduz o que precisa ser aprendido e operado.

**Contrapartida:** os dados ficam "presos" no Kafka. Para consultar um perfil, é preciso ler o tópico (aceitável, porque só o motor consome).

## 2. Contexto técnico
| Item | Valor |
|---|---|
| Runtime | Java 21, Kafka Streams, `processing.guarantee=exactly_once_v2` |
| Entrada | `cdc.bank.customers`, `cdc.bank.customer_devices` (Debezium, `ExtractNewRecordState` *desabilitado*: o serviço trata `op` e `after`) |
| Saída | `customer-profile.v1` (compact) |
| HTTP | `StreamsAppBootstrap` + `MiniHttpServer` do módulo `platform-streams` (criado aqui, reutilizado no 004 e no 006): `/actuator/health/liveness`, `/actuator/health/readiness` e `/actuator/prometheus`, os mesmos caminhos dos serviços Spring (constituição VII) |
| Porta | 8083 |

## 3. Tabelas do banco legado (definidas em 000, `infra/bank-db/init.sql`, schema Postgres `bank`)
```sql
customers(account_token PK, customer_token, full_name, cpf, email, phone,
          segment, account_opened_at, last_credential_change_at)
customer_devices(account_token, device_hash, first_seen_at, PRIMARY KEY(account_token, device_hash))
```
`full_name`, `cpf`, `email` e `phone` existem **de propósito**, para demonstrar que não vazam (H3).

## 4. Topologia
```
cdc.bank.customers ──► map(Debezium → CustomerRow | tombstone) ──► KTable<accountId, CustomerRow>
                                                                         │
cdc.bank.customer_devices ──► map(→ DeviceRow) ──► groupBy(accountId)    │
        ──► aggregate(KnownDevices: top 10 por first_seen_at) ──► KTable │
                                                                         ▼
                                 join(customers, devices, ProfileProjector::project)   (left join: dispositivos opcionais)
                                                                         ▼
                                            toStream ──► customer-profile.v1
```


## 4A. Decisões (revisão antes da implementação)
- **`updatedAt` é a hora do evento do CDC, não o relógio do serviço.** É o maior `ts_ms` entre o evento do cliente e o do dispositivo que formaram o perfil. Assim a projeção é determinística (constituição III): reprocessar um evento, ou reconstruir tudo (H4), gera exatamente o mesmo perfil, e a compactação do tópico não muda nada. O `ProfileProjector` não recebe `Clock`; `CustomerRow` e `KnownDevices` carregam o instante do último evento.
- **Mesmos caminhos de saúde e métricas dos serviços Spring** (constituição VII): `/actuator/health/liveness`, `/actuator/health/readiness` e `/actuator/prometheus`. O Prometheus usa um único job (`services`) para os seis serviços.
- **Co-particionamento:** `customer-profile.v1` tem a mesma quantidade de partições de `transaction-commands.v1` (6 local, 48 produção) e a chave é o `accountId` como `String` UTF-8. O `to()` do Kafka Streams usa o particionador padrão (murmur2 dos bytes da chave), o mesmo do produtor do receiver; sem isso o `KTable` do motor não casaria com o fluxo de comandos.
- **Chave dos dispositivos:** a chave do Debezium em `cdc.bank.customer_devices` é composta (`account_token`, `device_hash`); a topologia reparticiona por `account_token` antes do `groupBy`.

## 5. Clean Architecture
```
customer-service/src/main/java/br/com/motorfraude/customer/
├── domain/
│   ├── CustomerRow.java          # só os campos permitidos (sem dado pessoal)
│   ├── DeviceRow.java
│   ├── KnownDevices.java         # agregado limitado a 10, ordenado por first_seen_at
│   └── ProfileProjector.java     # função pura e determinística: (CustomerRow, KnownDevices) → CustomerProfile; updatedAt = hora do evento do CDC
├── application/
│   └── ProjectCustomerProfileUseCase.java   # orquestra o projetor; usado pela topologia
├── adapters/
│   ├── controller/cdc/DebeziumCustomerMapper.java   # JSON Debezium → CustomerRow; descarta colunas não permitidas
│   ├── controller/cdc/DebeziumDeviceMapper.java
│   └── gateway/kafka/ProfileSerde.java
└── infrastructure/
    ├── CustomerServiceApp.java   # main, monta a topologia, sobe o MiniHttpServer
    └── TopologyFactory.java
```
**Lista de campos permitidos:** o mapper Debezium lê só `account_token`, `customer_token`, `segment`, `account_opened_at` e `last_credential_change_at`. Qualquer outra coluna é ignorada **antes** de virar objeto de domínio, então dado pessoal nunca entra no estado do Kafka Streams.

## 6. Testes
| Nível | O que cobre |
|---|---|
| Domínio | `KnownDevices` mantém os 10 mais recentes; `ProfileProjector` monta o perfil correto; campos nulos |
| Mapper | eventos Debezium `c`/`u`/`d`/`r`; colunas de dado pessoal descartadas |
| Topologia (`TopologyTestDriver`) | H1 (atualização e tombstone), H2 (limite 10, dispositivo antes do cliente), idempotência |
| Privacidade | todos os perfis gerados de 10 mil clientes validam contra o schema e não contêm padrões de CPF/e-mail |
| Integração (Testcontainers Postgres + Kafka + Debezium) | `UPDATE` no banco → perfil no tópico em ≤ 5 s |
| ArchUnit | regra de dependência |

## 7. Constitution Check
- **I/II:** ✅.
- **III:** ✅ projeção determinística, tópico compactado.
- **IV:** ✅ o motor nunca chama este serviço.
- **VI:** ✅ lista de campos permitidos no adapter + teste de privacidade.
- **VII:** ✅ métrica de atraso do CDC.

## 8. Riscos
- **Debezium no ambiente local:** mesmo risco e mesmo plano B da 000 (CDC simulado).
- **Mudança de schema na base do banco:** o mapper falha de forma explícita (métrica + log), sem publicar um perfil incompleto.
