# Tarefas 003 — Customer service

Base de caminhos: `customer-service/src/{main,test}/java/br/com/motorfraude/customer/`

## Fase 1 — Setup (inclui a base para apps Kafka Streams)
- [ ] T301 Criar o módulo `platform-streams` (depende de `platform-commons`, kafka-streams e micrometer-prometheus; pacote `br.com.motorfraude.platform.streams`) com `StreamsAppBootstrap` (configuração EOS, static membership, shutdown limpo, binding de métricas do Kafka Streams no Micrometer) e `MiniHttpServer` (`/health/live`, `/health/ready`, `/metrics`; usa o servidor HTTP do JDK, sem Spring), com testes — **reutilizado pelo 004 e pelo 006** (000 plan §4 e §6)
- [ ] T302 Criar o módulo `customer-service` (depende de `contracts`, `platform-commons` e `platform-streams`) + teste ArchUnit
- [ ] T303 [P] Registrar os conectores Debezium para `customers` e `customer_devices` em `infra/connect/register-connectors.sh` (publicação `dbz_customers`, slot próprio, `topic.prefix=cdc`, JSON sem schema — 000 plan §5.1) e adicionar o serviço ao compose (porta 8083)

## Fase 2 — Domínio (H1, H2, H3)
- [ ] T304 **Testes** de `KnownDevices` (limite 10, ordem por `first_seen_at`, remoção) e de `ProfileProjector` (campos, nulos) — `domain/*Test.java`
- [ ] T305 Implementar `CustomerRow`, `DeviceRow`, `KnownDevices` e `ProfileProjector`
- [ ] T306 **Testes** dos mappers Debezium com fixtures reais (`c`, `u`, `d`, `r`) — colunas `full_name`, `cpf`, `email` e `phone` precisam ser descartadas — `adapters/in/cdc/*Test.java`, `src/test/resources/debezium/`
- [ ] T307 Implementar `DebeziumCustomerMapper` e `DebeziumDeviceMapper` com lista de campos permitidos

## Fase 3 — Topologia (MVP)
- [ ] T308 **Testes** com `TopologyTestDriver`: atualização → perfil; remoção → tombstone; dispositivo antes do cliente; 15 dispositivos → 10; evento repetido → mesmo perfil; todo perfil emitido passa em `ContractAssertions.assertProduces("customer-profile.v1", …)` (schema estrito: coluna de dado pessoal que vaze quebra o teste) — `TopologyFactoryTest.java`
- [ ] T309 Implementar `TopologyFactory` (plan §4), `ProfileSerde` e `CustomerServiceApp`
- [ ] T310 **Teste de privacidade:** gerar perfis para os 10 mil clientes sintéticos e validar contra o schema + ausência de padrões de dado pessoal — `PrivacyTest.java`

**Checkpoint:** o motor (004) já pode consumir `customer-profile.v1`.

## Fase 4 — Integração e operação
- [ ] T311 **Teste de integração** (Testcontainers: Postgres com `wal_level=logical` + Kafka + Debezium Connect): `UPDATE` → perfil em ≤ 5 s (SC-001) — `CustomerServiceIT.java`
- [ ] T312 Métricas `customer_cdc_events_total{op}`, `customer_profiles_published_total` e `customer_cdc_lag_seconds` + painel no Grafana
- [ ] T313 Documentar a reconstrução (H4): como reiniciar o conector com snapshot — `customer-service/README.md`

## Dependências
T301 → T302 → Fase 2 → Fase 3 → Fase 4. T303 pode correr junto com a Fase 2.
