# Motor de Detecção de Transações Suspeitas

Motor de detecção de fraude em tempo real: recebe transações de serviços internos, aplica regras configuráveis
(CEL) e gera alertas para o antifraude e para o cliente, com p99 ≤ 500 ms e 8.000/25.000 TPS (média/pico).

- Arquitetura e decisões: [technical-spec-motor-deteccao-fraude.md](technical-spec-motor-deteccao-fraude.md) (ADRs 1–13)
- Specs por unidade de implementação: [specs/](specs/README.md)
- Uso de IA no projeto: [docs/uso-de-ia.md](docs/uso-de-ia.md)

## Pré-requisitos
- **JDK 21**
- **Docker** com Docker Compose, com pelo menos **4 GB de memória** liberados para o Docker

Não é preciso instalar Maven: cada repositório de código traz o Maven Wrapper (`./mvnw`).

## Subir o ambiente (4 comandos)

```bash
git clone https://github.com/luizavramos/motor-fraude.git
cd motor-fraude
docker compose up -d
bash infra/smoke.sh
```

O `infra/smoke.sh` espera tudo ficar saudável (limite de 3 minutos) e confere tópicos, bancos e Grafana.
Terminando com `0 falhas`, o ambiente está pronto.

> A primeira vez baixa as imagens (cerca de 5 minutos, dependendo da rede). Isso não conta nos 3 minutos.
> O Kafka Connect é o mais lento para ficar pronto (cerca de 2 minutos).

| Serviço | Endereço no host | Observação |
|---|---|---|
| Kafka (KRaft, 1 broker) | `localhost:9092` | 15 tópicos criados pelo `kafka-init` (`infra/kafka-init/create-topics.sh`) |
| Postgres do projeto | `localhost:5432` | bancos `rules_db` e `alerts_db`; usuário e senha `motorfraude`; `wal_level=logical` (o Debezium lê a outbox do `rules_db`) |
| Banco legado (fonte do CDC) | `localhost:5433` | banco `bank`, usuário e senha `bank`; 10 mil clientes **sintéticos** |
| Redis | `localhost:6379` | |
| Kafka Connect + Debezium | `localhost:18083` | conector `rule-sets-outbox` (registre com `sh infra/connect/register-rule-sets-outbox.sh`, depois de subir o rule-management); o conector `customers-cdc` (spec 003) registra com `sh infra/connect/register-connectors.sh` e se refaz com `sh infra/connect/rebuild-customers-cdc.sh`; o da 006 ainda não existe |
| Prometheus | `localhost:9090` | alvos dos serviços 8081–8086 aparecem como DOWN até as specs 001–006 |
| Grafana | `localhost:3000` | usuário `admin`, senha `admin`; dashboard "Motor de fraude" já provisionado |

Alguma porta ocupada? Copie `.env.example` para `.env` e mude a porta do host.

### Serviços da aplicação
Ficam no perfil `app` do compose, para o `up -d` acima continuar sem build. Exemplo, o receiver (porta 8081):

```bash
# com o repositório receiver clonado ao lado (../receiver)
(cd ../receiver && ./mvnw -DskipTests package)
docker compose --profile app up -d --build receiver
```

### Parar e limpar
```bash
docker compose down        # para os containers e mantém os dados
docker compose down -v     # para e apaga os volumes (próximo start do zero)
```

## Testar
Cada repositório de código tem o seu `./mvnw verify` (unitários `*Test`, integração `*IT`, Enforcer). No Windows, use `mvnw.cmd`;
se aparecer `JAVA_HOME not found`, aponte para o JDK 21 (PowerShell: `$env:JAVA_HOME = "C:\Program Files\Java\jdk-21.0.12.1"`).
Os testes de integração usam Testcontainers e precisam do Docker.

## Estrutura: repositórios
O projeto segue uma estrutura de microsserviços, **um repositório por serviço e por biblioteca**. Este repositório (`motor-fraude`) guarda a visão geral: specs, documentação, compose e infraestrutura local. Clone todos lado a lado:

| Repositório | O que é |
|---|---|
| `motor-fraude` (este) | Specs 000–006, ADRs, `docs/`, `docker-compose.yml` e `infra/` (tópicos, bancos, Prometheus, Grafana, `smoke.sh`) |
| `motor-fraude-parent` | Pom pai: versões, plugins e regras do Enforcer |
| `contracts` | JSON Schemas versionados (estritos), records Java, `JsonCodec` e `ContractAssertions` |
| `platform-commons` | Base comum sem Spring: logs JSON com dados pessoais mascarados, `traceparent`, `Clock` e regras ArchUnit |
| `receiver` | Porta de entrada HTTP (spec 001) |
| `load-generator` | Gerador de carga (spec 000, Fase 5) |
| `rules-kernel` | Biblioteca de regras: compilação e avaliação CEL (spec 002) |
| `rule-management` | Gestão de regras (spec 002, porta 8082) |
| `platform-streams` | Base dos apps Kafka Streams: EOS, static membership, saúde e métricas sem Spring |
| `customer-service` | Perfil do cliente a partir do CDC do banco legado (spec 003, porta 8083) |
| `detection-engine` | Motor de detecção (spec 004, porta 8084; `--profile app`, e `--profile chaos` para a 2ª instância) |
| `auditor` | Consumidor-auditor: uma decisão por transação aceita, nenhum alerta duplicado (spec 004, H6) |
| `notification-service` | Notificações ao antifraude e ao cliente (spec 005, porta 8085; em implementação) |

O `transaction-service` (spec 006) ganha o repositório quando for implementado. Provas e roteiros neste repositório: `infra/h5-acceptance.sh` (regras a quente), `infra/chaos-h6.sh` (derrubar uma instância do motor), `infra/mocks/smoke.sh` (simuladores dos provedores) e `infra/seed/blocklist.sh` (lista de bloqueio de demonstração).

**Código compartilhado.** `motor-fraude-parent`, `contracts` e `platform-commons` são artefatos Maven versionados (`0.1.0`), publicados no GitHub Packages (repositórios privados; token em `~/.m2/settings.xml`, ver o README do pom pai). Sem registry, instale localmente, nessa ordem: `./mvnw install` em `motor-fraude-parent`, `contracts` e `platform-commons`.

## Teste de carga
O gerador (repositório `load-generator`) envia transações sintéticas ao receiver em **modelo aberto**: cada requisição tem
horário planejado e a latência é medida a partir dele, então uma parada do alvo aparece no p99 em vez de sumir.
Cenários de fraude (sequestro de conta, rajada, pulverização, beneficiário em lista) e duplicatas são sorteados nas
proporções pedidas e marcados no header `x-synthetic-scenario`. O relatório mostra o TPS alcançado, as respostas por
status, os erros de rede por tipo, a latência (p50 a máx) e **a CPU que o próprio gerador gastou** (para saber se ele foi
o gargalo).

```bash
(cd ../load-generator && ./mvnw -q package -DskipTests)
(cd ../receiver && ./mvnw -q package -DskipTests)
RECEIVER_MEM_LIMIT=1g docker compose --profile app up -d --build receiver
java -jar ../load-generator/target/load-generator.jar --tps 300 --duration 60s --warmup 15s
```

Também roda com `./mvnw exec:java -Dexec.args="--tps 300 --duration 60s"` na pasta do `load-generator`. Para medir, prefira o jar: sem o Maven por perto. `--help` lista todas as opções.

**Escala medida.** Os 8.000 TPS (média) e 25.000 (pico) do case **não foram medidos nesta entrega** (a medição na nuvem é uma etapa futura): num notebook, receiver, Kafka e gerador dividem os mesmos núcleos. O que há é a medição local numa máquina de
4 núcleos físicos (8 lógicos), com receiver e Kafka no Docker e o gerador no host:

| TPS | Erros | p99 |
|---|---|---|
| 100 a 300 | 0 | 71 a 103 ms (receiver já aquecido) |
| 500 | 0 | 104 ms |
| 800 | 93% | colapso (o gerador sozinho usa 3,4 núcleos) |

A **escala de referência local é 300 TPS**, com folga; 500 é o teto confortável. Detalhes e ressalvas em
[docs/resultados-carga.md](docs/resultados-carga.md).

Três cuidados ao medir localmente:
- **Aqueça** (`--warmup 15s`): o primeiro minuto de um receiver recém-iniciado tem p99 de segundos (JIT).
- **Espere entre rodadas**: depois de uma rodada que colapsa, o Windows fica minutos com milhares de conexões em `TIME_WAIT`
  e as rodadas seguintes falham por falta de portas (`No buffer space available`), mesmo que o receiver esteja bem.
- **Memória do receiver**: o padrão do compose é 512 MB; para carga use `RECEIVER_MEM_LIMIT=1g`.

## Andamento das specs

| Spec | Estado |
|---|---|
| 000 Plataforma e contratos | Pronta (inclui o gerador de carga, Fase 5) |
| 001 Receiver | Fases 1 a 6 prontas (aceita, publica, duplicatas, fail-open, rate limit, métricas, readiness, painel); falta só medir o p99 com o gerador de carga (T122) |
| 002–006 | Não iniciadas |
