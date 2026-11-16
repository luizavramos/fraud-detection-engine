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
| `notification-service` | Notificações ao antifraude e ao cliente (spec 005, porta 8085; `--profile app`) |
| `transaction-service` | Backfill de 90 dias de histórico para o motor (spec 006, porta 8086; `--profile app`) |

Provas e roteiros neste repositório: `infra/h5-acceptance.sh` (regras a quente), `infra/chaos-h6.sh` (derrubar uma instância do motor), `infra/mocks/smoke.sh` (simuladores dos provedores) e `infra/seed/blocklist.sh` (lista de bloqueio de demonstração).

**Código compartilhado.** `motor-fraude-parent`, `contracts` e `platform-commons` são artefatos Maven versionados (`0.1.0`), publicados no GitHub Packages (repositórios privados; token em `~/.m2/settings.xml`, ver o README do pom pai). Sem registry, instale localmente, nessa ordem: `./mvnw install` em `motor-fraude-parent`, `contracts` e `platform-commons`.

## Rodar o processo completo (um script)

`infra/run-all.sh` faz o caminho inteiro e confere o resultado: sobe a infraestrutura em etapas, registra os conectores do
Debezium (as regras semeadas chegam ao motor pela outbox, o perfil do cliente pelo CDC), sobe gestão de regras, perfil do
cliente, motor, notificações e receiver, semeia a lista de bloqueio e envia duas transações ao receiver. No fim confere que:
as duas tiveram decisão em `fraud-decisions.v1`, só a que tem contraparte em lista de bloqueio gerou alerta HIGH
(`fraud-alerts.v1`), e o notification-service chamou o antifraude (1 vez) e o push do cliente (simuladores dos provedores).

```bash
# repositórios de código ao lado deste (ver "Estrutura: repositórios"); JDK 21; Docker com 4 GB
export JAVA_HOME="C:/Program Files/Java/jdk-21.0.12.1"   # só se o ./mvnw reclamar de JAVA_HOME
sh infra/run-all.sh
```

Termina com `OK: o processo inteiro funcionou (0 falhas)` e imprime o tempo de cada etapa e o total. O primeiro build das
imagens e o `./mvnw package` de cada repositório não entram nessa conta, e a primeira vez baixa as imagens.

Variáveis: `BUILD=1` refaz os JARs; `WITH_BACKFILL=1` inclui a carga de histórico da spec 006 (2.000 transações por
padrão, `N=...` muda); `WITH_OBS=1` sobe também Prometheus e Grafana; `KEEP_ALL=1` não para o Connect e o banco legado
depois de carregar o perfil do cliente (e do backfill, se pedido). Com `WITH_BACKFILL=1` o script semeia as transações no
banco legado, sobe o `transaction-service`, registra o conector `transactions-cdc`, espera o status `COMPLETED` e confere que
o tópico `transaction-history-backfill.v1` recebeu o histórico; só depois sobe o motor, que lê o histórico do tópico (7 dias).

Sobre memória: com 4 GB no Docker, o conjunto inteiro não cabe de uma vez. Por isso o script sobe a infraestrutura em etapas
e, depois do perfil do cliente carregado, **para o Connect e o banco legado** antes de subir motor, notificações e receiver
(sem isso o Docker travou com `i/o timeout`). Consequência: depois do script, mudar o banco legado ou registrar conectores
novos exige subir de novo (`docker compose up -d connect bank-db`). Pode ser rodado mais de uma vez sobre o ambiente de pé.
Se o Docker Desktop cair no meio (aconteceu várias vezes neste notebook), reinicie-o e rode o script de novo.

**O que o script prova e o que não prova.** Prova o caminho de ponta a ponta com duas transações e os simuladores dos
provedores. Não mede latência nem carga (ver "Teste de carga"), não exercita regras a quente (`infra/h5-acceptance.sh`) nem
o caos (`infra/chaos-h6.sh`). Com `WITH_BACKFILL=1` prova o transporte do backfill (status `COMPLETED`, registros no
tópico), mas **não confere que o motor usou o histórico**: isso está provado nos testes do `detection-engine` e no ensaio
H3-B da spec 006 (`infra/backfill-verify.sh`).

Se rodar no Git Bash do Windows: o script exporta `MSYS_NO_PATHCONV=1` (necessário para os caminhos `/opt/kafka/...` do
`docker compose exec`), e com ele o `curl -o` falha com código 23. Por isso os scripts de `infra/` evitam `-o` ou chamam o
`curl` sem essa variável.

## Observabilidade: Prometheus e Grafana

| Ferramenta | Endereço | Acesso |
|---|---|---|
| Grafana | http://localhost:3000 | usuário `admin`, senha `admin` |
| Prometheus | http://localhost:9090 | sem login |

**Subir.** O `docker compose up -d` já sobe os dois. Com o `infra/run-all.sh` eles só sobem com `WITH_OBS=1`, porque usam
memória (com o Docker em 4 GB, deixe desligado se o ambiente travar):

```bash
docker compose up -d prometheus grafana      # com o ambiente já de pé
WITH_OBS=1 sh infra/run-all.sh               # ou junto com o processo completo
```

**Prometheus.** A configuração está em `infra/prometheus/prometheus.yml`. Ele coleta as métricas dos serviços a cada 15 s:
segundos: receiver (8081), rule-management (8082), customer-service (8083), detection-engine (8084, e a segunda instância),
notification-service (8085) e transaction-service (8086). Em *Status → Targets* cada um aparece `UP` ou `DOWN`; um serviço
que não está de pé fica `DOWN`, e isso é esperado. Para consultar uma métrica, use a aba *Graph*; por exemplo, o p99 de
latência do motor fim a fim:

```
histogram_quantile(0.99, sum by (le) (rate(engine_e2e_latency_seconds_bucket[1m])))
```

**Grafana.** O Prometheus já vem configurado como fonte de dados e o dashboard **Motor de fraude** já vem provisionado
(`infra/grafana/dashboards/motor-fraude.json`); abra em *Dashboards*. Ele tem uma seção por serviço:

- **Receiver (spec 001):** requisições por segundo por desfecho, p99 da publicação no Kafka, duplicatas barradas, fail-open do Redis.
- **Gestão de regras (spec 002):** conjuntos publicados, falhas de compilação, consultas ao conjunto ativo.
- **Perfil do cliente (spec 003):** eventos do CDC, perfis publicados, atraso do CDC até o perfil.
- **Motor de detecção (spec 004):** latência fim a fim (p99 e p50 contra o SLO de 0,5 s), decisões por severidade, disparos por regra
  (e em sombra), lag do consumidor, versão do conjunto de regras, DLQ.
- **Notificações (spec 005):** entregas por canal, latência do alerta até a entrega, circuit breakers, DLQ, feedback do analista.

**Dicas.**
- Painéis vazios quase sempre significam que o serviço não está de pé ou que ainda não passou tráfego: rode o
  `infra/run-all.sh` ou o gerador de carga e ajuste o período no canto superior direito para os últimos 5 a 15 minutos.
- Os p99 só valem com o motor **aquecido**; nos primeiros segundos após subir, o JIT frio infla a latência (ver "Teste de carga").
- O script `infra/engine-latency.sh` lê essas mesmas métricas do Prometheus para medir a latência.

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
| 001 Receiver | Fases 1 a 6 prontas (aceita, publica, duplicatas, fail-open, rate limit, métricas, readiness, painel); p99 medido com o gerador de carga **só em escala local de 300 TPS** (T122, `docs/resultados-carga.md`); os 8.000 TPS do SC-001 ficam para a nuvem |
| 002 Rule management, 003 Customer service, 004 Detection engine, 005 Notification service | Prontas, com testes de integração e validação no compose; a medição de carga (8.000 TPS média, 25.000 de pico) é etapa futura e **não foi feita** |
| 006 Transaction service | Pronta e validada no compose (1 milhão de transações: 750.082 publicadas na janela, 0 dado pessoal; depois, com 500 mil e o motor de pé: carga `COMPLETED`, reset do motor com 100 contas iguais ao SQL, limitador a 2 mil/s); SC-001 provado em processo no motor; o limite padrão de 20 mil registros/s contra o motor sob tráfego ao vivo não foi medido |
