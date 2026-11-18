# Motor de Detecção de Transações Suspeitas

Motor de detecção de fraude em tempo real: recebe transações de serviços internos, aplica regras configuráveis
(CEL) e gera alertas para o antifraude e para o cliente, com p99 ≤ 500 ms e 8.000/25.000 TPS (média/pico).

- Serviços, arquitetura e fluxo BPMN: [docs/arquitetura-e-servicos.md](docs/arquitetura-e-servicos.md)
- Fluxo de chamadas, da transação à notificação: [docs/fluxo-de-chamadas.md](docs/fluxo-de-chamadas.md)
- Todos os endpoints (request, response e finalidade): [docs/endpoints.md](docs/endpoints.md)

## Passo a passo

Do zero até ver o fluxo completo funcionando (transação entra, decisão sai, alerta chega ao antifraude e ao cliente).
Ao final, o `infra/run-all.sh` confere o resultado sozinho.

**1. Instale o necessário.** Git, **JDK 21** e **Docker** com Docker Compose, com pelo menos **4 GB de memória** liberados para o Docker. No Windows, rode os comandos no Git Bash. Se o `./mvnw` reclamar de `JAVA_HOME`, aponte para o JDK 21.

**2. Clone os repositórios lado a lado**, todos na mesma pasta (o compose procura os serviços em `../receiver`, `../rule-management` etc.):

```bash
mkdir fraud-detection-project && cd fraud-detection-project
for r in fraud-detection-engine motor-fraude-parent contracts platform-commons rules-kernel platform-streams \
         receiver rule-management customer-service detection-engine notification-service transaction-service \
         load-generator auditor; do
  git clone https://github.com/luizavramos/$r.git
done
```

**3. Instale as bibliotecas compartilhadas**, nesta ordem (cada serviço depende delas):

```bash
for r in motor-fraude-parent contracts platform-commons rules-kernel platform-streams; do
  (cd $r && ./mvnw -q install -DskipTests) || break
done
```

**4. Rode o processo completo** a partir do repositório `fraud-detection-engine`:

```bash
cd fraud-detection-engine
sh infra/run-all.sh
```

O script empacota os serviços que faltam, sobe a infraestrutura (Kafka, Redis, Postgres, Debezium), registra os conectores, sobe os
serviços, envia duas transações e confere o resultado. A primeira vez baixa as imagens e compila tudo, então demora mais.

**5. Confira o resultado.** O script termina com:

```
OK: o processo inteiro funcionou (0 falhas)
```

e imprime o tempo de cada etapa. Ele confirma que as duas transações tiveram decisão, que só a da lista de bloqueio gerou alerta
`HIGH` e que o notification-service chamou o antifraude e o push do cliente.

**6. (Opcional) Veja as métricas.** Rode `WITH_OBS=1 sh infra/run-all.sh` ou, com o ambiente de pé, `docker compose up -d prometheus grafana`:
Grafana em http://localhost:3000 (usuário `admin`, senha `admin`) e Prometheus em http://localhost:9090.

**7. Pare e limpe** quando terminar:

```bash
docker compose down -v
```

Para entender o que cada parte faz, comece por [docs/arquitetura-e-servicos.md](docs/arquitetura-e-servicos.md). Mais opções do script
(carga de histórico, observabilidade, caos) estão em "Rodar o processo completo", abaixo.

## Pré-requisitos
- **JDK 21**
- **Docker** com Docker Compose, com pelo menos **4 GB de memória** liberados para o Docker

Não é preciso instalar Maven: cada repositório de código traz o Maven Wrapper (`./mvnw`).

## Subir o ambiente (4 comandos)

```bash
git clone https://github.com/luizavramos/fraud-detection-engine.git
cd fraud-detection-engine
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
| Kafka Connect + Debezium | `localhost:18083` | conector `rule-sets-outbox` (registre com `sh infra/connect/register-rule-sets-outbox.sh`, depois de subir o rule-management); o conector `customers-cdc` registra com `sh infra/connect/register-connectors.sh` e se refaz com `sh infra/connect/rebuild-customers-cdc.sh`; o da 006 ainda não existe |
| Prometheus | `localhost:9090` | alvos dos serviços 8081–8086 aparecem como DOWN até os serviços subirem |
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
O projeto segue uma estrutura de microsserviços, **um repositório por serviço e por biblioteca**. Este repositório (`fraud-detection-engine`) guarda a visão geral: documentação, compose e infraestrutura local. Clone todos lado a lado:

| Repositório | O que é |
|---|---|
| `fraud-detection-engine` (este) | `docs/`, `docker-compose.yml` e `infra/` (tópicos, bancos, Prometheus, Grafana, `smoke.sh`) |
| `motor-fraude-parent` | Pom pai: versões, plugins e regras do Enforcer |
| `contracts` | JSON Schemas versionados (estritos), records Java, `JsonCodec` e `ContractAssertions` |
| `platform-commons` | Base comum sem Spring: logs JSON com dados pessoais mascarados, `traceparent`, `Clock` e regras ArchUnit |
| `receiver` | Porta de entrada HTTP |
| `load-generator` | Gerador de carga |
| `rules-kernel` | Biblioteca de regras: compilação e avaliação CEL |
| `rule-management` | Gestão de regras (porta 8082) |
| `platform-streams` | Base dos apps Kafka Streams: EOS, static membership, saúde e métricas sem Spring |
| `customer-service` | Perfil do cliente a partir do CDC do banco legado (porta 8083) |
| `detection-engine` | Motor de detecção (porta 8084; `--profile app`, e `--profile chaos` para a 2ª instância) |
| `auditor` | Consumidor-auditor: uma decisão por transação aceita, nenhum alerta duplicado |
| `notification-service` | Notificações ao antifraude e ao cliente (porta 8085; `--profile app`) |
| `transaction-service` | Backfill de 90 dias de histórico para o motor (porta 8086; `--profile app`) |

Provas e roteiros neste repositório: `infra/h5-acceptance.sh` (regras a quente), `infra/chaos-h6.sh` (derrubar uma instância do motor), `infra/mocks/smoke.sh` (simuladores dos provedores) e `infra/seed/blocklist.sh` (lista de bloqueio de demonstração).

**Código compartilhado.** `motor-fraude-parent`, `contracts`, `platform-commons`, `rules-kernel` e `platform-streams` são bibliotecas Maven versionadas (`0.1.0`) usadas pelos serviços. O caminho recomendado é instalá-las localmente, nessa ordem, como no "Passo a passo": `./mvnw install` em cada uma. As três primeiras também foram publicadas no GitHub Packages (que costuma exigir token em `~/.m2/settings.xml`, ver o README do pom pai).

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

Variáveis: `BUILD=1` refaz os JARs; `WITH_BACKFILL=1` inclui a carga de histórico (2.000 transações por
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
tópico), mas **não confere que o motor usou o histórico**: isso está provado nos testes do `detection-engine` e no ensaio de verificação do backfill (`infra/backfill-verify.sh`).

Se rodar no Git Bash do Windows: o script exporta `MSYS_NO_PATHCONV=1` (necessário para os caminhos `/opt/kafka/...` do
`docker compose exec`), e com ele o `curl -o` falha com código 23. Por isso os scripts de `infra/` evitam `-o` ou chamam o
`curl` sem essa variável. O mesmo vale para o `./mvnw`: com `MSYS_NO_PATHCONV=1` ele não carrega o Maven (\x{60}Could not find or load main class ...Launcher\x{60}); se for empacotar à mão no Git Bash, rode-o sem essa variável.

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
receiver (8081), rule-management (8082), customer-service (8083), detection-engine (8084, e a segunda instância),
notification-service (8085) e transaction-service (8086). Em *Status → Targets* cada um aparece `UP` ou `DOWN`; um serviço
que não está de pé fica `DOWN`, e isso é esperado. Para consultar uma métrica, use a aba *Graph*; por exemplo, o p99 de
latência do motor fim a fim:

```
histogram_quantile(0.99, sum by (le) (rate(engine_e2e_latency_seconds_bucket[1m])))
```

**Alertas.** As regras estão em `infra/prometheus/alerts.yml` (13 regras) e o estado aparece em http://localhost:9090/alerts
(`inactive`, `pending` enquanto espera o tempo da regra, `firing`). As regras e os grupos:

| Grupo | Regras | Severidade |
|---|---|---|
| Disponibilidade | `ServicoForaDoAr`, `MotorParado` (entrada chegando e nenhuma decisão saindo) | page |
| Latência e vazão | `LatenciaFimAFimAcimaDoSLO` (p99 > 500 ms), `ReceiverRecusandoTransacoes` (> 1% de 503), `ConsumerLagAlto`, `ReceiverPublicacaoLenta` | page / ticket |
| Falsos positivos e regras | `RegraDisparandoAcimaDoNormal` (3x a média de 3 h), `FeedbackDeFalsoPositivoAlto`, `RegrasFalhandoAoCompilarOuCarregar` | ticket |
| Dependências e filas | `CircuitBreakerAberto`, `EntregasFalhandoNoCanal`, `MensagensNaDLQ`, `RedisEmFailOpen` | ticket |

Simplificações: não há Alertmanager (nada é enviado a on-call; o alerta só aparece no Prometheus) e os limites de latência são
fixos. Só o `ServicoForaDoAr` foi visto disparando de verdade, com
serviços parados; as demais foram validadas na sintaxe (`promtool check rules`) e carregadas sem erro.

**Grafana.** O Prometheus já vem configurado como fonte de dados e o dashboard **Motor de fraude** já vem provisionado
(`infra/grafana/dashboards/motor-fraude.json`); abra em *Dashboards*. Ele tem uma seção por serviço:

- **Receiver:** requisições por segundo por desfecho, p99 da publicação no Kafka, duplicatas barradas, fail-open do Redis.
- **Gestão de regras:** conjuntos publicados, falhas de compilação, consultas ao conjunto ativo.
- **Perfil do cliente:** eventos do CDC, perfis publicados, atraso do CDC até o perfil.
- **Motor de detecção:** latência fim a fim (p99 e p50 contra o SLO de 0,5 s), decisões por severidade, disparos por regra
  (e em sombra), lag do consumidor, versão do conjunto de regras, DLQ.
- **Notificações:** entregas por canal, latência do alerta até a entrega, circuit breakers, DLQ, feedback do analista.
- **Carga de histórico:** transações publicadas no histórico e velocidade da carga (publicadas e descartadas por segundo).

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

A **escala de referência local é 300 TPS**, com folga; 500 é o teto confortável.

Três cuidados ao medir localmente:
- **Aqueça** (`--warmup 15s`): o primeiro minuto de um receiver recém-iniciado tem p99 de segundos (JIT).
- **Espere entre rodadas**: depois de uma rodada que colapsa, o Windows fica minutos com milhares de conexões em `TIME_WAIT`
  e as rodadas seguintes falham por falta de portas (`No buffer space available`), mesmo que o receiver esteja bem.
- **Memória do receiver**: o padrão do compose é 512 MB; para carga use `RECEIVER_MEM_LIMIT=1g`.
