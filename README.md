# Motor de Detecção de Transações Suspeitas

Motor de detecção de fraude em tempo real: recebe transações de serviços internos, aplica regras configuráveis
(CEL) e gera alertas para o antifraude e para o cliente, com p99 ≤ 500 ms e 8.000/25.000 TPS (média/pico).

- Arquitetura e decisões: [technical-spec-motor-deteccao-fraude.md](technical-spec-motor-deteccao-fraude.md) (ADRs 1–13)
- Specs por unidade de implementação: [specs/](specs/README.md)
- Uso de IA no projeto: [docs/uso-de-ia.md](docs/uso-de-ia.md)

## Pré-requisitos
- **JDK 21**
- **Docker** com Docker Compose, com pelo menos **4 GB de memória** liberados para o Docker

Não é preciso instalar Maven: o projeto usa o Maven Wrapper (`./mvnw`).

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
| Kafka (KRaft, 1 broker) | `localhost:9092` | 14 tópicos criados pelo `kafka-init` (`infra/kafka-init/create-topics.sh`) |
| Postgres do projeto | `localhost:5432` | bancos `rules_db` e `alerts_db`; usuário e senha `motorfraude` |
| Banco legado (fonte do CDC) | `localhost:5433` | banco `bank`, usuário e senha `bank`; 10 mil clientes **sintéticos** |
| Redis | `localhost:6379` | |
| Kafka Connect + Debezium | `localhost:18083` | sem conectores até as specs 003 e 006 |
| Prometheus | `localhost:9090` | alvos dos serviços 8081–8086 aparecem como DOWN até as specs 001–006 |
| Grafana | `localhost:3000` | usuário `admin`, senha `admin`; dashboard "Motor de fraude" já provisionado |

Alguma porta ocupada? Copie `.env.example` para `.env` e mude a porta do host.

### Parar e limpar
```bash
docker compose down        # para os containers e mantém os dados
docker compose down -v     # para e apaga os volumes (próximo start do zero)
```

## Testar

```bash
./mvnw verify
```

No Windows, use `mvnw.cmd verify`. Se aparecer `JAVA_HOME not found`, aponte para o JDK 21 (por exemplo, no
PowerShell: `$env:JAVA_HOME = "C:\Program Files\Java\jdk-21.0.12.1"`).

`verify` roda os testes unitários (`*Test`, Surefire) e de integração (`*IT`, Failsafe) de todos os módulos, e o
Maven Enforcer, que barra Java < 21 e Jackson 2 (o projeto usa Jackson 3).

## Estrutura

| Pasta | O que é |
|---|---|
| `contracts/` | JSON Schemas versionados (estritos), records Java, `JsonCodec` e `ContractAssertions` (teste de contrato dos produtores) |
| `platform-commons/` | Base comum sem Spring e sem Kafka Streams: logs JSON com dados pessoais mascarados, `traceparent` em headers Kafka, `Clock` e regras ArchUnit da Clean Architecture |
| `infra/` | Compose: criação de tópicos, `init.sql` dos bancos, Prometheus, Grafana e o `smoke.sh` |
| `specs/` | Constituição do projeto e specs 000–006, com plano e tarefas de cada uma |
| `docs/` | Registro do uso de IA |

Os módulos de serviço (`receiver`, `rule-management`, `customer-service`, `detection-engine`,
`notification-service`, `transaction-service`) entram nas specs 001–006.

## Teste de carga
Entra junto com o gerador de carga (spec 000, Fase 5), que foi adiado para depois do receiver (spec 001). Comando
previsto:

```bash
./mvnw -pl load-generator exec:java -Dexec.args="--tps 8000 --duration 60s"
```

## Andamento das specs

| Spec | Estado |
|---|---|
| 000 Plataforma e contratos | Fases 1–4 e fechamento prontos; gerador de carga (Fase 5) adiado |
| 001–006 | Não iniciadas |
