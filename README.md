# Motor de Detecção de Transações Suspeitas

Motor de detecção de fraude em tempo real: recebe transações de serviços internos, aplica regras configuráveis (CEL) e gera alertas para o antifraude e para o cliente, com p99 ≤ 500 ms e 8.000/25.000 TPS (média/pico).

- Arquitetura e decisões: [technical-spec-motor-deteccao-fraude.md](technical-spec-motor-deteccao-fraude.md) (ADRs 1–13)
- Specs por unidade de implementação: [specs/](specs/README.md)
- Uso de IA no projeto: [docs/uso-de-ia.md](docs/uso-de-ia.md)

## Pré-requisitos
- JDK 21
- Docker com Docker Compose

Não é preciso instalar Maven: o projeto usa o Maven Wrapper (`./mvnw`).

## Como rodar

> Em construção (spec 000, T026). Por enquanto:

```bash
./mvnw verify
```

No Windows, use `mvnw.cmd verify`.
