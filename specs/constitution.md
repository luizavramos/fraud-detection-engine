# Constituição do projeto — Motor de Detecção de Transações Suspeitas

> Regras que **toda** spec, plano e tarefa deste repositório deve seguir. Um plano que viola um princípio precisa justificar a exceção na seção "Constitution Check".
> Origem das decisões: [technical-spec-motor-deteccao-fraude.md](../technical-spec-motor-deteccao-fraude.md) (ADRs 1–13).

## Princípios

### I. Clean Architecture com regra de dependência verificada
Cada serviço tem quatro camadas: `domain` → `application` (casos de uso + portas) → `adapters` (in/out) → `infrastructure`. O código só depende de camadas mais internas. `domain` e `application` não importam Spring, Kafka, JDBC, Redis nem Jackson. **Teste ArchUnit obrigatório** em cada serviço; ele falha o build se a regra for violada.

### II. Testes antes da implementação
Toda tarefa de implementação tem uma tarefa de teste antes dela. A regra do ciclo é: o teste é escrito, roda e **falha**; só então a implementação é feita. Domínio e casos de uso são testados com portas em memória (sem infraestrutura). Adapters são testados com Testcontainers. Cada história de usuário tem ao menos um teste de aceite que reproduz seus cenários *Given/When/Then*.

### III. Idempotência por padrão
Toda operação que recebe uma mensagem ou requisição deve produzir o mesmo resultado se receber a mesma entrada de novo. A chave de idempotência é explícita no contrato (`transactionId`, `alertId`, `Idempotency-Key`).

### IV. Nada auxiliar no caminho quente
Entre receber a transação e publicar o alerta, nenhum serviço faz chamada síncrona a uma dependência auxiliar (banco de clientes, gestão de regras, provedores). Dados de apoio ficam em cópia local. Exceção única: o Redis do receiver, que é **fail-open** (se cair, deixa passar).

### V. Contratos versionados
- **Nomes de tópicos e rotas** carregam a versão (`.v1`, `/v1`).
- **Schemas JSON** ficam em `contracts/` e são validados nos testes.
- **Mudanças** só podem ser compatíveis com versões anteriores. Uma mudança incompatível exige uma versão nova.

### VI. Privacidade (LGPD)
Tópicos, bancos do projeto e logs carregam **apenas tokens** (`acc_tok_…`, `cus_tok_…`), nunca CPF, nome, número de conta, e-mail ou telefone. Um teste automatizado falha se um padrão de CPF, e-mail ou telefone aparecer em log.

### VII. Observabilidade desde o primeiro commit
- **Métricas:** todo serviço expõe `/actuator/health` (liveness e readiness) e `/actuator/prometheus`.
- **Logs:** são JSON, com `traceId`, `transactionId` e `alertId` quando existirem.
- **Rastreamento:** o `traceparent` é propagado em headers HTTP e Kafka.
- **Cardinalidade:** é proibido usar `accountId` ou `transactionId` como label de métrica.

### VIII. Dinheiro e tempo sem ambiguidade
Valores monetários em **centavos como `long`** (nunca `double`/`float`). Instantes em **UTC** (`Instant`, ISO-8601 com `Z`). Toda dependência de "agora" passa pela porta `Clock`, para os testes serem determinísticos.

### IX. Simplicidade e escopo honesto
Implementar o mínimo que prova o requisito. O que não for implementado fica marcado `[DOC]` na spec e citado na apresentação. Nenhuma abstração sem uso atual (YAGNI).

## Stack fixa

| Item | Escolha |
|---|---|
| Linguagem | Java 21 (records, sealed interfaces, virtual threads) |
| Build | Maven (com Maven Wrapper `./mvnw`), multi-módulo: um `pom.xml` pai + um módulo por serviço |
| Serviços HTTP | Spring Boot 4.0.x (último patch; Spring Framework 7, Jackson 3) |
| Motor | Kafka Streams "puro" (sem Spring) + Micrometer |
| Regras | CEL (`cel-java`) |
| Mensageria | Apache Kafka (KRaft) local; Amazon MSK em produção |
| Bancos | PostgreSQL 16 (Flyway), Redis 7, RocksDB (estado do motor) |
| Testes | JUnit 5, AssertJ, jqwik, Kafka Streams `TopologyTestDriver`, Testcontainers, WireMock, ArchUnit |
| Resiliência | Resilience4j |
| Observabilidade | Micrometer + Prometheus + Grafana, OpenTelemetry |
| Execução local | `docker compose up` sobe tudo |

## Convenções

- **Pacote base:** `br.com.motorfraude.<servico>`.
- **Portas locais:**
  - 8081 receiver
  - 8082 rule-management
  - 8083 customer-service
  - 8084 detection-engine (só actuator)
  - 8085 notification-service
  - 8086 transaction-service
- **Configuração:** por variáveis de ambiente, com valores padrão para execução local.
- **Mensagens de commit** em português, no imperativo.

## Governança
- **Alterar esta constituição** exige registrar o motivo no fim do arquivo.
- **Antes de implementar,** cada `plan.md` passa pelo "Constitution Check".
- **Uma tarefa só termina** com os testes passando e o ArchUnit verde.

| Versão | Data | Mudança |
|---|---|---|
| 1.0 | 2026-11-13 | Versão inicial |
| 1.1 | 2026-11-13 | Spring Boot 3.x → **4.0.x**. Motivo: a linha 3.x perdeu o suporte gratuito em 30/06/2026; começar o projeto numa versão sem patches de segurança contradiz o requisito de segurança do case. A 4.0 tem suporte gratuito até 31/12/2026 e a migração para a 4.1 é de versão menor, na mesma linha do Spring Framework 7. Consequência: Jackson 3 (`tools.jackson`) em todo o projeto, inclusive nos módulos sem Spring, para não conviverem duas versões do Jackson |
