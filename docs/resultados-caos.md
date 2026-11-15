# Resultado do teste de caos (spec 004, H6)

**Pergunta:** derrubar uma instância do motor no meio da carga gera alerta duplicado ou deixa transação sem decisão?
**Resposta nesta rodada local: não.** Auditoria **aprovada**: 7.416 transações aceitas, 7.416 com exatamente uma decisão,
0 faltantes, 0 decisões duplicadas, 0 alertas duplicados e 0 decisões MEDIUM ou HIGH sem alerta.

## O que foi feito
- **Duas instâncias do motor** (`detection-engine` e `detection-engine-2`, mesmo `application.id`), cada uma com 1 GB, dividindo as
  18 partições de entrada (6 tarefas: 2 e 4). Receiver, Kafka (1 broker), Redis e Prometheus no mesmo Docker.
- **Carga:** `load-generator --tps 150 --duration 45s --warmup 5s --seed 11` (6.750 requisições; 6.688 `ACCEPTED`, 61 `DUPLICATE`,
  1 `503`, 0 erro de rede; p99 HTTP 305 ms). O gerador gravou os `transactionId` aceitos (`--accepted-log`), 7.416 ids únicos
  contando o aquecimento.
- **Falha:** 25 s depois do início, `docker kill` na instância `233a0bb7ab02`, a que tinha 12 das 18 partições. SIGKILL: sem
  encerramento limpo, sem commit final. Saiu com código 137.
- **Recuperação:** a outra instância assumiu as 18 partições, restaurou o estado do tópico de changelog e seguiu; decidiu
  5.428 transações no total (63 HIGH, 1 MEDIUM) e descartou 10 duplicatas no dedup.
- **Auditoria:** `auditor --accepted-log aceitas.log` lê `fraud-decisions.v1` e `fraud-alerts.v1` com `read_committed` (as saídas
  do motor são transacionais; mensagens de transações abortadas não contam).

```
aceitas: 7416            com decisão: 7416           faltantes: 0
decisões duplicadas: 0   alertas duplicados: 0       MEDIUM/HIGH sem alerta: 0
decisões fora do log de aceitas (informativo): 38369
APROVADO
```
As 38.369 decisões "fora do log" são de rodadas anteriores (testes de aceite e a rodada de carga que colapsou, em que o Kafka
gravou transações que o receiver respondeu com timeout). Não entram na conta de faltantes, mas o auditor também confere
duplicidade nelas: nenhuma.

## Por que passa (o mecanismo é o do EOS; esta rodada mostra o resultado, não observei a transação aberta ser abortada)
- **EOS v2:** a decisão, a atualização do estado (features e dedup) e o commit do offset de entrada saem na mesma transação do
  Kafka. A instância morta deixa uma transação aberta, que expira e é abortada; a que assume reprocessa a partir do último
  offset confirmado, com o estado restaurado do changelog, e o consumidor `read_committed` não vê o que foi abortado.
- **Dedup e UUIDv5:** o `seen-tx` (dentro da mesma transação) barra o reprocessamento de um `transactionId` já decidido, e o
  `alertId` e o `decisionId` são derivados do `transactionId`, então até uma repetição geraria o mesmo identificador.

## O que **não** foi provado aqui
- **8.000 TPS** e a carga de pico de 25.000 TPS: **não medidos** (não há medição na nuvem). Aqui foram 150 TPS, porque o limite de portas efêmeras do Windows
  (cerca de 9 mil conexões por rodada) e o notebook compartilhado entre gerador, Docker e motor não permitem mais.
- **Uma só rodada, um só ponto de falha** (a instância com mais partições, no meio da carga). Não variei o instante do kill, não
  matei a instância durante um rebalance e não testei a queda do broker.
- **Reinício da instância morta** e volta ao grupo: não repeti a auditoria depois disso.
- **Impacto no p99:** durante o failover as decisões das partições da instância morta esperam o timeout da transação aberta e o
  rebalance; o painel mostra o atraso, mas não medi o p99 dessa janela isoladamente.

## Como reproduzir
```bash
docker compose up -d kafka kafka-init redis prometheus
docker compose --profile app --profile chaos up -d --build receiver detection-engine detection-engine-2
sh infra/chaos-h6.sh
```
Espere ~1 min depois de subir as duas instâncias (intervalo de *probing rebalance* do Kafka Streams, no mínimo 60 s) para que
a segunda receba tarefas; o cabeçalho do script mostra como conferir.
