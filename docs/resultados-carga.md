# Resultados de carga

Medições do receiver com o gerador de carga. O motor acrescenta as suas.

## Escala medida e o que não foi medido
O case pede 8.000 TPS de média e 25.000 de pico. **Essas taxas não foram medidas nesta entrega: a medição na nuvem é uma etapa futura.** Localmente tudo divide os mesmos núcleos (o gerador chega a gastar 3,4 núcleos), então o que se mede aqui é o
comportamento numa **escala de referência de 300 TPS**, com 500 TPS como teto confortável.

**Este documento não comprova os requisitos de 8.000 e 25.000 TPS.** Não verificados: 8.000 TPS sustentados por 5 min
p99 do receiver ≤ 30 ms a 8.000 TPS e os 25.000 TPS de pico. Não extrapolo a partir
dos 300 TPS: o que a escada abaixo mostra é onde este notebook deixa de ser confiável (800 TPS já colapsa).

## Ambiente
Windows 11, 4 núcleos físicos (8 lógicos), 7,8 GB de RAM, Docker Desktop com 4 GB e 8 CPUs.
- Receiver no Docker com `RECEIVER_MEM_LIMIT=1g` e G1 GC; Kafka (1 broker, 1 GB), Redis, Prometheus e Grafana no Docker.
- Gerador no host (`java -jar load-generator.jar`), `--warmup 10s --duration 20s --seed 1`, fraude 0,5% e duplicatas 1%.
- Entre as rodadas, espera as conexões em `TIME_WAIT` do Windows drenarem (abaixo de 300).

## Escada de taxas (receiver aquecido, exceto a primeira linha)
| TPS | Respondidas | Erros de rede | p50 | p90 | p99 | Obs. |
|---|---|---|---|---|---|---|
| 100 | 2.000 de 2.000 | 0 | 90,9 ms | 1.398,8 ms | 2.439,2 ms | receiver **recém-iniciado** (JIT frio) |
| 150 | 3.000 de 3.000 | 0 | 22,5 ms | 35,2 ms | 463,6 ms | |
| 200 | 4.000 de 4.000 | 0 | 22,5 ms | 36,4 ms | 71,1 ms | |
| 300 | 6.000 de 6.000 | 0 | 30,7 ms | 61,7 ms | 103,2 ms | **escala de referência** |
| 500 | 10.000 de 10.000 | 0 | 23,7 ms | 43,6 ms | 104,2 ms | 3 respostas 503 |
| 800 | 1.144 de 16.000 | 14.856 | 4.309 ms | 5.513 ms | 6.693 ms | colapso; o gerador usa 3,4 núcleos |

A latência é medida a partir do **horário planejado** de cada requisição (modelo aberto): se o alvo trava, o atraso entra
na conta. O receiver só responde `202` depois do ack do Kafka, então o p99 inclui a publicação.

## O que as medições encontraram
1. **Serial GC em container pequeno.** Com menos de ~1,8 GB, a JVM escolhe o Serial GC sozinha. Medido a 500 TPS: pausa
   máxima de 3,0 s e cerca de 10 s de pausa acumulada, o que gerou timeouts do Redis (fail-open, duplicatas não reconhecidas)
   e publicações de até 11,9 s. O `Dockerfile` do receiver passou a fixar `-XX:+UseG1GC` e `MaxRAMPercentage=60`.
2. **Esgotamento de portas no Windows contamina as medições.** Uma rodada que colapsa deixa milhares de conexões em
   `TIME_WAIT` (5.862 medidas); as rodadas seguintes falham com `No buffer space available` mesmo com o receiver saudável.
   Várias medições minhas feitas logo depois do primeiro colapso (receiver no host, receiver sob JFR, gerador isolado contra
   um alvo trivial) ficaram confundidas e **foram descartadas**. A escada acima foi refeita com espera entre as rodadas.
3. **Contenção sob oversubscrição (hipótese, não confirmada).** Num perfil JFR do receiver (rodada já contaminada), 31,6 s de
   contenção de monitor em 40 s, no `ConnectionHandler` do Tomcat e no `KafkaProducer.doSend`. Com threads virtuais, bloquear
   dentro de `synchronized` prende o núcleo de execução. Não confirmada: exigiria medir com o gerador em outra máquina (etapa futura, na nuvem).
4. **`503` não prova que nada foi gravado**: o Kafka pode gravar depois do timeout.
5. **Partida a frio pesa.** O p99 do primeiro degrau (2,4 s) contra 71 a 103 ms nos seguintes. Por isso `--warmup`.

## Como reproduzir
```bash
(cd ../load-generator && ./mvnw -q package -DskipTests) && (cd ../receiver && ./mvnw -q package -DskipTests)
RECEIVER_MEM_LIMIT=1g docker compose --profile app up -d --build receiver
java -jar ../load-generator/target/load-generator.jar --tps 300 --duration 20s --warmup 10s --seed 1
```

## Motor de detecção: latência fim a fim a 300 TPS
Medida pelo histograma `engine_e2e_latency_seconds` do motor (do `receivedAt`, quando o receiver aceita a transação, até a
decisão publicada), lido no Prometheus (`histogram_quantile` sobre `increase` na janela da rodada). Mesmo ambiente do restante
deste documento, com receiver, Kafka, Redis, Prometheus e **uma** instância do motor (1 GB) no Docker e o gerador no host.
Meta: p99 ≤ 500 ms, com meta interna de ~225 ms.

| Rodada | Motor | p50 | p90 | p99 | p99,9 | Decisões ≤ 0,5 s |
|---|---|---|---|---|---|---|
| 300 TPS, 10 s de aquecimento + 20 s | recém-iniciado (JIT frio) | 35 ms | 343 ms | **925 ms** | 992 ms | 94 % |
| 300 TPS, 20 s, sem aquecimento | **aquecido** | 20 ms | 83 ms | **230 ms** | 250 ms | 100 % |

- **O número que vale é o do motor aquecido: p99 de 230 ms**, dentro da meta de 500 ms e perto da meta interna (225 ms). O p99
  de 925 ms da primeira linha é partida a frio (o JIT do motor ainda esquentando nos primeiros segundos), do mesmo jeito que o
  p99 do receiver no primeiro degrau da escada acima; nessa linha a janela inclui o aquecimento.
- A mesma rodada aquecida teve o receiver com p99 HTTP de 1,2 s e 7 respostas 503, sem relação com o motor: é a pressão sobre
  o Windows e o Docker (limite de portas e CPU compartilhada). A rodada anterior, com o receiver em 176 ms de p99, deu
  p99 do motor de 925 ms por causa do frio. Com máquinas separadas essa competição por CPU deixaria de existir, mas isso não foi medido.
- **Ensaio com o motor de pé: o p99 de ~2,2 s era artefato do produtor de teste.** O `kafka-console-producer` segura cada lote por `linger.ms=1000` e a latência é medida desde o timestamp do registro. Com `linger.ms=0`, só tráfego ao vivo a ~200 comandos/s, motor aquecido (INFO, 1 GiB): p50 7 ms, p90 23 ms, p99 88 ms (`infra/engine-latency.sh`). Pico de CPU durante o envio: motor 43%, Kafka 288%. Uma rodada, longe dos 8.000 TPS.
- **Uma rodada que colapsa contamina a seguinte.** A primeira tentativa (60 s de medição + 15 s de aquecimento = 22 mil
  conexões) esgotou as portas efêmeras do Windows: 100 % de erros e ~15 mil transações que o Kafka gravou mesmo assim, depois
  de o receiver ter respondido timeout. O motor gastou minutos consumindo esse atraso, e a primeira medição depois dela deu
  p50 e p99 no último bucket (5 s); foi descartada. O limite local é de **cerca de 9 mil conexões por rodada**.
- **Não verificado agora (etapa futura, na nuvem):** 8.000 TPS por 5 min e o p99 fim a fim a 8.000 e 25.000 TPS, que
  exigiriam máquinas separadas (várias instâncias do motor, partições e disco de produção), e o impacto de um rebalance durante a carga.

### Como reproduzir
```bash
(cd ../detection-engine && ./mvnw -q -DskipTests package) && (cd ../receiver && ./mvnw -q -DskipTests package)
docker compose up -d kafka kafka-init redis prometheus
RECEIVER_MEM_LIMIT=1g docker compose --profile app up -d --build receiver detection-engine
java -jar ../load-generator/target/load-generator.jar --tps 300 --duration 20s --warmup 10s --seed 1 --accepted-log aceitas.log
# p99 do motor na janela da rodada (substitua 59 pela duração da janela em segundos)
curl -s -G localhost:9090/api/v1/query --data-urlencode 'query=histogram_quantile(0.99, sum by (le) (increase(engine_e2e_latency_seconds_bucket[59s])))'
```
