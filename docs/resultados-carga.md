# Resultados de carga

Medições do receiver (spec 001) com o gerador de carga (spec 000, Fase 5). O motor (spec 004) acrescenta as suas.

## Escala local x nuvem
O case pede 8.000 TPS de média e 25.000 de pico. **Isso só se reproduz de verdade na nuvem**, com receiver, Kafka e gerador
em máquinas separadas. Localmente tudo divide os mesmos núcleos (o gerador chega a gastar 3,4 núcleos), então o que se mede
aqui é o comportamento numa **escala de referência de 300 TPS**, com 500 TPS como teto confortável.

**Não verificado neste notebook:** 8.000 TPS sustentados por 5 min (SC-003 da spec 000), p99 do receiver ≤ 30 ms a
8.000 TPS (SC-001 da spec 001) e os 25.000 TPS de pico. Ficam para a execução na nuvem.

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
   dentro de `synchronized` prende o núcleo de execução. Vale reavaliar na nuvem, sem o gerador competindo pela CPU.
4. **`503` não prova que nada foi gravado** (ver plano da spec 001, §8): o Kafka pode gravar depois do timeout.
5. **Partida a frio pesa.** O p99 do primeiro degrau (2,4 s) contra 71 a 103 ms nos seguintes. Por isso `--warmup`.

## Como reproduzir
```bash
./mvnw -q -pl load-generator -am package -DskipTests
RECEIVER_MEM_LIMIT=1g docker compose --profile app up -d --build receiver
java -jar load-generator/target/load-generator.jar --tps 300 --duration 20s --warmup 10s --seed 1
```
