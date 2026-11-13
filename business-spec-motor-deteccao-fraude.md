# Spec de Negócio — Motor de Detecção de Transações Suspeitas em Tempo Real

> **Origem:** Case técnico do processo seletivo (Eng. de Software Backend), take-home de 7 dias + apresentação de 1h (30 min de apresentação, 30 min de discussão).
> **Uso deste documento:** entrada para o Technical Refiner. Itens marcados com **[H]** são **hipóteses** assumidas por mim (o case não define); devem ser declaradas como premissas na apresentação.

---

## 0. Leitura do case: o que realmente está sendo avaliado

Antes do negócio, vale deixar claro o "meta-objetivo", porque ele guia todas as priorizações:

| O case pede | O que o avaliador quer ver |
|---|---|
| "Desenvolver **ou propor**" a solução | Profundidade de raciocínio > volume de código. Implementação parcial é aceita, **desde que as simplificações sejam explicadas** |
| 8k TPS médio / 25k pico / alerta em ≤ 500 ms | Capacidade de dimensionar, provar (teste de carga) e discutir onde está o gargalo |
| Idempotência, resiliência, segurança/LGPD, observabilidade | Maturidade de engenharia de sistemas financeiros críticos |
| Regras configuráveis sem redeploy | Design de domínio e extensibilidade (não só infraestrutura) |
| Uso de IA explícito | Transparência: onde a IA ajudou, onde o candidato decidiu e validou |

**Entregáveis obrigatórios:** README com instruções de execução · código-fonte e/ou documentação arquitetural · diagramas · estratégia de testes · justificativa de decisões e trade-offs · limitações e evoluções · registro do uso de IA.

**Habilidades avaliadas (checklist para cobrir na apresentação):** estruturas de dados e algoritmos · linguagens · mensageria e eventos · containers · cloud · modelagem/armazenamento/governança de dados · segurança · deploy · APIs e integração · escalabilidade · latência e vazão · testes · resiliência · observabilidade · engenharia de software.

---

## 1. Problema / oportunidade

**Problema.** Fraudes em transações financeiras (especialmente em meios instantâneos como Pix) acontecem e se consumam em segundos. Detecção em batch (D+1) ou mesmo em minutos chega tarde: o dinheiro já saiu e foi pulverizado em contas de passagem ("laranjas"). Cada minuto de atraso reduz drasticamente a chance de recuperação.

**O que o banco quer.** Um módulo que:
1. Consuma eventos de transação publicados por outros serviços internos, em tempo real;
2. Avalie cada evento contra regras de detecção;
3. Emita alertas para canais **internos** (equipe antifraude, sistemas antifraude existentes — fila/tópico/REST) e **externos** (cliente final — push/e-mail);
4. Continue operando quando dependências auxiliares estiverem indisponíveis.

**Por que agora.**
- Pagamentos instantâneos 24/7 elevaram a velocidade e o volume dos golpes (engenharia social, conta-laranja, sequestro de conta).
- Pressão regulatória crescente sobre os bancos em segurança de pagamentos instantâneos (mecanismos de devolução e compartilhamento de marcações de fraude no ecossistema Pix) e responsabilização da instituição pelo dano ao cliente.
- O custo reputacional e financeiro (ressarcimentos, judicialização) de uma fraude não detectada supera em muito o custo de infraestrutura de detecção.

**Escopo explícito do case (e o que fica fora).**
- **Dentro:** detecção *pós-recebimento do evento* com geração de alerta em ≤ 500 ms.
- **[H] Fora (mas citar como evolução):** bloqueio síncrono da transação no caminho de autorização. O case fala em "alertas", não em "aprovar/negar". Isso muda tudo: o motor **não está no caminho crítico da transação**, então a arquitetura assíncrona orientada a eventos é legítima. Vale dizer isso explicitamente na apresentação, porque é a decisão que habilita o resto.

---

## 2. Proposta de valor

> **Transformar cada transação em uma decisão de risco em menos de meio segundo, com regras que o time antifraude muda sozinho, em minutos, sem depender de deploy — e sem perder nenhum evento nem duplicar nenhum alerta.**

Os quatro pilares que diferenciam esta solução de um "consumidor de fila com `if`s":

1. **Velocidade com garantia** — p99 ≤ 500 ms do recebimento ao alerta emitido, a 25k TPS.
2. **Autonomia do time antifraude** — regras, thresholds e composições versionadas e publicadas em runtime (com validação, *shadow mode* e rollback).
3. **Confiabilidade financeira** — processamento efetivamente único por transação; nenhum alerta duplicado ao cliente; nenhum evento perdido (degrada, mas não descarta).
4. **Operabilidade** — SRE enxerga throughput, latência, lag, taxa de alertas e falsos positivos por regra, e tem runbooks para os incidentes previstos.

---

## 3. Público / personas

| Persona | Necessidade | Urgência | O que define "bom" para ela |
|---|---|---|---|
| **Cliente final** (PF/PJ) | Ser avisado imediatamente de atividade suspeita para agir (contestar, bloquear cartão/conta) | Altíssima — segundos importam | Alerta rápido, claro, **uma única vez**, sem excesso de falsos alarmes |
| **Analista antifraude** | Receber alertas priorizados com contexto suficiente para decidir | Alta | Alertas acionáveis, com motivo (qual regra, que evidência), pouca fila de falso positivo |
| **Gestor/estrategista de fraude** | Criar e ajustar regras rapidamente frente a novos padrões de golpe | Alta | Publicar regra nova em minutos, medir impacto antes de ativar, desfazer com segurança |
| **SRE / Operações** | Garantir disponibilidade e performance; responder incidentes | Alta | Dashboards e alertas claros, runbooks, degradação previsível |
| **Segurança da Informação / DPO (LGPD)** | Proteção de dados pessoais e transacionais | Contínua | Criptografia, minimização, mascaramento, trilha de auditoria, retenção definida |
| **Sistemas internos** (core, antifraude legado, notificação, cadastro de clientes) | Integração estável e com contrato claro | Contínua | Contratos versionados, tolerância a falhas, sem acoplamento temporal |
| **Auditoria / Compliance** | Explicar *por que* uma transação foi (ou não) sinalizada | Média | Rastreabilidade: evento → regras avaliadas (com versão) → decisão → alerta |

---

## 4. Panorama competitivo e de aprendizado

O objetivo aqui não é "quem compete", e sim **que padrões de mercado validam (ou questionam) as escolhas da solução**.

| Referência | O que faz bem | Onde tropeça / limitação | Lição para esta solução |
|---|---|---|---|
| **Feedzai** (plataforma de risco para bancos) | ML + regras em tempo real, com foco em explicabilidade para o analista | Plataforma pesada e cara; customização depende do fornecedor | **Regras + score coexistem**; explicar o "porquê" de cada alerta é requisito, não luxo |
| **Featurespace (ARIC)** | *Behavioral analytics*: perfila comportamento "normal" de cada cliente e detecta desvios | Exige muito histórico e infraestrutura de features | Regras de **desvio do perfil do próprio cliente** são mais precisas que thresholds globais → justifica um *feature store* de agregados por cliente |
| **Stripe Radar** | Regras declarativas editáveis pelo usuário (linguagem simples) sobre sinais pré-calculados; modo de teste das regras antes de ativar | Contexto de e-commerce, menos aplicável a transferências | **DSL de regras + backtest/shadow mode** antes de ativar — modelo direto para "regras sem redeploy" |
| **SEON / Sift** | Enriquecimento de sinais (device, e-mail, IP) + regras configuráveis | Dependência de dados de terceiros (latência, disponibilidade) | Enriquecimento externo **não pode estar no caminho crítico sem timeout/fallback** |
| **Ecossistema Pix (BCB): marcações de fraude e mecanismo de devolução** | Compartilhamento de sinais de fraude entre instituições; devolução de valores | Depende de cada banco reagir rápido | Integração com bases de fraude compartilhadas é **caminho de expansão** natural; velocidade de detecção é o que viabiliza a devolução |
| **Bancos digitais (ex.: Nubank, Revolut)** | Notificação push instantânea em toda transação + ação de "não fui eu" no próprio app | Fadiga de notificação se mal calibrado | O alerta ao cliente é **canal de detecção também** (feedback "não fui eu" = rótulo de fraude) |
| **Motores de regras genéricos** (Drools, Easy Rules, OPA, CEL) | Avaliação de expressões configuráveis maduras | Drools é pesado; Rete pode ser excessivo para regras simples | Para latência baixa: **expressões compiladas e cacheadas** (ex.: CEL/SpEL/JSONLogic) costumam bastar; avaliar trade-off na spec técnica |
| **Streaming (Kafka Streams / Flink)** | Padrão de mercado para agregações em janela (velocidade, contagem por intervalo) com estado local | Complexidade operacional (Flink) | Regras de **velocidade/janela** (ex.: "N transações em X minutos") precisam de estado → decisão-chave da spec técnica |

**Padrão de mercado que emerge:** pipeline orientado a eventos → enriquecimento com contexto do cliente (cache) → avaliação de regras configuráveis (+ score opcional) → decisão explicável → roteamento de alertas desacoplado por canal → feedback loop para calibrar.

---

## 5. Diferencial (o que vai destacar esta entrega no processo seletivo)

A maioria dos candidatos vai entregar "Kafka + consumidor + regras em `if` + Redis para idempotência". Para se destacar:

1. **Separar claramente idempotência de evento vs. deduplicação de alerta** — são problemas diferentes (o mesmo evento reprocessado; e eventos distintos que geram o "mesmo" alerta para o cliente). Poucos fazem essa distinção.
2. **Regras como dados, com ciclo de vida completo:** rascunho → validação → *shadow mode* (avalia, loga, não alerta) → ativa → rollback; versão da regra registrada em cada decisão (auditoria).
3. **Orçamento de latência explícito** — decompor os 500 ms por etapa (ingestão, enriquecimento, regras, publicação do alerta) e mostrar medição real (teste de carga) contra o orçamento.
4. **Degradação elegante documentada por dependência** — tabela "se X cai, o sistema faz Y" (ex.: cadastro de clientes fora → usa cache/regras que não precisam do dado e marca decisão como "contexto parcial"; push fora → outbox + retry, e o alerta interno segue).
5. **Métrica de negócio no monitoramento**, não só técnica — taxa de alertas por regra, desvio vs. baseline, % de "não fui eu" (proxy de verdadeiro positivo) — permite detectar "aumento súbito de falsos positivos", um dos incidentes citados no case.
6. **Transparência no uso de IA** — log de onde foi usada (brainstorm, código boilerplate, testes, revisão) e o que foi decidido/validado pelo candidato.

---

## 6. Impacto esperado

### Para o cliente
- **Curto prazo:** aviso em segundos de atividade suspeita → janela real para reagir (bloquear, contestar).
- **Longo prazo:** confiança no banco; menos perdas; menos fricção (bons alertas e poucos falsos positivos).

### Para o negócio
- **Redução de perda financeira** por fraude (detecção mais cedo = maior taxa de recuperação).
- **Eficiência operacional** do time antifraude (alertas priorizados e explicáveis).
- **Time-to-market contra novos golpes:** de semanas (ciclo de deploy) para minutos (regra publicada em runtime).
- **Conformidade** (LGPD, regulação de segurança de pagamentos) e trilha de auditoria.
- **Plataforma reutilizável:** a mesma infraestrutura serve para outros domínios de risco (ver expansão).

**Ordem de grandeza [H]:** 8k TPS médios ≈ **~690 milhões de eventos/dia**. Se 0,1% gera alerta → ~690 mil alertas/dia, ~8 alertas/s médios e ~25/s em pico. Isso mostra que o gargalo é **avaliar** tudo rápido, não **notificar** — e que a taxa de alerta precisa ser monitorada (um bug de regra pode multiplicar alertas por 100 e derrubar canais de notificação).

---

## 7. Caminhos de expansão (versões mais ambiciosas)

Para citar na seção "evoluções" da apresentação — mostra visão além do case:

1. **De alerta para decisão inline:** oferecer endpoint síncrono de score de risco (p99 < 100 ms) para o fluxo de autorização bloquear/segurar transações de alto risco (com *fail-open* controlado).
2. **Score de ML ao lado das regras:** modelo servido em tempo real consumindo as mesmas features; regras e modelo combinados (ex.: "score > 0,9 **e** valor > X"). Feedback do cliente ("não fui eu") e do analista viram rótulos de treino.
3. **Feature store de comportamento por cliente:** horários típicos, valor médio, dispositivos e destinatários frequentes → regras de desvio de perfil (padrão Featurespace).
4. **Análise de grafo de contas:** detectar contas-laranja e redes de mula (muitas origens → um destino → pulverização).
5. **Integração com sinais do ecossistema:** consumir/publicar marcações de fraude compartilhadas entre instituições.
6. **Ação ativa no alerta ao cliente:** push com botões "fui eu / não fui eu" → resposta alimenta o motor e pode disparar bloqueio automático.
7. **Plataforma multi-domínio de risco:** mesmo motor para crédito (fraude de aplicação), abertura de conta, PLD/AML (lavagem de dinheiro), cartões.
8. **Simulação e backtest de regras** sobre histórico antes de publicar (impacto estimado de alertas e falsos positivos).

---

## 8. Riscos de negócio

| Risco | Impacto | Mitigação esperada na solução |
|---|---|---|
| **Excesso de falsos positivos** | Fadiga do cliente (ignora alertas reais), sobrecarga do time antifraude, custo de notificação | Shadow mode, métricas de taxa de alerta por regra com alarme de desvio, *rate-limit*/agrupamento de alertas por cliente |
| **Falso negativo (fraude não detectada)** | Perda financeira e reputacional | Regras compostas + perfil comportamental; feedback loop; evolução para ML |
| **Alerta duplicado ao cliente** | Confusão, perda de confiança | Deduplicação de alerta com chave determinística + idempotência na entrega |
| **Evento perdido** | Fraude passa sem avaliação; falha de auditoria | Log durável, commit de offset após processamento, DLQ, reconciliação |
| **Regra mal configurada publicada em produção** | Tempestade de alertas ou cegueira total | Validação de sintaxe/semântica, shadow mode, rollout gradual, kill switch, rollback de versão |
| **Vazamento de dados pessoais** | Sanção LGPD, dano reputacional | Minimização, tokenização/mascaramento, criptografia em trânsito e repouso, mTLS, controle de acesso, retenção definida |
| **Dependência indisponível** (cadastro, notificação, antifraude legado) | Atraso/perda de alertas | Circuit breaker, timeouts, cache, outbox + retry, degradação documentada |
| **Latência acima de 500 ms em pico** | Descumprimento do requisito; alerta tardio | Particionamento, escala horizontal, estado local, orçamento de latência por etapa, teste de carga |
| **Risco do case em si:** escopo grande para 7 dias | Entrega rasa em tudo | Priorizar: núcleo funcional executável + documentação profunda das partes não implementadas |

---

## 9. Critérios de sucesso

### Do produto (o que a solução precisa demonstrar)
| Critério | Meta |
|---|---|
| Vazão sustentada | ≥ 8.000 TPS; suportar picos de 25.000 TPS **[H: sem perda; lag recuperado em poucos minutos]** |
| Latência recebimento → alerta | **[H] p99 ≤ 500 ms** (o case diz "no máximo"; adotar p99 como SLO e discutir p99.9) |
| Processamento único | 0 alertas duplicados em testes de reentrega/falha (caos: kill de consumidor, rede instável) |
| Perda de eventos | 0 eventos perdidos; falhas irrecuperáveis vão para DLQ com rastreabilidade |
| Regras sem redeploy | Nova regra/threshold ativa sem restart (requisito do PDF). O tempo até valer **não está no PDF**: é premissa nossa, definida pelo tempo aceitável para desligar uma regra quebrada (kill switch) — padrão 30 s, configurável **[H]** |
| Resiliência | Sistema segue processando e gerando alertas internos com qualquer dependência auxiliar fora |
| Observabilidade | Métricas RED/USE, lag, latência por etapa, taxa de alerta por regra, tracing ponta a ponta |
| Segurança/LGPD | Criptografia em trânsito/repouso, autenticação entre serviços, dados sensíveis mascarados em logs |

### Do case (o que define uma boa entrega)
- Avaliador consegue **rodar** a solução pelo README (ex.: `docker compose up` + gerador de carga).
- Cada requisito não funcional do PDF tem **resposta explícita** (implementada ou proposta) com trade-off.
- Apresentação cabe em 30 min e segue os 4 blocos pedidos: contexto → decisões/trade-offs → qualidade/operação/evolução → estratégia ampliada.
- Limitações e simplificações declaradas com honestidade.
- Uso de IA documentado.

---

## 10. Premissas assumidas [H] (consolidado)

1. Os serviços internos de origem enviam cada transação por HTTP a uma porta de entrada do motor (decidido na spec técnica); o motor não precisa criar a ingestão a partir de sistemas legados.
2. O motor **gera alertas**; não bloqueia a transação de forma síncrona (bloqueio é evolução).
3. Cada evento tem um identificador único de transação estável entre reenvios.
4. "Alerta em no máximo 500 ms" é tratado como SLO de **p99**, medido do recebimento do evento pelo motor até a publicação do alerta no canal interno. Entrega ao cliente (push/e-mail) depende de provedores externos e tem SLO próprio, mais frouxo.
5. Taxa de transações suspeitas é baixa (~0,1–1%).
6. Dados de cliente (cadastro, perfil) podem ser cacheados com alguma defasagem tolerável.
7. Implementação de referência roda localmente (containers); a arquitetura alvo é descrita em cloud.

---

## 11. Perguntas em aberto (para levar ao Technical Refiner ou ao avaliador)

- Escopo de transações: só Pix, ou também TED, cartão, boleto? (afeta o modelo de evento e regras)
- O alerta interno deve alimentar um sistema antifraude existente com contrato definido, ou podemos propor o contrato?
- Há necessidade de o cliente receber o alerta mesmo quando a transação é dele (confirmação ativa)?
- Ordenação é necessária por cliente/conta? (regras de janela temporal normalmente exigem particionar por conta)
- Retenção de dados de decisão para auditoria: por quanto tempo?
- Qual a stack/linguagem preferida? (o case permite livre escolha — decisão do candidato, a ser justificada)

---

## 12. Próximo passo

Levar esta spec ao **Technical Refiner** para definir: arquitetura (ingestão, processamento com estado, motor de regras, roteamento de alertas), escolhas de tecnologia e linguagem, estratégia de idempotência/dedup, orçamento de latência, segurança ponta a ponta, observabilidade e runbooks, estratégia de testes (unitário, contrato, integração, carga, caos) e o recorte do que será **implementado vs. apenas documentado** dentro dos 7 dias.
