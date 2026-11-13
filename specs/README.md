# Specs — Spec-Driven Development

Cada pasta é uma unidade de implementação com três documentos:

| Documento | Responde | Quem usa |
|---|---|---|
| `spec.md` | **O quê e por quê**: histórias de usuário, critérios de aceite, requisitos | Quem valida se está certo |
| `plan.md` | **Como**: arquitetura, contratos, dados, decisões técnicas | Quem implementa |
| `tasks.md` | **Em que ordem**: tarefas pequenas, testáveis, com caminhos de arquivo | Quem executa (pessoa ou IA) |

Regras comuns: [constitution.md](constitution.md).

## Ordem de implementação

| # | Unidade | Depende de | Prioridade |
|---|---|---|---|
| 000 | [Plataforma e contratos](000-plataforma-e-contratos/spec.md) | — | Fundação |
| 001 | [Receiver](001-receiver/spec.md) | 000 | P1 |
| 002 | [Gestão de regras](002-rule-management/spec.md) | 000 | P1 |
| 003 | [Customer service](003-customer-service/spec.md) | 000 | P1 |
| 004 | [Motor de detecção](004-detection-engine/spec.md) | 000, 002 (API), 003 (tópico) | P1 — núcleo |
| 005 | [Notificações](005-notification-service/spec.md) | 000, 004 (tópico de alertas) | P1 |
| 006 | [Transaction service](006-transaction-service/spec.md) | 000, 004 (consumidor do backfill) | P2 |

001, 002 e 003 podem ser feitos em paralelo depois da 000. **Exceção:** o gerador de carga (000, Fase 5 / H4) fica para depois do checkpoint da Fase 3 da 001 e antes da T122 da 001, porque só pode ser validado com o receiver no ar. A 004 começa usando dados de teste no lugar de 002 e 003 e se integra a eles quando ficarem prontos.

## Marcadores usados

- **[NEEDS CLARIFICATION]:** decisão ainda aberta. A spec registra o padrão adotado até alguém decidir.
- **[DOC]:** previsto na arquitetura, mas não implementado no take-home.
- **[P]** (em `tasks.md`): a tarefa pode rodar em paralelo com as outras [P] da mesma fase, porque mexe em arquivos diferentes.

## Mapa com os desenhos

A numeração dos passos (1, 1.1 …) é a mesma de [arquitetura-aws.drawio](../arquitetura-aws.drawio) e [fluxo-bpmn-sequencia.drawio](../fluxo-bpmn-sequencia.drawio).
