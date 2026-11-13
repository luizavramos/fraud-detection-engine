package br.com.motorfraude.contracts;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Conjunto ativo de regras, gestão de regras -> motor (GET /v1/rule-sets/active). */
public record RuleSet(long version, Instant publishedAt, Scoring scoring, List<Rule> rules) {

    public record Scoring(int mediumThreshold, int highThreshold) {}

    /** params e paramOverrides são mapas de escalares (ou listas de escalares); a chave do override é o segmento (PF/PJ). */
    public record Rule(
            String ruleId,
            int version,
            RuleStatus status,
            String expression,
            Map<String, Object> params,
            Map<String, Map<String, Object>> paramOverrides,
            int weight,
            String reasonTemplate) {}
}
