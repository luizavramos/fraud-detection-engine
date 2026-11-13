package br.com.motorfraude.contracts;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Toda decisão do motor, com ou sem alerta (fraud-decisions.v1). features: escalares curtos, sem dado pessoal. */
public record FraudDecision(
        String decisionId,
        String transactionId,
        String accountId,
        Decision decision,
        int score,
        Severity severity,
        List<RuleHit> triggeredRules,
        List<RuleHit> shadowHits,
        Map<String, Object> features,
        long ruleSetVersion,
        ContextCompleteness contextCompleteness,
        Instant eventReceivedAt,
        Instant decidedAt) {}
