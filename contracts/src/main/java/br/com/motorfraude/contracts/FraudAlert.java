package br.com.motorfraude.contracts;

import java.time.Instant;
import java.util.List;

/** Alerta MEDIUM/HIGH, motor -> notificações (fraud-alerts.v1). alertId = UUIDv5(transactionId). */
public record FraudAlert(
        String alertId,
        String transactionId,
        String accountId,
        Severity severity,
        int score,
        List<RuleHit> triggeredRules,
        long ruleSetVersion,
        ContextCompleteness contextCompleteness,
        Instant eventReceivedAt,
        Instant detectedAt,
        String traceId) {}
