package br.com.motorfraude.contracts;

import java.time.Instant;

/** Histórico de 90 dias, transaction-service -> motor (transaction-history-backfill.v1). */
public record TransactionHistoryRecord(
        String transactionId,
        String accountId,
        Money amount,
        String counterpartyToken,
        String deviceIdHash,
        Instant occurredAt) {}
