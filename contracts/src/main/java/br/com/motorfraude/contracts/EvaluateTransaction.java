package br.com.motorfraude.contracts;

import java.time.Instant;

/** Comando receiver -> motor (transaction-commands.v1). Contrato: schemas/evaluate-transaction.v1.json. */
public record EvaluateTransaction(
        int schemaVersion,
        String transactionId,
        String accountId,
        String customerId,
        TransactionType type,
        Money amount,
        Counterparty counterparty,
        Channel channel,
        String deviceIdHash,
        Instant occurredAt) {}
