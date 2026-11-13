package br.com.motorfraude.contracts;

import java.time.Instant;

/** Entrada da lista de bloqueio (blocklist.v1); valor nulo no tópico significa remoção. */
public record BlocklistEntry(String entityToken, EntityType entityType, String reason, Instant listedAt) {}
