package br.com.motorfraude.contracts;

/** Dinheiro em centavos (nunca double/float) e moeda ISO-4217. */
public record Money(long value, String currency) {}
