package br.com.motorfraude.contracts;

public record RuleHit(String ruleId, int version, int weight, String reason) {}
