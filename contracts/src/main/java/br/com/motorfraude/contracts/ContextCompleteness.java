package br.com.motorfraude.contracts;

/** PARTIAL quando faltou contexto (ex.: perfil do cliente ausente) e regras dependentes não puderam disparar. */
public enum ContextCompleteness { FULL, PARTIAL }
