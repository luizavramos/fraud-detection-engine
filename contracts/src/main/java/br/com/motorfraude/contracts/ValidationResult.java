package br.com.motorfraude.contracts;

import java.util.List;

/** Resultado da validação de uma mensagem contra um schema. Cada erro já traz o caminho do campo (ex.: {@code $.amount.value}). */
public record ValidationResult(List<String> errors) {

    public ValidationResult {
        errors = List.copyOf(errors);
    }

    public boolean valid() {
        return errors.isEmpty();
    }
}
