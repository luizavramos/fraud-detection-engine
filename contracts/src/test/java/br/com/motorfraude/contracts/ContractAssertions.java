package br.com.motorfraude.contracts;

import static org.assertj.core.api.Assertions.assertThat;

import tools.jackson.databind.JsonNode;

/**
 * Teste de contrato do produtor. Publicado no test-jar de {@code contracts}: todo módulo que produz mensagens
 * (001, 002, 003, 004, 006) valida nos seus testes de saída que o que sai respeita o schema estrito.
 *
 * <pre>{@code ContractAssertions.assertProduces("customer-profile.v1", profileEmitido);}</pre>
 */
public final class ContractAssertions {

    private ContractAssertions() {}

    /**
     * Falha (AssertionError) se a mensagem violar o schema. Aceita record, {@link JsonNode}, texto ou bytes JSON.
     * A mensagem de erro traz o nome do contrato e o caminho de cada campo inválido.
     */
    public static void assertProduces(String schemaName, Object message) {
        JsonNode node = switch (message) {
            case JsonNode n -> n;
            case String s -> JsonCodec.readTree(s);
            case byte[] b -> JsonCodec.readTree(b);
            default -> JsonCodec.toTree(message);
        };
        ValidationResult result = Schemas.validate(schemaName, node);
        assertThat(result.errors())
                .as("A mensagem produzida viola o contrato %s", schemaName)
                .isEmpty();
    }
}
