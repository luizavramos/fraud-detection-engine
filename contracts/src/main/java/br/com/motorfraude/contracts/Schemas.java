package br.com.motorfraude.contracts;

import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import tools.jackson.databind.JsonNode;

/**
 * Validação das mensagens contra os JSON Schemas versionados em {@code /schemas}.
 *
 * <p>Em produção só o receiver valida (fronteira HTTP). Os produtores internos validam nos testes de contrato
 * ({@code ContractAssertions}); consumidores leem de forma tolerante.
 */
public final class Schemas {

    private static final List<String> NAMES = List.of(
            "evaluate-transaction.v1",
            "customer-profile.v1",
            "transaction-history-record.v1",
            "blocklist-entry.v1",
            "fraud-decision.v1",
            "fraud-alert.v1",
            "rule-set.v1");

    private static final SchemaRegistry REGISTRY = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
    private static final Map<String, Schema> COMPILED = new ConcurrentHashMap<>();

    private Schemas() {}

    /** Nomes dos contratos conhecidos, sem extensão (ex.: {@code evaluate-transaction.v1}). */
    public static List<String> names() {
        return NAMES;
    }

    /**
     * @throws IllegalArgumentException se o nome não for de um contrato conhecido (erro de programação)
     */
    public static ValidationResult validate(String schemaName, JsonNode message) {
        var errors = schema(schemaName).validate(message).stream().map(e -> e.getMessage()).toList();
        return new ValidationResult(errors);
    }

    private static Schema schema(String name) {
        if (!NAMES.contains(name)) {
            throw new IllegalArgumentException("Contrato desconhecido: " + name + " (conhecidos: " + NAMES + ")");
        }
        return COMPILED.computeIfAbsent(name, Schemas::load);
    }

    private static Schema load(String name) {
        try (InputStream in = Schemas.class.getResourceAsStream("/schemas/" + name + ".json")) {
            if (in == null) {
                throw new IllegalStateException("Schema ausente no classpath: /schemas/" + name + ".json");
            }
            return REGISTRY.getSchema(in, InputFormat.JSON);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
