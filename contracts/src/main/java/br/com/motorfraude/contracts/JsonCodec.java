package br.com.motorfraude.contracts;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Codec JSON único dos contratos (Jackson 3).
 *
 * <ul>
 *   <li>Leitura tolerante a campos novos ({@code FAIL_ON_UNKNOWN_PROPERTIES=false}): consumidores não quebram com
 *       evolução compatível do contrato.
 *   <li>Escrita: campos nulos omitidos, instantes em ISO-8601 UTC, mapas ordenados por chave (saída estável).
 *   <li>Dinheiro é {@code long} (centavos): nunca sai como decimal.
 * </ul>
 */
public final class JsonCodec {

    private static final JsonMapper MAPPER = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .changeDefaultPropertyInclusion(v -> v.withValueInclusion(JsonInclude.Include.NON_NULL))
            .build();

    private JsonCodec() {}

    public static JsonMapper mapper() {
        return MAPPER;
    }

    public static String toJson(Object value) {
        return MAPPER.writeValueAsString(value);
    }

    public static byte[] toBytes(Object value) {
        return MAPPER.writeValueAsBytes(value);
    }

    public static JsonNode toTree(Object value) {
        return MAPPER.valueToTree(value);
    }

    public static <T> T fromJson(String json, Class<T> type) {
        return MAPPER.readValue(json, type);
    }

    public static <T> T fromJson(byte[] json, Class<T> type) {
        return MAPPER.readValue(json, type);
    }

    public static <T> T fromTree(JsonNode tree, Class<T> type) {
        return MAPPER.treeToValue(tree, type);
    }

    public static JsonNode readTree(String json) {
        return MAPPER.readTree(json);
    }

    public static JsonNode readTree(byte[] json) {
        return MAPPER.readTree(json);
    }
}
