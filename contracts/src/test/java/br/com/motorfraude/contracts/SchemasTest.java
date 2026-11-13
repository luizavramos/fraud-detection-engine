package br.com.motorfraude.contracts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * T010 — todo contrato tem ao menos um exemplo válido e vários inválidos (SC-002).
 * Os schemas são estritos (additionalProperties=false em todos os níveis) e dinheiro só aceita inteiro em centavos.
 */
class SchemasTest {

    static final List<String> CONTRACTS = List.of(
            "evaluate-transaction.v1",
            "customer-profile.v1",
            "transaction-history-record.v1",
            "blocklist-entry.v1",
            "fraud-decision.v1",
            "fraud-alert.v1",
            "rule-set.v1");

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    static ObjectNode example(String contract) {
        try (InputStream in = SchemasTest.class.getResourceAsStream("/examples/" + contract + ".json")) {
            assertThat(in).as("exemplo /examples/%s.json", contract).isNotNull();
            return (ObjectNode) MAPPER.readTree(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static ObjectNode obj(ObjectNode root, String pointer) {
        return (ObjectNode) root.at(pointer);
    }

    private static ArrayNode arr(ObjectNode root, String pointer) {
        return (ArrayNode) root.at(pointer);
    }

    private record Case(String contract, String why, Consumer<ObjectNode> mutation) {
        Arguments args() {
            return Arguments.of(contract, why, mutation);
        }
    }

    private static Case valid(String contract, String why, Consumer<ObjectNode> m) {
        return new Case(contract, why, m);
    }

    // ---------------------------------------------------------------- exemplos oficiais

    @ParameterizedTest(name = "exemplo oficial de {0} é válido")
    @MethodSource("contracts")
    void exemploOficialPassa(String contract) {
        var result = Schemas.validate(contract, example(contract));

        assertThat(result.errors()).isEmpty();
        assertThat(result.valid()).isTrue();
    }

    static Stream<String> contracts() {
        return CONTRACTS.stream();
    }

    @Test
    @DisplayName("todo contrato da lista tem schema carregável")
    void todosOsSchemasExistem() {
        assertThat(Schemas.names()).containsExactlyInAnyOrderElementsOf(CONTRACTS);
    }

    @Test
    @DisplayName("nome de schema desconhecido é erro de programação")
    void schemaDesconhecido() {
        assertThatThrownBy(() -> Schemas.validate("nao-existe.v1", example("fraud-alert.v1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nao-existe.v1");
    }

    // ---------------------------------------------------------------- H2.2 e H2.3: dinheiro e dado pessoal

    @Test
    @DisplayName("H2.2: amount.value decimal (10.5) falha")
    void valorDecimalFalha() {
        var tx = example("evaluate-transaction.v1");
        obj(tx, "/amount").put("value", 10.5);

        assertThat(Schemas.validate("evaluate-transaction.v1", tx).valid()).isFalse();
    }

    @Test
    @DisplayName("H2.3: campo cpf na raiz falha")
    void cpfNaRaizFalha() {
        var tx = example("evaluate-transaction.v1");
        tx.put("cpf", "123.456.789-09");

        var result = Schemas.validate("evaluate-transaction.v1", tx);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).anyMatch(e -> e.contains("cpf"));
    }

    @Test
    @DisplayName("H2.3: campo cpf dentro de counterparty falha (estrito em todos os níveis)")
    void cpfAninhadoFalha() {
        var tx = example("evaluate-transaction.v1");
        obj(tx, "/counterparty").put("cpf", "123.456.789-09");

        assertThat(Schemas.validate("evaluate-transaction.v1", tx).valid()).isFalse();
    }

    @Test
    @DisplayName("Draft 2020-12: 10.0 é inteiro válido; 10.5 não")
    void decimalComParteFracionariaZeroEhInteiro() {
        var tx = example("evaluate-transaction.v1");
        obj(tx, "/amount").put("value", 10.0);

        assertThat(Schemas.validate("evaluate-transaction.v1", tx).valid()).isTrue();
    }

    @Test
    @DisplayName("limite superior: 2^53-1 passa; 2^53 falha")
    void limiteDeInteiroExatoEmJson() {
        var tx = example("evaluate-transaction.v1");

        obj(tx, "/amount").put("value", 9_007_199_254_740_991L);
        assertThat(Schemas.validate("evaluate-transaction.v1", tx).valid()).isTrue();

        obj(tx, "/amount").put("value", 9_007_199_254_740_992L);
        assertThat(Schemas.validate("evaluate-transaction.v1", tx).valid()).isFalse();
    }

    // ---------------------------------------------------------------- variações válidas

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("validVariants")
    void variacoesValidasPassam(String contract, String why, Consumer<ObjectNode> mutation) {
        var node = example(contract);
        mutation.accept(node);

        var result = Schemas.validate(contract, node);

        assertThat(result.errors()).as(why).isEmpty();
    }

    static Stream<Arguments> validVariants() {
        return Stream.of(
                valid("evaluate-transaction.v1", "deviceIdHash é opcional", n -> n.remove("deviceIdHash")),
                valid("evaluate-transaction.v1", "instante com milissegundos", n -> n.put("occurredAt", "2026-11-13T21:30:00.123Z")),
                valid("customer-profile.v1", "lastCredentialChangeAt nulo", n -> n.putNull("lastCredentialChangeAt")),
                valid("customer-profile.v1", "lastCredentialChangeAt ausente", n -> n.remove("lastCredentialChangeAt")),
                valid("customer-profile.v1", "sem dispositivos conhecidos", n -> arr(n, "/knownDeviceHashes").removeAll()),
                valid("customer-profile.v1", "segmento PJ", n -> n.put("segment", "PJ")),
                valid("blocklist-entry.v1", "entidade DEVICE", n -> n.put("entityType", "DEVICE").put("entityToken", "dev_0f3a9c1d5b7e2a64")),
                valid("fraud-decision.v1", "NO_ALERT sem regras", n -> {
                    n.put("decision", "NO_ALERT").put("severity", "NONE").put("score", 0);
                    arr(n, "/triggeredRules").removeAll();
                    arr(n, "/shadowHits").removeAll();
                }),
                valid("fraud-decision.v1", "contexto PARTIAL", n -> n.put("contextCompleteness", "PARTIAL")),
                valid("fraud-alert.v1", "severidade MEDIUM", n -> n.put("severity", "MEDIUM").put("score", 45)),
                valid("rule-set.v1", "conjunto sem regras", n -> arr(n, "/rules").removeAll()),
                valid("rule-set.v1", "param em array de escalares", n ->
                        obj(n, "/rules/0/params").set("allowedTypes", MAPPER.createArrayNode().add("PIX").add("TED"))))
                .map(c -> c.args());
    }

    // ---------------------------------------------------------------- variações inválidas

    @ParameterizedTest(name = "{0}: {1} falha")
    @MethodSource("invalidVariants")
    void variacoesInvalidasFalham(String contract, String why, Consumer<ObjectNode> mutation) {
        var node = example(contract);
        mutation.accept(node);

        var result = Schemas.validate(contract, node);

        assertThat(result.valid()).as(why).isFalse();
        assertThat(result.errors()).as("mensagens de erro para %s", why).isNotEmpty();
    }

    static Stream<Arguments> invalidVariants() {
        return Stream.of(
                // evaluate-transaction
                valid("evaluate-transaction.v1", "valor zero", n -> obj(n, "/amount").put("value", 0)),
                valid("evaluate-transaction.v1", "valor negativo", n -> obj(n, "/amount").put("value", -100)),
                valid("evaluate-transaction.v1", "valor como texto", n -> obj(n, "/amount").put("value", "15000")),
                valid("evaluate-transaction.v1", "moeda diferente de BRL", n -> obj(n, "/amount").put("currency", "USD")),
                valid("evaluate-transaction.v1", "campo extra em amount", n -> obj(n, "/amount").put("decimals", 2)),
                valid("evaluate-transaction.v1", "sem transactionId", n -> n.remove("transactionId")),
                valid("evaluate-transaction.v1", "sem amount", n -> n.remove("amount")),
                valid("evaluate-transaction.v1", "accountId que não é token", n -> n.put("accountId", "12345-6")),
                valid("evaluate-transaction.v1", "customerId que não é token", n -> n.put("customerId", "maria.silva")),
                valid("evaluate-transaction.v1", "e-mail no lugar do beneficiário", n -> obj(n, "/counterparty").put("accountToken", "maria@exemplo.com")),
                valid("evaluate-transaction.v1", "bankIspb com 7 dígitos", n -> obj(n, "/counterparty").put("bankIspb", "6070119")),
                valid("evaluate-transaction.v1", "tipo desconhecido", n -> n.put("type", "CRIPTO")),
                valid("evaluate-transaction.v1", "canal desconhecido", n -> n.put("channel", "TELEPATIA")),
                valid("evaluate-transaction.v1", "deviceIdHash que não é token", n -> n.put("deviceIdHash", "iPhone de Maria")),
                valid("evaluate-transaction.v1", "instante com fuso diferente de Z", n -> n.put("occurredAt", "2026-11-13T18:30:00-03:00")),
                valid("evaluate-transaction.v1", "instante sem fuso", n -> n.put("occurredAt", "2026-11-13T21:30:00")),
                valid("evaluate-transaction.v1", "schemaVersion 2", n -> n.put("schemaVersion", 2)),
                valid("evaluate-transaction.v1", "e-mail em campo extra", n -> n.put("email", "maria@exemplo.com")),
                // customer-profile
                valid("customer-profile.v1", "coluna de dado pessoal vazada (email)", n -> n.put("email", "maria@exemplo.com")),
                valid("customer-profile.v1", "coluna de dado pessoal vazada (cpf)", n -> n.put("cpf", "123.456.789-09")),
                valid("customer-profile.v1", "11 dispositivos (máximo 10)", n -> {
                    for (int i = 0; i < 9; i++) {
                        arr(n, "/knownDeviceHashes").add("dev_ffffffff0000000" + i);
                    }
                }),
                valid("customer-profile.v1", "dispositivo que não é token", n -> arr(n, "/knownDeviceHashes").add("meu celular")),
                valid("customer-profile.v1", "segmento desconhecido", n -> n.put("segment", "PX")),
                valid("customer-profile.v1", "sem updatedAt", n -> n.remove("updatedAt")),
                valid("customer-profile.v1", "sem accountId", n -> n.remove("accountId")),
                // transaction-history-record
                valid("transaction-history-record.v1", "beneficiário em texto livre", n -> n.put("counterpartyToken", "Joao da Silva")),
                valid("transaction-history-record.v1", "nome do pagador vazado", n -> n.put("payer_name", "Maria")),
                valid("transaction-history-record.v1", "cpf do pagador vazado", n -> n.put("payer_cpf", "123.456.789-09")),
                valid("transaction-history-record.v1", "valor decimal", n -> obj(n, "/amount").put("value", 89.9)),
                valid("transaction-history-record.v1", "sem occurredAt", n -> n.remove("occurredAt")),
                // blocklist-entry
                valid("blocklist-entry.v1", "tipo de entidade desconhecido", n -> n.put("entityType", "PESSOA")),
                valid("blocklist-entry.v1", "token que não é de conta nem de dispositivo", n -> n.put("entityToken", "123.456.789-09")),
                valid("blocklist-entry.v1", "motivo em texto livre", n -> n.put("reason", "Maria Silva foi fraudadora")),
                valid("blocklist-entry.v1", "sem listedAt", n -> n.remove("listedAt")),
                valid("blocklist-entry.v1", "campo extra", n -> n.put("owner", "Maria")),
                // fraud-decision
                valid("fraud-decision.v1", "decisionId que não é UUID", n -> n.put("decisionId", "decisao-1")),
                valid("fraud-decision.v1", "decisão desconhecida", n -> n.put("decision", "BLOQUEAR")),
                valid("fraud-decision.v1", "severidade desconhecida", n -> n.put("severity", "CRITICAL")),
                valid("fraud-decision.v1", "score negativo", n -> n.put("score", -1)),
                valid("fraud-decision.v1", "completude desconhecida", n -> n.put("contextCompleteness", "MAIS_OU_MENOS")),
                valid("fraud-decision.v1", "feature com nome fora do padrão", n -> obj(n, "/features").put("Nome Completo", 1)),
                valid("fraud-decision.v1", "feature com chave iniciada por maiúscula", n -> obj(n, "/features").put("Cliente", 1)),
                valid("fraud-decision.v1", "feature com texto acima de 64 caracteres", n -> obj(n, "/features").put("observacao", "x".repeat(65))),
                valid("fraud-decision.v1", "feature com objeto aninhado", n -> obj(n, "/features").set("detalhe", MAPPER.createObjectNode().put("a", 1))),
                valid("fraud-decision.v1", "feature com array", n -> obj(n, "/features").set("lista", MAPPER.createArrayNode().add(1))),
                valid("fraud-decision.v1", "regra disparada sem motivo", n -> obj(n, "/triggeredRules/0").remove("reason")),
                valid("fraud-decision.v1", "campo extra na regra disparada", n -> obj(n, "/triggeredRules/0").put("ownerEmail", "a@b.com")),
                valid("fraud-decision.v1", "campo extra na raiz", n -> n.put("customerName", "Maria")),
                valid("fraud-decision.v1", "sem features", n -> n.remove("features")),
                // fraud-alert
                valid("fraud-alert.v1", "alertId que não é UUID", n -> n.put("alertId", "alerta-1")),
                valid("fraud-alert.v1", "alertId UUID v4 (precisa ser v5 de transactionId)", n -> n.put("alertId", "7c9e6679-7425-4a1e-b2f1-3f1d8e6a9c10")),
                valid("fraud-alert.v1", "severidade NONE (alerta só MEDIUM ou HIGH)", n -> n.put("severity", "NONE")),
                valid("fraud-alert.v1", "alerta sem regras disparadas", n -> arr(n, "/triggeredRules").removeAll()),
                valid("fraud-alert.v1", "traceId inválido", n -> n.put("traceId", "trace-1")),
                valid("fraud-alert.v1", "sem detectedAt", n -> n.remove("detectedAt")),
                valid("fraud-alert.v1", "campo extra", n -> n.put("phone", "+5511999990000")),
                // rule-set
                valid("rule-set.v1", "versão zero", n -> n.put("version", 0)),
                valid("rule-set.v1", "sem scoring", n -> n.remove("scoring")),
                valid("rule-set.v1", "limiar zero", n -> obj(n, "/scoring").put("mediumThreshold", 0)),
                valid("rule-set.v1", "regra em DRAFT (só ACTIVE e SHADOW entram no conjunto)", n -> obj(n, "/rules/0").put("status", "DRAFT")),
                valid("rule-set.v1", "regra sem expressão", n -> obj(n, "/rules/0").remove("expression")),
                valid("rule-set.v1", "expressão vazia", n -> obj(n, "/rules/0").put("expression", "")),
                valid("rule-set.v1", "override para segmento inexistente", n -> obj(n, "/rules/0/paramOverrides").set("XX", MAPPER.createObjectNode().put("multiplier", 1))),
                valid("rule-set.v1", "param com chave fora do padrão", n -> obj(n, "/rules/0/params").put("Max Count", 5)),
                valid("rule-set.v1", "param com objeto aninhado", n -> obj(n, "/rules/0/params").set("x", MAPPER.createObjectNode())),
                valid("rule-set.v1", "campo extra na regra", n -> obj(n, "/rules/0").put("author", "maria")),
                valid("rule-set.v1", "campo extra na raiz", n -> n.put("approvedBy", "joao")))
                .map(c -> c.args());
    }
}
