package br.com.motorfraude.contracts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.JsonNode;

/** T015 — ida e volta record → JSON → validação → record, para cada contrato, e comportamento do JsonCodec. */
class ContractRoundTripTest {

    static Stream<Arguments> contractTypes() {
        return Stream.of(
                Arguments.of("evaluate-transaction.v1", EvaluateTransaction.class),
                Arguments.of("customer-profile.v1", CustomerProfile.class),
                Arguments.of("transaction-history-record.v1", TransactionHistoryRecord.class),
                Arguments.of("blocklist-entry.v1", BlocklistEntry.class),
                Arguments.of("fraud-decision.v1", FraudDecision.class),
                Arguments.of("fraud-alert.v1", FraudAlert.class),
                Arguments.of("rule-set.v1", RuleSet.class));
    }

    @Test
    @DisplayName("todo contrato conhecido tem record no teste de ida e volta")
    void todosOsContratosCobertos() {
        var covered = contractTypes().map(a -> (String) a.get()[0]).toList();

        assertThat(covered).containsExactlyInAnyOrderElementsOf(Schemas.names());
    }

    @ParameterizedTest(name = "{0}: exemplo → record → JSON → válido → mesmo record")
    @MethodSource("contractTypes")
    void idaEVolta(String contract, Class<?> type) {
        JsonNode original = SchemasTest.example(contract);

        Object record = JsonCodec.fromTree(original, type);
        JsonNode serialized = JsonCodec.toTree(record);

        assertThat(Schemas.validate(contract, serialized).errors()).as("JSON gerado a partir do record").isEmpty();
        assertThat(JsonCodec.fromTree(serialized, type)).as("record lido de volta").isEqualTo(record);
        assertThat(Set.copyOf(serialized.propertyNames()))
                .as("nenhum campo do exemplo se perdeu")
                .containsAll(Set.copyOf(original.propertyNames()));
    }

    @ParameterizedTest(name = "{0}: texto JSON → record → texto JSON é estável")
    @MethodSource("contractTypes")
    void textoEstavel(String contract, Class<?> type) {
        Object first = JsonCodec.fromJson(SchemasTest.example(contract).toString(), type);
        String text = JsonCodec.toJson(first);

        assertThat(JsonCodec.toJson(JsonCodec.fromJson(text, type))).isEqualTo(text);
    }

    // ---------------------------------------------------------------- JsonCodec

    @Test
    @DisplayName("dinheiro sai como inteiro em centavos, nunca como decimal")
    void dinheiroSaiInteiro() {
        var tx = sampleTransaction(null);

        assertThat(JsonCodec.toJson(tx)).contains("\"value\":15000").doesNotContain("15000.0");
    }

    @Test
    @DisplayName("instantes saem em ISO-8601 UTC com Z")
    void instantesEmUtc() {
        var tx = sampleTransaction("dev_0f3a9c1d5b7e2a64");

        assertThat(JsonCodec.toJson(tx)).contains("\"occurredAt\":\"2026-11-13T21:30:00Z\"");
    }

    @Test
    @DisplayName("campo opcional nulo é omitido (deviceIdHash ausente, não null)")
    void opcionalNuloEOmitido() {
        var tx = sampleTransaction(null);
        JsonNode tree = JsonCodec.toTree(tx);

        assertThat(tree.has("deviceIdHash")).isFalse();
        assertThat(Schemas.validate("evaluate-transaction.v1", tree).errors()).isEmpty();
    }

    @Test
    @DisplayName("consumidor tolera campo novo (evolução compatível)")
    void consumidorToleraCampoNovo() {
        JsonNode tree = SchemasTest.example("fraud-alert.v1").deepCopy();
        ((tools.jackson.databind.node.ObjectNode) tree).put("campoDaVersaoFutura", "x");

        FraudAlert alert = JsonCodec.fromTree(tree, FraudAlert.class);

        assertThat(alert.alertId()).isEqualTo("7c9e6679-7425-5a1e-b2f1-3f1d8e6a9c10");
    }

    @Test
    @DisplayName("record montado em código gera JSON válido (CustomerProfile sem credencial alterada)")
    void recordMontadoEmCodigo() {
        var profile = new CustomerProfile(
                "acc_tok_00000007",
                Segment.PJ,
                Instant.parse("2024-01-15T10:00:00Z"),
                null,
                List.of("dev_aa11bb22cc33dd44"),
                Instant.parse("2026-11-13T21:00:00Z"));

        ContractAssertions.assertProduces("customer-profile.v1", profile);
    }

    // ---------------------------------------------------------------- ContractAssertions

    @Test
    @DisplayName("assertProduces aceita record, JsonNode e texto válidos")
    void assertProducesAceitaVarias() {
        var alert = JsonCodec.fromTree(SchemasTest.example("fraud-alert.v1"), FraudAlert.class);

        ContractAssertions.assertProduces("fraud-alert.v1", alert);
        ContractAssertions.assertProduces("fraud-alert.v1", SchemasTest.example("fraud-alert.v1"));
        ContractAssertions.assertProduces("fraud-alert.v1", SchemasTest.example("fraud-alert.v1").toString());
        ContractAssertions.assertProduces("fraud-alert.v1", SchemasTest.example("fraud-alert.v1").toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("assertProduces falha e mostra o campo quando a mensagem viola o contrato")
    void assertProducesFalhaComCampo() {
        var decision = JsonCodec.fromTree(SchemasTest.example("fraud-decision.v1"), FraudDecision.class);
        var features = new java.util.HashMap<>(decision.features());
        features.put("Nome Completo", "Maria Silva");
        var vazando = new FraudDecision(decision.decisionId(), decision.transactionId(), decision.accountId(),
                decision.decision(), decision.score(), decision.severity(), decision.triggeredRules(),
                decision.shadowHits(), features, decision.ruleSetVersion(), decision.contextCompleteness(),
                decision.eventReceivedAt(), decision.decidedAt());

        assertThatThrownBy(() -> ContractAssertions.assertProduces("fraud-decision.v1", vazando))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("fraud-decision.v1")
                .hasMessageContaining("Nome Completo");
    }

    private static EvaluateTransaction sampleTransaction(String deviceIdHash) {
        return new EvaluateTransaction(
                1,
                "tx_20261113_0001",
                "acc_tok_00000001",
                "cus_tok_00000001",
                TransactionType.PIX,
                new Money(15000, "BRL"),
                new Counterparty("acc_tok_00004711", "60701190"),
                Channel.MOBILE,
                deviceIdHash,
                Instant.parse("2026-11-13T21:30:00Z"));
    }
}
