package br.com.motorfraude.receiver.adapters.in.rest;

import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.receiver.application.ReceiveTransactionUseCase;
import br.com.motorfraude.receiver.domain.InvalidTransactionException;
import br.com.motorfraude.receiver.domain.ReceiveResult;
import br.com.motorfraude.receiver.domain.TransactionCommand;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;

/**
 * Entrada HTTP do receiver (spec 001, plan §4). Ordem: origem → corpo (até 16 KB) → contrato → invariantes de domínio
 * → caso de uso. Erros seguem o formato Problem Details (RFC 9457).
 */
@RestController
public class TransactionController {

    static final int MAX_BODY_BYTES = 16 * 1024;

    private final ReceiveTransactionUseCase useCase;
    private final RequestValidator validator;
    private final AllowedClients allowedClients;
    private final Clock clock;
    private final TraceContextResolver traceResolver;

    public TransactionController(
            ReceiveTransactionUseCase useCase,
            RequestValidator validator,
            AllowedClients allowedClients,
            Clock clock,
            TraceContextResolver traceResolver) {
        this.useCase = useCase;
        this.validator = validator;
        this.allowedClients = allowedClients;
        this.clock = clock;
        this.traceResolver = traceResolver;
    }

    @PostMapping(path = "/v1/transactions:evaluate", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> evaluate(
            HttpServletRequest request,
            @RequestHeader(name = "X-Client-Id", required = false) String clientId,
            @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestHeader(name = "traceparent", required = false) String traceparent)
            throws IOException {

        if (!allowedClients.contains(clientId)) {
            return problem(HttpStatus.UNAUTHORIZED, "Origem não autorizada", "X-Client-Id ausente ou desconhecido", List.of());
        }

        var body = readBody(request);
        if (body == null) {
            return problem(HttpStatus.PAYLOAD_TOO_LARGE, "Corpo grande demais",
                    "O corpo deve ter no máximo " + MAX_BODY_BYTES + " bytes", List.of());
        }

        var errors = new ArrayList<String>();
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            errors.add("Idempotency-Key: header obrigatório");
        }

        JsonNode tree;
        try {
            tree = JsonCodec.readTree(body);
        } catch (JacksonException e) {
            errors.add("corpo: não é um JSON válido");
            return invalid(errors);
        }

        errors.addAll(validator.validate(tree));
        var bodyTransactionId = tree.path("transactionId");
        if (idempotencyKey != null && bodyTransactionId.isString() && !idempotencyKey.equals(bodyTransactionId.asString())) {
            errors.add("Idempotency-Key: deve ser igual ao transactionId do corpo");
        }
        if (!errors.isEmpty()) {
            return invalid(errors);
        }

        TransactionCommand command;
        try {
            command = TransactionCommand.of(JsonCodec.fromTree(tree, EvaluateTransaction.class), clock.instant());
        } catch (InvalidTransactionException e) {
            return invalid(e.violations());
        } catch (JacksonException e) {
            return invalid(List.of("corpo: não corresponde ao contrato"));
        }

        var result = useCase.execute(command, clientId, traceResolver.resolve(traceparent));
        return respond(result);
    }

    /** Lê no máximo o limite + 1 byte: nunca carrega um corpo gigante na memória. Nulo se excedeu. */
    private static byte[] readBody(HttpServletRequest request) throws IOException {
        if (request.getContentLengthLong() > MAX_BODY_BYTES) {
            return null;
        }
        var bytes = request.getInputStream().readNBytes(MAX_BODY_BYTES + 1);
        return bytes.length > MAX_BODY_BYTES ? null : bytes;
    }

    private ResponseEntity<Map<String, Object>> respond(ReceiveResult result) {
        return switch (result) {
            case ReceiveResult.Accepted accepted -> accepted(accepted.transactionId(), "ACCEPTED");
            case ReceiveResult.Duplicate duplicate -> accepted(duplicate.transactionId(), "DUPLICATE");
            case ReceiveResult.RateLimited limited ->
                retryLater(HttpStatus.TOO_MANY_REQUESTS, "Limite de requisições excedido",
                        "A origem excedeu o limite de requisições; tente novamente");
            case ReceiveResult.InFlight inFlight ->
                retryLater(HttpStatus.SERVICE_UNAVAILABLE, "Transação em andamento",
                        "A mesma transação ainda está sendo processada; tente novamente");
            case ReceiveResult.Unavailable unavailable ->
                retryLater(HttpStatus.SERVICE_UNAVAILABLE, "Serviço indisponível",
                        "A transação não foi aceita; tente novamente");
            case ReceiveResult.Rejected rejected -> invalid(rejected.errors());
        };
    }

    private static ResponseEntity<Map<String, Object>> accepted(String transactionId, String status) {
        var payload = new LinkedHashMap<String, Object>();
        payload.put("transactionId", transactionId);
        payload.put("status", status);
        return ResponseEntity.status(HttpStatus.ACCEPTED).contentType(MediaType.APPLICATION_JSON).body(payload);
    }

    private static ResponseEntity<Map<String, Object>> invalid(List<String> errors) {
        return problem(HttpStatus.BAD_REQUEST, "Requisição inválida", "O corpo ou os headers não respeitam o contrato", errors);
    }

    private static ResponseEntity<Map<String, Object>> retryLater(HttpStatus status, String title, String detail) {
        var response = problem(status, title, detail, List.of());
        var headers = new HttpHeaders();
        headers.putAll(response.getHeaders());
        headers.set(HttpHeaders.RETRY_AFTER, "1");
        return new ResponseEntity<>(response.getBody(), headers, status);
    }

    private static ResponseEntity<Map<String, Object>> problem(
            HttpStatus status, String title, String detail, List<String> errors) {
        var body = new LinkedHashMap<String, Object>();
        body.put("type", "about:blank");
        body.put("title", title);
        body.put("status", status.value());
        body.put("detail", detail);
        if (!errors.isEmpty()) {
            body.put("errors", errors);
        }
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_PROBLEM_JSON).body(body);
    }
}
