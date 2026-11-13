package br.com.motorfraude.contracts;

import java.time.Instant;
import java.util.List;

/** Perfil sem dado pessoal, customer-service -> motor (customer-profile.v1). Contrato: schemas/customer-profile.v1.json. */
public record CustomerProfile(
        String accountId,
        Segment segment,
        Instant accountOpenedAt,
        Instant lastCredentialChangeAt,
        List<String> knownDeviceHashes,
        Instant updatedAt) {}
