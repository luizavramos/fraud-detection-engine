package br.com.motorfraude.receiver.infrastructure.config;

import br.com.motorfraude.receiver.application.port.out.DuplicateGuard;
import br.com.motorfraude.receiver.application.port.out.RateLimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * TEMPORÁRIO (Fase 3): portas que ainda não têm adapter real, para o serviço já aceitar e publicar (checkpoint da
 * Fase 3). Sem Redis o receiver trabalha em fail-open; sem rate limit ele não limita.
 *
 * <p>REMOVER: o {@code DuplicateGuard} na T114 (RedisDuplicateGuard) e o {@code RateLimiter} na T118 (Bucket4j).
 */
@Configuration
class TemporaryPortsConfig {

    @Bean
    DuplicateGuard duplicateGuard() {
        return new DuplicateGuard() {
            @Override
            public Reservation tryReserve(String transactionId) {
                return Reservation.UNAVAILABLE;
            }

            @Override
            public void confirm(String transactionId) {}

            @Override
            public void release(String transactionId) {}
        };
    }

    @Bean
    RateLimiter rateLimiter() {
        return clientId -> true;
    }
}
