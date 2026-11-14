package br.com.motorfraude.receiver.infrastructure.config;

import br.com.motorfraude.receiver.application.port.out.RateLimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * TEMPORÁRIO: porta que ainda não tem adapter real. Sem rate limit o receiver não limita ninguém.
 *
 * <p>REMOVER na T118 (Bucket4jRateLimiter).
 */
@Configuration
class TemporaryPortsConfig {

    @Bean
    RateLimiter rateLimiter() {
        return clientId -> true;
    }
}
