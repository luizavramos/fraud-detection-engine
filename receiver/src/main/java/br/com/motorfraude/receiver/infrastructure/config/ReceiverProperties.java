package br.com.motorfraude.receiver.infrastructure.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuração do receiver, toda sobrescrevível por variável de ambiente (ex.: {@code RECEIVER_KAFKA_BOOTSTRAP_SERVERS},
 * {@code RECEIVER_CLIENTS}). Valores locais em {@code application.yml}.
 *
 * @param clients origens autorizadas (FR-007a)
 */
@ConfigurationProperties(prefix = "receiver")
public record ReceiverProperties(List<String> clients, Kafka kafka) {

    /**
     * @param bootstrapServers brokers
     * @param topic tópico de comandos
     * @param publishTimeout quanto esperar o ack do Kafka antes de responder 503
     */
    public record Kafka(String bootstrapServers, String topic, Duration publishTimeout) {}
}
