package br.com.motorfraude.contracts;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class SmokeTest {

    @Test
    void moduloCompilaComJackson3() {
        var mapper = JsonMapper.builder().build();

        assertThat(mapper.readTree("{\"ok\":true}").get("ok").asBoolean()).isTrue();
    }
}
