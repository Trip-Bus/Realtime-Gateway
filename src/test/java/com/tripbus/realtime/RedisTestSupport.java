package com.tripbus.realtime;

import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;

/** Redis real (Testcontainers) para probar los almacenes en Redis sin levantar Spring. */
public final class RedisTestSupport {

    private RedisTestSupport() {
    }

    public static GenericContainer<?> container() {
        return new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    }

    /** Un StringRedisTemplate conectado al contenedor; cada llamada es como otra réplica. */
    public static StringRedisTemplate template(GenericContainer<?> redis) {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379)));
        factory.afterPropertiesSet();
        factory.start();
        StringRedisTemplate template = new StringRedisTemplate(factory);
        template.afterPropertiesSet();
        return template;
    }
}
