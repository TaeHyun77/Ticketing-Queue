package com.example.integrated.redis.config

import com.example.integrated.util.Loggable
import io.lettuce.core.ClientOptions
import io.lettuce.core.TimeoutOptions
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.RedisSentinelConfiguration
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import java.time.Duration

@Configuration
class RedisConfig(
    @Value("\${spring.redis.sentinel.master}") val master: String,
    @Value("\${spring.redis.sentinel.nodes}") val sentinelNodes: String
): Loggable {

    @Bean
    fun lettuceConnectionFactory(): LettuceConnectionFactory {
        val sentinelConfig = RedisSentinelConfiguration().master(master)

        sentinelNodes.split(",").forEach {
            val (host, port) = it.trim().split(":")
            sentinelConfig.sentinel(host, port.toInt())
        }

        val clientConfig = LettuceClientConfiguration.builder()
            .clientOptions(
                ClientOptions.builder()
                    .autoReconnect(true)
                    .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                    .timeoutOptions(
                        TimeoutOptions.builder()
                            .fixedTimeout(Duration.ofSeconds(5))
                            .build()
                    )
                    .build()
            )
            .build()

        return LettuceConnectionFactory(sentinelConfig, clientConfig)
    }

    @Bean
    fun reactiveRedisTemplate(): ReactiveRedisTemplate<String, String> {
        val context = RedisSerializationContext
            .newSerializationContext<String, String>(StringRedisSerializer())
            .value(StringRedisSerializer())
            .build()

        return ReactiveRedisTemplate(lettuceConnectionFactory(), context)
    }

    @Bean
    fun listenerContainer(
        lettuceConnectionFactory: ReactiveRedisConnectionFactory
    ): ReactiveRedisMessageListenerContainer {
        return ReactiveRedisMessageListenerContainer(lettuceConnectionFactory)
    }
}
