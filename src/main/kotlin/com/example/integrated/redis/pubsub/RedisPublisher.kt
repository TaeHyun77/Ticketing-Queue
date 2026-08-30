package com.example.integrated.redis.pubsub

import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component

@Component
class RedisPublisher(
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>
) {

    suspend fun publish(channel: String, message: String) {
        reactiveRedisTemplate.convertAndSend(channel, message)
            .awaitSingle()
    }
}
