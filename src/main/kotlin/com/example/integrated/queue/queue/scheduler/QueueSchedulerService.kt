package com.example.integrated.queue.queue.scheduler

import com.example.integrated.queue.queue.dto.QueueChangePayload
import com.example.integrated.queue.sse.SseEventService
import com.example.integrated.util.ACTIVE_QUEUE_KEY
import com.example.integrated.util.ADMITTED_COUNTER_KEY_PREFIX
import com.example.integrated.util.ALLOW_QUEUE
import com.example.integrated.util.WAIT_QUEUE
import com.example.integrated.util.Loggable
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.ClassPathResource
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.script.RedisScript
import org.springframework.stereotype.Service

@Service
class QueueSchedulerService(
        @Value("\${queue.allow.max-capacity}")
        private val maxCapacity: Long,

        @Value("\${queue.allow.token-ttl-ms}")
        private val tokenTtlMs: Long,

        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
        private val objectMapper: ObjectMapper
) : Loggable {

    companion object {
        private val ENQUEUE_OR_ALLOW_SCRIPT: RedisScript<Long> = RedisScript.of(
                ClassPathResource("scripts/enqueue-or-allow.lua"),
                Long::class.java
        )

        private val SCHEDULE_PROMOTE_SCRIPT: RedisScript<String> = RedisScript.of(
                ClassPathResource("scripts/schedule-promote.lua"),
                String::class.java
        )

        private val CANCEL_USER_SCRIPT: RedisScript<String> = RedisScript.of(
                ClassPathResource("scripts/cancel-user.lua"),
                String::class.java
        )

        private val MOVE_TO_TAIL_SCRIPT: RedisScript<Long> = RedisScript.of(
                ClassPathResource("scripts/move-to-tail.lua"),
                Long::class.java
        )

        const val EVENT_PROMOTE = "promote"
    }

    suspend fun promoteUsers(queueType: String): Long {
        val nowMs = System.currentTimeMillis()
        val expireAt = nowMs + tokenTtlMs

        val keys = listOf(
                "$queueType$ALLOW_QUEUE",
                "$queueType$WAIT_QUEUE",
                ACTIVE_QUEUE_KEY,
                "$ADMITTED_COUNTER_KEY_PREFIX$queueType"
        )

        val args = listOf(
                maxCapacity.toString(),
                nowMs.toString(),
                expireAt.toString(),
                queueType
        )

        val raw = reactiveRedisTemplate.execute(SCHEDULE_PROMOTE_SCRIPT, keys, args)
                .next()
                .awaitSingle()

        val result = objectMapper.readValue<PromoteResult>(raw)

        if (result.count > 0) {
            SseEventService.emit(
                QueueChangePayload(
                    queueType = queueType,
                    event = EVENT_PROMOTE,
                    ids = result.ids,
                    admittedThrough = result.admittedThrough
                )
            )
        }

        return result.count
    }

    suspend fun enqueueOrAllow(
            queueType: String,
            userId: String
    ): Long {
        val nowMs = System.currentTimeMillis()
        val expireAt = nowMs + tokenTtlMs

        val keys = listOf(
                "$queueType$ALLOW_QUEUE",
                "$queueType$WAIT_QUEUE",
                "queue:seq:$queueType",
                ACTIVE_QUEUE_KEY
        )

        val args = listOf(
                userId,
                maxCapacity.toString(),
                nowMs.toString(),
                expireAt.toString(),
                queueType
        )

        return reactiveRedisTemplate.execute(ENQUEUE_OR_ALLOW_SCRIPT, keys, args)
                .next()
                .awaitSingle()
    }

    suspend fun cancelUser(queueType: String, userId: String): String {
        val keys = listOf(
                "$queueType$WAIT_QUEUE",
                "$queueType$ALLOW_QUEUE",
                ACTIVE_QUEUE_KEY
        )

        val args = listOf(userId, queueType)

        return reactiveRedisTemplate.execute(CANCEL_USER_SCRIPT, keys, args)
                .next()
                .awaitSingle()
    }

    suspend fun moveToTail(queueType: String, userId: String): Long {
        val keys = listOf(
                "$queueType$WAIT_QUEUE",
                "queue:seq:$queueType"
        )

        val args = listOf(userId)

        return reactiveRedisTemplate.execute(MOVE_TO_TAIL_SCRIPT, keys, args)
                .next()
                .awaitSingle()
    }

    private data class PromoteResult(val count: Long, val ids: List<String>, val admittedThrough: Long = 0)
}
