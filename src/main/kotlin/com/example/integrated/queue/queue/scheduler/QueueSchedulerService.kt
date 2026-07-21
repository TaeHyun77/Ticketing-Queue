package com.example.integrated.queue.queue.scheduler

import com.example.integrated.queue.queue.dto.QueueChangePayload
import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.util.ACTIVE_QUEUE_KEY
import com.example.integrated.util.ALLOW_QUEUE
import com.example.integrated.util.CHANNEL_NAME
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
        private val redisPublisher: RedisPublisher,
        private val objectMapper: ObjectMapper
) : Loggable {

    companion object {
        // Consumer용 : 중복 체크 + score 생성 + 참가열/대기열 삽입을 원자적으로 수행, 단일 정수 반환
        private val ENQUEUE_OR_ALLOW_SCRIPT: RedisScript<Long> = RedisScript.of(
                ClassPathResource("scripts/enqueue-or-allow.lua"),
                Long::class.java
        )

        // 스케줄러용 : 만료 정리 + 빈 자리 계산 + 승격을 원자적으로 수행 ( JSON 문자열 반환 )
        private val SCHEDULE_PROMOTE_SCRIPT: RedisScript<String> = RedisScript.of(
                ClassPathResource("scripts/schedule-promote.lua"),
                String::class.java
        )

        // 취소용 : wait → allow 순서로 ZREM 원자적 처리, 위치 문자열 반환
        private val CANCEL_USER_SCRIPT: RedisScript<String> = RedisScript.of(
                ClassPathResource("scripts/cancel-user.lua"),
                String::class.java
        )

        const val EVENT_PROMOTE = "promote"
    }

    // 스케줄러 : 만료 정리 + 빈 자리 계산 + 승격을 원자적으로 처리
    suspend fun promoteUsers(queueType: String): Long {
        val nowMs = System.currentTimeMillis()
        val expireAt = nowMs + tokenTtlMs

        val keys = listOf(
                "$queueType$ALLOW_QUEUE",   // KEYS[1] : 참가열 키
                "$queueType$WAIT_QUEUE",    // KEYS[2] : 대기열 키
                ACTIVE_QUEUE_KEY            // KEYS[3] : 활성 큐 레지스트리
        )

        val args = listOf(
                maxCapacity.toString(),     // ARGV[1] : 참가열 최대 수용 인원
                nowMs.toString(),           // ARGV[2] : 현재 시각 ( 만료 판단 )
                expireAt.toString(),        // ARGV[3] : 참가열 score
                queueType                   // ARGV[4] : 레지스트리 멤버
        )

        val raw = reactiveRedisTemplate.execute(SCHEDULE_PROMOTE_SCRIPT, keys, args)
                .next()
                .awaitSingle()

        val result = objectMapper.readValue<PromoteResult>(raw)

        if (result.count > 0) {
            val payload = objectMapper.writeValueAsString(
                    QueueChangePayload(
                            queueType = queueType,
                            event = EVENT_PROMOTE,
                            ids = result.ids
                    )
            )
            redisPublisher.publish(CHANNEL_NAME, payload)
        }

        return result.count
    }

    /**
     * 중복 체크 + score 생성 + 대기열/참가열 삽입을 원자적으로 처리
     * 반환: -1 (대기열 존재), -2 (참가열 존재), 0 (대기열 삽입), 1 (참가열 삽입)
     */
    suspend fun enqueueOrAllow(
            queueType: String,
            userId: String
    ): Long {
        val nowMs = System.currentTimeMillis()
        val expireAt = nowMs + tokenTtlMs

        val keys = listOf(
                "$queueType$ALLOW_QUEUE",       // KEYS[1] : 참가열 키
                "$queueType$WAIT_QUEUE",        // KEYS[2] : 대기열 키
                "queue:seq:$queueType",         // KEYS[3] : 이벤트별 시퀀스 카운터 키 ( score 생성용 )
                ACTIVE_QUEUE_KEY                // KEYS[4] : 활성 큐 레지스트리
        )

        val args = listOf(
                userId,                         // ARGV[1] : userId
                maxCapacity.toString(),         // ARGV[2] : 참가열 최대 수용 인원
                nowMs.toString(),               // ARGV[3] : 현재 시각
                expireAt.toString(),            // ARGV[4] : 참가열 score
                queueType                       // ARGV[5] : 레지스트리 멤버
        )

        return reactiveRedisTemplate.execute(ENQUEUE_OR_ALLOW_SCRIPT, keys, args)
                .next()
                .awaitSingle()
    }

    // 취소 원자적 처리 : wait → allow 순서로 ZREM, 어느 쪽에서 제거됐는지 반환
    // 반환 : 'wait' | 'allow' | 'none'
    suspend fun cancelUser(queueType: String, userId: String): String {
        val keys = listOf(
                "$queueType$WAIT_QUEUE",    // KEYS[1] : 대기열 키
                "$queueType$ALLOW_QUEUE",   // KEYS[2] : 참가열 키
                ACTIVE_QUEUE_KEY           // KEYS[3] : 활성 큐 레지스트리
        )

        val args = listOf(userId, queueType)   // ARGV[1] : userId, ARGV[2] : 레지스트리 멤버

        return reactiveRedisTemplate.execute(CANCEL_USER_SCRIPT, keys, args)
                .next()
                .awaitSingle()
    }

    private data class PromoteResult(val count: Long, val ids: List<String>)
}