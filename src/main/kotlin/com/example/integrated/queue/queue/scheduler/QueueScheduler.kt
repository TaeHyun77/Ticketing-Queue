package com.example.integrated.queue.queue.scheduler

import com.example.integrated.redis.RedisLockUtil
import com.example.integrated.util.ACTIVE_QUEUE_KEY
import com.example.integrated.util.Loggable
import com.example.integrated.util.SCHEDULING_KEY
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class QueueScheduler(
        private val queueSchedulerService: QueueSchedulerService,
        private val redisLockUtil: RedisLockUtil,
        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
) : Loggable {

    // "이 시스템이 시작된 이후로 한 번이라도 사용자가 진입한 적 있는 큐" 는 모두 스케줄링 대상
    @Scheduled(
            fixedDelayString = "\${queue.allow.interval-ms}",
            initialDelay = 5000
    )
    suspend fun scheduling() {
        val knownQueues = getKnownQueues()
        if (knownQueues.isEmpty()) return

        redisLockUtil.acquireLockAndRun(SCHEDULING_KEY) {
            knownQueues.forEach { queueType ->
                try {
                    // Lua 스크립트 하나로 만료 정리 + 빈 자리 계산 + 승격을 원자적으로 처리
                    // 빈 큐는 Lua 내부에서 즉시 count=0 으로 리턴되므로 별도 정리 분기 불필요
                    val count = queueSchedulerService.promoteUsers(queueType)
                    if (count > 0L) {
                        log.info { "$queueType 허용열로 이동한 사용자 : $count" }
                    }
                } catch (e: Exception) {
                    log.error(e) { "스케줄링 중 예외 발생 - ${e.message}" }
                }
            }
        }
    }

    suspend fun getKnownQueues(): List<String> {
        return reactiveRedisTemplate.opsForSet()
                .members(ACTIVE_QUEUE_KEY)
                .collectList()
                .awaitSingleOrNull()
                ?: emptyList()
    }

    suspend fun addActiveQueue(queueType: String) {
        reactiveRedisTemplate.opsForSet()
                .add(ACTIVE_QUEUE_KEY, queueType)
                .awaitSingle()
    }
}