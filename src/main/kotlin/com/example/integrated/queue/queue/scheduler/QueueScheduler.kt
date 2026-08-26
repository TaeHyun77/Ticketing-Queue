package com.example.integrated.queue.queue.scheduler

import com.example.integrated.util.ACTIVE_QUEUE_KEY
import com.example.integrated.util.Loggable
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class QueueScheduler(
        private val queueSchedulerService: QueueSchedulerService,
        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
) : Loggable {

    @Scheduled(
            fixedDelayString = "\${queue.allow.interval-ms}",
            initialDelay = 5000
    )
    suspend fun scheduling() {
        val knownQueues = getKnownQueues()
        if (knownQueues.isEmpty()) return

        knownQueues.forEach { queueType ->
            try {
                val count = queueSchedulerService.promoteUsers(queueType)
                if (count > 0L) {
                    log.info { "$queueType 허용열로 이동한 사용자 : $count" }
                }
            } catch (e: Exception) {
                log.error(e) { "스케줄링 중 예외 발생 - ${e.message}" }
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
}
