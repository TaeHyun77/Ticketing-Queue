package com.example.integrated.redis

import com.example.integrated.util.Loggable
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.withContext
import org.springframework.core.io.ClassPathResource
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.script.RedisScript
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.coroutines.cancellation.CancellationException

// SET NX PX 기반 단발 락 유틸.
// 정합성은 승격 Lua의 원자성이 보장하므로, 이 락은 "이번 틱을 단독 서버에서만 실행"시켜
// N개 서버의 중복 EVAL 부하를 억제하기 위한 용도다.
@Component
class RedisLockUtil(
        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>
) : Loggable {

    companion object {
        // lease 만료 후 타 서버가 획득한 락을 오삭제하지 않도록, 소유 토큰이 일치할 때만 DEL 한다.
        private val RELEASE_SCRIPT: RedisScript<Long> = RedisScript.of(
                ClassPathResource("scripts/release-lock.lua"),
                Long::class.java
        )
    }

    // 락 획득에 성공하면 block 을 실행하고, 실패 시 대기 없이 즉시 null 을 반환한다.
    // leaseTime 은 SET 의 PX(만료 시간)로 쓰인다. block 이 이를 초과하면 락이 풀려 이중 실행이 가능하나,
    // 승격 로직이 멱등이라 무해하다.
    suspend fun <T> acquireLockAndRun(
            key: String,
            leaseTime: Long,
            timeUnit: TimeUnit = TimeUnit.MILLISECONDS,
            block: suspend () -> T
    ): T? {
        // 해제 시 소유권 검증에 사용할 획득 단위 고유 토큰
        val token = UUID.randomUUID().toString()

        val acquired = try {
            reactiveRedisTemplate.opsForValue()
                    .setIfAbsent(key, token, Duration.ofMillis(timeUnit.toMillis(leaseTime)))
                    .awaitSingleOrNull() ?: false
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error { "Lock 획득 중 예외 발생 - key: $key, ${e.message}" }
            return null
        }

        if (!acquired) {
            log.debug { "Lock 획득 실패 - key: $key" }
            return null
        }

        try {
            return block()
        } finally {
            releaseLock(key, token)
        }
    }

    // 코루틴이 취소되어도 락은 해제되도록 NonCancellable 로 감싼다.
    private suspend fun releaseLock(key: String, token: String) {
        withContext(NonCancellable) {
            try {
                reactiveRedisTemplate.execute(RELEASE_SCRIPT, listOf(key), listOf(token))
                        .next()
                        .awaitSingleOrNull()
            } catch (e: Exception) {
                log.warn { "Lock 해제 실패 - key: $key, ${e.message}" }
            }
        }
    }
}
