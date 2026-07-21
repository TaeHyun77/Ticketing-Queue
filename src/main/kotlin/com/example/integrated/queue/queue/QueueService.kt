package com.example.integrated.queue.queue

import com.example.integrated.queue.queue.dto.QueueChangePayload
import com.example.integrated.queue.queue.dto.RegisterResult
import com.example.integrated.queue.queue.scheduler.QueueScheduler
import com.example.integrated.queue.queue.scheduler.QueueSchedulerService
import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import com.example.integrated.util.*
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseCookie
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.stereotype.Service
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.*
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

@Service
class QueueService(
        @Value("\${queue.validation.key}")
        private val validationKey: String,

        private val queueSchedulerService: QueueSchedulerService,
        private val queueScheduler: QueueScheduler,
        private val redisPublisher: RedisPublisher,
        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
        private val objectMapper: ObjectMapper
) : Loggable {

    // 대기열 등록, wait/allow 진입 여부를 enum으로 반환
    suspend fun registerUserToWaitQueue(
            queueType: String,
            userId: String
    ): RegisterResult {
        val code = queueSchedulerService.enqueueOrAllow(queueType, userId)

        return when (code) {
            -1L, -2L -> RegisterResult.ALREADY_EXISTS
            0L -> {
                queueScheduler.addActiveQueue(queueType)
                RegisterResult.REGISTERED_WAIT
            }
            else -> {
                queueScheduler.addActiveQueue(queueType)
                RegisterResult.REGISTERED_ALLOW
            }
        }
    }

    // 대기열에서의 사용자 순위 조회 ( 존재하지 않으면 -1L 반환 )
    suspend fun getWaitQueueRank(
        queueType: String,
        userId: String
    ): Long =
        reactiveRedisTemplate.opsForZSet()
            .rank("$queueType$WAIT_QUEUE", userId)
            .awaitFirstOrNull()
            ?.let { it + 1L }
            ?: -1L

    /* Lua로 wait/allow에서 원자적으로 제거 ( race 문제 해결을 위함 )
    * wait/allow : publish 발행 → 본인 SSE에 'cancelled' 전달
    * none : publish 생략 → 멱등 응답으로 간주
    * */
    suspend fun cancelUser(
        queueType: String,
        userId: String
    ): Boolean {
        val location = queueSchedulerService.cancelUser(queueType, userId)
        val removed = location != "none"

        // 삭제가 됐다면 삭제 이벤트를 publish
        if (removed) {
            val payload = objectMapper.writeValueAsString(
                    QueueChangePayload(
                            queueType = queueType,
                            event = "cancel",
                            ids = listOf(userId)
                    )
            )
            redisPublisher.publish(CHANNEL_NAME, payload)
        }
        return removed
    }

    // 참가열로 이동하면 유효성 인증을 위해 토큰을 생성하여 쿠키에 저장
    suspend fun issueAccessTokenCookie(
            queueType: String,
            userId: String,
            response: ServerHttpResponse
    ): ResponseEntity<String> {
        if (!isAllowTokenExpired(queueType, userId)) {
            val encodedName = URLEncoder.encode(userId, StandardCharsets.UTF_8)
            val cookieName = "reserve-user-access-cookie-$encodedName"

            val token = createAccessToken(queueType, userId)

            val responseCookie = ResponseCookie.from(cookieName, token)
                    .path("/")
                    .maxAge(Duration.ofSeconds(600))
                    .build()

            response.addCookie(responseCookie)

            return ResponseEntity.ok("쿠키 발급 완료")
        } else {
            throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.FAILED_TO_STORE_TOKEN_IN_COOKIE)
        }
    }

    // 인증을 위한 토큰 생성
    fun createAccessToken(
            queueType: String,
            userId: String
    ): String {
        try {
            val mac = Mac.getInstance("HmacSHA256")
            val keySpec = SecretKeySpec(validationKey.toByteArray(StandardCharsets.UTF_8), "HmacSHA256")
            mac.init(keySpec)

            val raw = "$queueType:$userId"
            val digest = mac.doFinal(raw.toByteArray(StandardCharsets.UTF_8))

            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest)
        } catch (e: Exception) {
            log.error(e) { "토큰 생성 중 에러 발생." }
            throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.FAIL_TO_GENERATE_TOKEN)
        }
    }

    // 타겟 페이지에 접속했을 때, 입장 가능 기간과 쿠키에 저장된 토큰의 유효성을 검증
    suspend fun isAllowTokenValid(
            queueType: String,
            userId: String,
            token: String
    ): Boolean = !(isAllowTokenExpired(queueType, userId) || isTokenMismatch(queueType, userId, token))

    // 참가열에서의 사용자 TTL 만료 여부 조회, 만료 시 true 반환
    suspend fun isAllowTokenExpired(
            queueType: String,
            userId: String
    ): Boolean {
        val key = "$queueType$ALLOW_QUEUE"

        val score = reactiveRedisTemplate.opsForZSet()
                .score(key, userId)
                .awaitSingleOrNull()
                ?: return true

        val now = System.currentTimeMillis().toDouble()

        return score < now
    }

    // 토큰의 유효성 판별 로직, 유효하지 않다면 true 반환
    private fun isTokenMismatch(
            queueType: String,
            userId: String,
            token: String
    ): Boolean = createAccessToken(queueType, userId) != token
}