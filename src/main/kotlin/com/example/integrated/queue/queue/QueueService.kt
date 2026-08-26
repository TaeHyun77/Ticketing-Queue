package com.example.integrated.queue.queue

import com.example.integrated.queue.queue.dto.QueueChangePayload
import com.example.integrated.queue.queue.dto.QueueStatus
import com.example.integrated.queue.queue.dto.QueueStatusResponse
import com.example.integrated.queue.queue.dto.RegisterResult
import com.example.integrated.queue.queue.scheduler.QueueSchedulerService
import com.example.integrated.queue.sse.SseEventService
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
        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
        private val objectMapper: ObjectMapper
) : Loggable {

    suspend fun registerUserToWaitQueue(
            queueType: String,
            userId: String
    ): RegisterResult {
        val code = queueSchedulerService.enqueueOrAllow(queueType, userId)

        return when (code) {
            -1L, -2L -> RegisterResult.ALREADY_EXISTS
            0L -> RegisterResult.REGISTERED_WAIT
            else -> RegisterResult.REGISTERED_ALLOW
        }
    }

    suspend fun getWaitQueueRank(
        queueType: String,
        userId: String
    ): Long =
        reactiveRedisTemplate.opsForZSet()
            .rank("$queueType$WAIT_QUEUE", userId)
            .awaitFirstOrNull()
            ?.let { it + 1L }
            ?: -1L

    suspend fun getAdmittedThrough(
        queueType: String
    ): Long =
        reactiveRedisTemplate.opsForValue()
            .get("$ADMITTED_COUNTER_KEY_PREFIX$queueType")
            .awaitFirstOrNull()
            ?.toLongOrNull()
            ?: 0L

    suspend fun getQueueStatus(
        queueType: String,
        userId: String
    ): QueueStatusResponse {
        val rank = getWaitQueueRank(queueType, userId)
        if (rank > 0) {
            return QueueStatusResponse(QueueStatus.WAIT, rank)
        }

        if (!isAllowTokenExpired(queueType, userId)) {
            return QueueStatusResponse(QueueStatus.ALLOW)
        }

        return QueueStatusResponse(QueueStatus.NONE)
    }

    suspend fun cancelUser(
        queueType: String,
        userId: String
    ): Boolean {
        val location = queueSchedulerService.cancelUser(queueType, userId)
        val removed = location != "none"

        if (removed) {
            SseEventService.emit(
                QueueChangePayload(
                    queueType = queueType,
                    event = "cancel",
                    ids = listOf(userId)
                )
            )
        }
        return removed
    }

    suspend fun moveToWaitQueueTail(
        queueType: String,
        userId: String
    ): Boolean = queueSchedulerService.moveToTail(queueType, userId) > 0

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

    suspend fun isAllowTokenValid(
            queueType: String,
            userId: String,
            token: String
    ): Boolean = !(isAllowTokenExpired(queueType, userId) || isTokenMismatch(queueType, userId, token))

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

    private fun isTokenMismatch(
            queueType: String,
            userId: String,
            token: String
    ): Boolean = createAccessToken(queueType, userId) != token
}
