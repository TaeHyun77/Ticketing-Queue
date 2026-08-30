package com.example.integrated.reserveException

import com.example.integrated.util.Loggable
import io.lettuce.core.RedisCommandExecutionException
import io.lettuce.core.RedisCommandTimeoutException
import io.lettuce.core.RedisConnectionException
import io.lettuce.core.RedisException
import org.springframework.data.redis.RedisConnectionFailureException
import org.springframework.data.redis.RedisSystemException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler

@ControllerAdvice
class CustomExceptionHandler : Loggable {

    @ExceptionHandler(ReserveException::class)
    fun handleReserveException(ex: ReserveException): ResponseEntity<ErrorCodeDto> =
        ErrorCodeDto.Companion.toException(ex)

    @ExceptionHandler(
        RedisCommandTimeoutException::class,
        RedisConnectionException::class,
        RedisConnectionFailureException::class
    )
    fun handleRedisException(ex: Exception): ResponseEntity<ErrorCodeDto> {
        log.warn { "Redis 연결 실패 (failover 가능성): ${ex.message}" }

        return redisUnavailableResponse()
    }

    /* ReactiveRedisTemplate 경로는 Lettuce 예외를 RedisSystemException으로 감싸므로 cause로 판별한다.
     * failover 장애 주입 실험에서 실측된 일시 장애 케이스만 503으로 변환한다.
     * - NOREPLICAS : min-replicas-to-write 발동 (failover 유실 창 보호 동작)
     * - 그 외 RedisException : Connection closed, Currently not connected 등 연결 상태 오류
     * NOREPLICAS가 아닌 명령 실행 오류(WRONGTYPE, 스크립트 버그 등)는 일시 장애가 아니므로
     * 503으로 가리지 않고 그대로 전파해 500을 유지한다.
     */
    @ExceptionHandler(RedisSystemException::class)
    fun handleRedisSystemException(ex: RedisSystemException): ResponseEntity<ErrorCodeDto> {
        val cause = ex.mostSpecificCause

        val unavailable = when (cause) {
            is RedisCommandExecutionException -> cause.message?.startsWith("NOREPLICAS") == true
            is RedisException -> true
            else -> false
        }

        if (!unavailable) {
            throw ex
        }

        log.warn { "Redis 연결 실패 (failover 가능성): ${cause.message}" }

        return redisUnavailableResponse()
    }

    private fun redisUnavailableResponse(): ResponseEntity<ErrorCodeDto> {
        val errorCode = ErrorCode.REDIS_UNAVAILABLE
        return ResponseEntity
            .status(HttpStatus.SERVICE_UNAVAILABLE)
            .header("Retry-After", "3")
            .body(
                ErrorCodeDto(
                    code = errorCode.errorCode,
                    message = errorCode.message,
                    detail = null
                )
            )
    }
}
