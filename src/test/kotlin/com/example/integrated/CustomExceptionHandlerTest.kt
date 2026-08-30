package com.example.integrated

import com.example.integrated.reserveException.CustomExceptionHandler
import com.example.integrated.reserveException.ErrorCode
import io.lettuce.core.RedisCommandExecutionException
import io.lettuce.core.RedisException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.data.redis.RedisSystemException

/* failover 장애 주입 실험에서 실측한 예외 체인 그대로 검증한다.
 * - NOREPLICAS : 레플리카 pause 시 min-replicas-to-write 발동 (500으로 새던 케이스)
 * - Currently not connected : 마스터 stop 후 REJECT_COMMANDS 즉시 거부 (500으로 새던 케이스)
 * - Connection closed : failover 전환 순간 기존 마스터 연결 종료 (500으로 새던 케이스)
 * - WRONGTYPE : 코드 버그성 실행 오류 → 503으로 가리면 안 되므로 그대로 전파
 */
class CustomExceptionHandlerTest {

    private val handler = CustomExceptionHandler()

    @Test
    fun `NOREPLICAS 실행 오류는 503과 Retry-After를 반환한다`() {
        val ex = RedisSystemException(
            "Error in execution",
            RedisCommandExecutionException("NOREPLICAS Not enough good replicas to write. script: 8e03, on @user_script:42.")
        )

        val response = handler.handleRedisSystemException(ex)

        assertEquals(503, response.statusCode.value())
        assertEquals("3", response.headers.getFirst("Retry-After"))
        assertEquals(ErrorCode.REDIS_UNAVAILABLE.errorCode, response.body?.code)
    }

    @Test
    fun `연결 거부(REJECT_COMMANDS) 오류는 503을 반환한다`() {
        val ex = RedisSystemException(
            "Redis exception",
            RedisException("Currently not connected. Commands are rejected.")
        )

        val response = handler.handleRedisSystemException(ex)

        assertEquals(503, response.statusCode.value())
        assertEquals("3", response.headers.getFirst("Retry-After"))
    }

    @Test
    fun `failover 전환 중 연결 종료 오류는 503을 반환한다`() {
        val ex = RedisSystemException(
            "Redis exception",
            RedisException("Connection closed")
        )

        val response = handler.handleRedisSystemException(ex)

        assertEquals(503, response.statusCode.value())
    }

    @Test
    fun `NOREPLICAS가 아닌 실행 오류는 503으로 가리지 않고 그대로 전파한다`() {
        val ex = RedisSystemException(
            "Error in execution",
            RedisCommandExecutionException("WRONGTYPE Operation against a key holding the wrong kind of value")
        )

        assertThrows<RedisSystemException> { handler.handleRedisSystemException(ex) }
    }
}
