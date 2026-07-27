package com.example.integrated.queue.sse

import com.example.integrated.queue.queue.QueueService
import com.example.integrated.queue.queue.dto.QueueChangePayload
import com.example.integrated.queue.sse.event.CancelledSseEvent
import com.example.integrated.queue.sse.event.ConfirmSseEvent
import com.example.integrated.queue.sse.event.ErrorSseEvent
import com.example.integrated.queue.sse.event.UpdateSseEvent
import com.example.integrated.util.Loggable
import com.example.integrated.util.isRedisConnectionException
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import org.springframework.http.codec.ServerSentEvent
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@Service
class SseEventService(
    private val queueService: QueueService,
    private val objectMapper: ObjectMapper,
): Loggable {

    // queueType 별로 이벤트를 독립적으로 전달하기 위해 Flow를 분리
    companion object {
        const val EVENT_INIT = "init"
        const val EVENT_PROMOTE = "promote"
        const val EVENT_CANCEL = "cancel"

        private val sinks = ConcurrentHashMap<String, MutableSharedFlow<QueueChangePayload>>()

        fun getSink(queueType: String): MutableSharedFlow<QueueChangePayload> {
            return sinks.computeIfAbsent(queueType) {
                // 버퍼가 차면(느린 SSE 소비자) 오래된 이벤트부터 버린다.
                // 이벤트는 상태가 아니라 재조회 트리거이므로 최신 이벤트를 남기는 쪽이 옳고,
                // 기본 정책(SUSPEND)에서는 tryEmit이 조용히 실패해 전체 구독자가 이벤트를 잃는다.
                MutableSharedFlow(extraBufferCapacity = 256, onBufferOverflow = BufferOverflow.DROP_OLDEST)
            }
        }
    }

    fun streamQueueEvents(
        queueType: String,
        userId: String
    ): Flow<ServerSentEvent<String>> {
        val initial = flowOf(
            QueueChangePayload(queueType = queueType, event = EVENT_INIT, ids = emptyList())
        )

        return merge(initial, getSink(queueType))
            // cancel 이벤트는 본인일 때만 처리, 타인의 취소는 다음 promote 시점에 rank 갱신으로 자연 반영됨
            .filter { payload ->
                when (payload.event) {
                    EVENT_INIT, EVENT_PROMOTE -> true
                    EVENT_CANCEL -> userId in payload.ids
                    else -> false
                }
            }
            .map { buildSseEvent(queueType, userId, it) }
            .catch { e ->
                log.error { "SSE 처리 중 에러: ${e.message}" }
                emit(sse("error", ErrorSseEvent("SSE 이벤트 전송 오류 발생")))
            }
    }

    private suspend fun buildSseEvent(
        queueType: String,
        userId: String,
        payload: QueueChangePayload,
    ): ServerSentEvent<String> {
        return try {
            // 본인 승격 : ZRANK 없이 confirmed 즉시 송신
            if (payload.event == EVENT_PROMOTE && userId in payload.ids) {
                return sse("confirmed", ConfirmSseEvent(userId))
            }

            // 본인 취소 : ZRANK 없이 cancelled 즉시 송신
            if (payload.event == EVENT_CANCEL && userId in payload.ids) {
                return sse("cancelled", CancelledSseEvent(userId))
            }

            // 그 외( init 또는 본인 미포함 promote ) : 현재 rank 조회
            val rank = queueService.getWaitQueueRank(queueType, userId)

            if (rank > 0) {
                sse("update", UpdateSseEvent(rank))
            } else {
                // 대기열에 없으면 참가열 확인 (Lua 원자성으로 둘 중 하나에 반드시 존재해야 정상)
                val isInAllowQueue = !queueService.isAllowTokenExpired(queueType, userId)

                if (isInAllowQueue) {
                    sse("confirmed", ConfirmSseEvent(userId))
                } else {
                    // 대기열/참가열 모두 없음 → 취소 또는 토큰 만료
                    sse("cancelled", CancelledSseEvent(userId))
                }
            }
        } catch (e: Exception) {
            if (isRedisConnectionException(e)) {
                log.warn { "Redis 연결 실패 ${e.message}" }

                ServerSentEvent.builder<String>("")
                    .event("retry")
                    .retry(Duration.ofSeconds(30))
                    .comment("redis-failover")
                    .build()
            } else {
                log.error { "SSE 이벤트 생성 중 에러: ${e.message}" }
                sse("error", ErrorSseEvent("SSE 이벤트 생성 실패"))
            }
        }
    }

    private fun sse(name: String, payload: Any): ServerSentEvent<String> =
        ServerSentEvent.builder(objectMapper.writeValueAsString(payload))
            .event(name)
            .build()
}
