package com.example.integrated.queue.sse

import com.example.integrated.queue.queue.QueueService
import com.example.integrated.queue.queue.dto.QueueChangePayload
import com.example.integrated.queue.sse.event.CancelledSseEvent
import com.example.integrated.queue.sse.event.ConfirmSseEvent
import com.example.integrated.queue.sse.event.ErrorSseEvent
import com.example.integrated.queue.sse.event.MovedSseEvent
import com.example.integrated.queue.sse.event.UpdateSseEvent
import com.example.integrated.util.Loggable
import com.example.integrated.util.isRedisConnectionException
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
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
        const val EVENT_NONE = "none"

        private val sinks = ConcurrentHashMap<String, MutableStateFlow<QueueChangePayload>>()

        fun getSink(queueType: String): MutableStateFlow<QueueChangePayload> {
            return sinks.computeIfAbsent(queueType) {
                MutableStateFlow(QueueChangePayload(queueType, EVENT_NONE, emptyList()))
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

            // promote 팬아웃(본인 미포함) : 구독자별 rank를 계산하지 않고 절대 커서만 전송
            // 클라이언트가 앵커(R0, A0) 기준으로 자기 rank를 갱신하므로, 대용량 rank 맵과 구독자별 재조회가 사라져 StateFlow conflation의 O(N) equals와 GC 압력이 제거됨
            if (payload.event == EVENT_PROMOTE) {
                return sse("moved", MovedSseEvent(payload.admittedThrough))
            }

            // init : 접속자 본인의 현재 rank 1회 조회 (연결당 1회, 팬아웃 아님)
            val rank = queueService.getWaitQueueRank(queueType, userId)

            if (rank > 0) {
                // 앵커용 절대 커서를 rank 다음에 읽습니다.
                val admittedThrough = queueService.getAdmittedThrough(queueType)
                sse("update", UpdateSseEvent(rank, admittedThrough))
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
