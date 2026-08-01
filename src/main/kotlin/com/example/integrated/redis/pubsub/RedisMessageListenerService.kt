package com.example.integrated.redis.pubsub

import com.example.integrated.queue.queue.dto.QueueChangePayload
import com.example.integrated.queue.sse.SseEventService
import com.example.integrated.util.CHANNEL_NAME
import com.example.integrated.util.Loggable
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.stereotype.Service
import java.util.concurrent.atomic.AtomicLong

@Service
class RedisMessageListenerService(
    private val redisMessageListenerContainer: ReactiveRedisMessageListenerContainer,
    private val objectMapper: ObjectMapper
): Loggable {

    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    // StateFlow 동등성 conflation을 피하기 위한 이벤트 구분용 단조 시퀀스
    private val seqCounter = AtomicLong(0)

    // 애플리케이션 실행 시 pub/sub channel 구독
    @PostConstruct
    fun init() {
        scope.launch {
            receiveMessages(ChannelTopic(CHANNEL_NAME))
        }
        log.info { "pub/sub channel 구독 시작" }
    }

    // 애플리케이션 종료 시 pub/sub channel 구독 해제
    @PreDestroy
    fun destroy() {
        scope.cancel()
        log.info { "pub/sub channel 구독 종료" }
    }

    // Redis Pub/Sub 채널을 구독하여 수신된 JSON 메시지를 QueueChangePayload로 파싱 후 sink에 전달
    private suspend fun receiveMessages(channel: ChannelTopic) {
        val channelSerializer = createChannelAndValueSerializer()
        var attempt = 0L

        while (true) {
            try {
                redisMessageListenerContainer
                    .receive(listOf(channel), channelSerializer, channelSerializer)
                    .asFlow()
                    .collect { message ->
                        runCatching {
                            val payload = objectMapper.readValue<QueueChangePayload>(message.message)
                            // 절대 커서(admittedThrough)는 발행 측에서 Lua로 원자 계산해 payload에 이미 실려 있으므로
                            // 수신 인스턴스는 재계산 없이 그대로 전달한다. StateFlow는 동등 값이면 방출을 삼키므로 단조 seq로 매 이벤트를 구분한다.
                            SseEventService.getSink(payload.queueType).value =
                                payload.copy(seq = seqCounter.incrementAndGet())
                        }.onFailure {
                            log.warn { "Pub/Sub 메시지 파싱 실패: ${it.message}, raw=${message.message}" }
                        }
                    }
                // 예외 없는 정상 완료도 구독 중단이므로 재구독한다 (break 시 알림 수신이 영구 중단됨)
                val delayMs = 2000L * minOf(++attempt, 5)
                log.warn { "Redis Pub/Sub 스트림 정상 종료 (시도: $attempt), ${delayMs}ms 후 재구독" }
                delay(delayMs)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                val delayMs = 2000L * minOf(++attempt, 5)
                log.warn { "Redis Pub/Sub 연결 끊김 (시도: $attempt), ${delayMs}ms 후 재구독: ${e.message}" }
                delay(delayMs)
            }
        }
    }

    private fun createChannelAndValueSerializer(): RedisSerializationContext.SerializationPair<String> {
        return RedisSerializationContext.SerializationPair.fromSerializer(StringRedisSerializer())
    }
}
