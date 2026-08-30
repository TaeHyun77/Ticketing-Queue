package com.example.integrated

import com.example.integrated.queue.queue.dto.QueueChangePayload
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 이 변경의 핵심 주장을 검증한다:
 *   "delta(promotedCount)는 StateFlow conflation으로 중간 값이 유실되면 순번이 영구히 어긋나지만,
 *    절대 커서(admittedThrough)는 느린 구독자가 최신 값 하나만 봐도 정확한 순번을 복원한다."
 *
 * Redis/프론트가 없어도 성립하는 주장이므로, 흐름(StateFlow) 의미론과 복원 산술만 재현해 결정적으로 검증한다.
 */
class AbsoluteCursorConflationTest {

    private fun promote(admittedThrough: Long, promotedCount: Int): QueueChangePayload =
        QueueChangePayload(
            queueType = "q",
            event = "promote",
            // delta 방식의 promotedCount는 곧 ids.size였다(= 이번 tick 승격 인원). 두 방식을 같은 payload로 비교하기 위해 함께 담는다.
            ids = (1..promotedCount).map { "u$it" },
            admittedThrough = admittedThrough
        )

    // 초기 앵커: init에서 클라이언트가 받는 (rank R0, admittedThrough A0)
    private val R0 = 20L
    private val A0 = 100L

    // 서버가 순차 발행한 승격 이벤트. 누적 커서 = A0 + Σdelta = 103,105,108,112 (delta 3,2,3,4)
    private val emitted = listOf(
        promote(admittedThrough = 103, promotedCount = 3),
        promote(admittedThrough = 105, promotedCount = 2),
        promote(admittedThrough = 108, promotedCount = 3),
        promote(admittedThrough = 112, promotedCount = 4),
    )
    private val trueGain = 112L - A0            // 실제 누적 승격 12명
    private val trueRank = R0 - trueGain        // 정답 순번 = 8

    @Test
    fun `절대 커서는 중간 값이 유실돼도 최신 값 하나로 정확한 순번을 복원한다`() {
        // 느린 구독자가 마지막 값(112)만 관측했다고 가정 (conflation으로 103,105,108 유실)
        val observed = listOf(emitted.last())

        // 절대 커서 복원: rank = R0 - (A - A0)
        val absoluteRank = R0 - (observed.last().admittedThrough - A0)
        assertEquals(trueRank, absoluteRank, "절대 커서는 최신 값만으로 정답(8)을 복원해야 한다")

        // delta 누적: 구독자가 실제로 본 이벤트의 promotedCount만 합산 → 마지막 4만 반영
        val deltaRank = R0 - observed.sumOf { it.ids.size.toLong() }
        assertEquals(16L, deltaRank, "delta 방식은 유실분(3+2+3)을 못 세어 16으로 과대 보고된다")
        assertTrue(deltaRank != trueRank, "delta 방식은 정답과 영구히 어긋난다 (16 != 8)")
    }

    @Test
    fun `실제 StateFlow 느린 구독자에서도 절대 커서는 정확하고 delta는 어긋난다`() = runBlocking(Dispatchers.Default) {
        val sink = MutableStateFlow(QueueChangePayload("q", "none", emptyList()))
        val received = mutableListOf<QueueChangePayload>()

        val collector = launch {
            sink.collect { p ->
                if (p.event == "promote") received += p
                delay(50) // 느린 처리 → StateFlow가 중간 값을 conflation으로 덮어씀
            }
        }

        yield() // 구독자가 초기값('none')을 받도록 양보

        // 빠른 발행자: 4개 promote를 서스펜션 없이 연속 발행 → 느린 구독자는 중간 값을 놓친다
        emitted.forEach { sink.value = it }

        delay(400) // 구독자가 최신 값까지 소진하도록 대기
        collector.cancel()

        // StateFlow는 구독자에게 '최신 값'을 반드시 전달하므로, 마지막 관측값은 항상 최종 발행값(112)이다.
        val last = received.last()
        assertEquals(112L, last.admittedThrough, "느린 구독자라도 최신 절대 커서는 반드시 관측된다")

        // 절대 커서 복원 → 정확
        val absoluteRank = R0 - (last.admittedThrough - A0)
        assertEquals(trueRank, absoluteRank, "느린 구독자여도 절대 커서로 정답(8) 복원")

        // conflation이 실제로 일어났음을 입증 (4개 중 일부 유실)
        assertTrue(received.size < emitted.size, "conflation으로 일부 이벤트가 유실되어야 한다 (관측=${received.size})")

        // delta 방식이었다면: 구독자가 실제 본 이벤트의 promotedCount 합만 반영 → 유실분만큼 영구 과대 보고
        val deltaRank = R0 - received.sumOf { it.ids.size.toLong() }
        assertTrue(deltaRank != trueRank, "delta 방식은 유실로 정답과 어긋난다 (delta=$deltaRank, 정답=$trueRank)")
    }
}
