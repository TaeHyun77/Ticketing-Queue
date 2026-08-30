package com.example.integrated.queue.queue.dto

data class QueueChangePayload(
    val queueType: String,
    val event: String,
    val ids: List<String>,
    // 이 큐에서 지금까지 승격된 누적 인원, 클라이언트는 rank = R0 - (admittedThrough - A0)로 자기 순번을 계산합니다.
    // 절대값이라 StateFlow conflation으로 중간 값이 유실돼도 최신 값 하나로 정확도가 유지됩니다.
    val admittedThrough: Long = 0
)
