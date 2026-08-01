package com.example.integrated.queue.sse.event

// init 시점의 앵커. rank(R0)와 그때의 절대 커서(admittedThrough=A0)를 함께 내려주면,
// 클라이언트가 이후 moved 이벤트의 admittedThrough(A)로 rank = R0 - (A - A0)를 계산한다.
data class UpdateSseEvent(
    val rank: Long,
    val admittedThrough: Long
)
