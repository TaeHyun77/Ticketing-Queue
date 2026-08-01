package com.example.integrated.queue.sse.event

// promote 팬아웃 시 대기자 전원에게 보내는 절대 커서
// 서버가 구독자별 rank를 계산/전송하지 않고 "지금까지 누적 N명 승격됐다"만 브로드캐스트하면,
// 클라이언트가 init에서 받은 앵커(rank R0, admittedThrough A0)로 rank = R0 - (admittedThrough - A0)를 계산한다.
// 절대값이라 StateFlow conflation으로 중간 값이 유실돼도 최신 값 하나로 정확도가 유지된다.
data class MovedSseEvent(
    val admittedThrough: Long
)
