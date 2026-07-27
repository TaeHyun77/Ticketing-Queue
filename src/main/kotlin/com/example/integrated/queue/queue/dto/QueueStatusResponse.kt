package com.example.integrated.queue.queue.dto

// 대기열/참가열 통합 상태
enum class QueueStatus {
    WAIT,   // 대기열에 존재
    ALLOW,  // 참가열에 존재 (만료 전)
    NONE,   // 어디에도 없음 (미등록·취소·만료·유실)
}

// 클라이언트 상태 재동기화용 응답 (SSE 재접속 시 재조회, NONE 감지 시 재등록 판단에 사용)
data class QueueStatusResponse(
    val status: QueueStatus,
    val rank: Long? = null,   // WAIT일 때만 값 존재 (1부터 시작)
)
