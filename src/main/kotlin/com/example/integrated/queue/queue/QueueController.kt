package com.example.integrated.queue.queue

import com.example.integrated.queue.queue.dto.QueueRequest
import com.example.integrated.queue.queue.dto.QueueStatusResponse
import com.example.integrated.queue.queue.dto.RegisterResult
import com.example.integrated.util.Loggable
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.web.bind.annotation.*

@RequestMapping("/queue")
@RestController
class QueueController (
    @Value("\${SERVER_NAME}")
    private val serverName: String? = null,

    private val queueService: QueueService
): Loggable {
    // 대기열에 사용자 등록, wait/allow 진입 여부는 응답 enum으로 노출
    @PostMapping("/register")
    suspend fun registerUser(
        @RequestBody request: QueueRequest,
    ): RegisterResult {
        val queueType = request.queueType
        val userId = request.userId

        log.info { "대기열 등록 요청 : server=$serverName, userId=$userId, queueType=$queueType" }

        return queueService.registerUserToWaitQueue(queueType, userId)
    }

    // 대기열에서의 사용자 순위 조회
    @GetMapping("/get/rank")
    suspend fun getUserRank(
        @RequestParam queueType: String,
        @RequestParam userId: String,
    ): Long = queueService.getWaitQueueRank(queueType, userId)

    // 대기열/참가열 통합 상태 조회 ( SSE 재접속·유실 복구 시 클라이언트 재동기화용 )
    @GetMapping("/get/status")
    suspend fun getUserStatus(
        @RequestParam queueType: String,
        @RequestParam userId: String,
    ): QueueStatusResponse = queueService.getQueueStatus(queueType, userId)

    // 쿠키에 토큰 전달
    @GetMapping("/create/cookie")
    suspend fun issueAccessTokenCookie(
        @RequestParam queueType: String,
        @RequestParam userId: String,
        response: ServerHttpResponse
    ): ResponseEntity<String> = queueService.issueAccessTokenCookie(queueType, userId, response)

    // 토큰의 유효성 판단
    @PostMapping("/isValidateToken/{token}")
    suspend fun isAccessTokenValid(
        @RequestBody request: QueueRequest,
        @PathVariable("token") token: String
    ): Boolean =
        queueService.isAllowTokenValid(request.queueType, request.userId, token)

    // 대기열 or 참가열 등록 취소
    @PostMapping("/cancel")
    suspend fun cancelReserve(
        @RequestBody request: QueueRequest,
    ): Boolean =
        queueService.cancelUser(request.queueType, request.userId)
}