package com.example.integrated.util

import com.fasterxml.jackson.databind.ObjectMapper
import io.lettuce.core.RedisCommandTimeoutException
import io.lettuce.core.RedisConnectionException
import org.springframework.data.redis.RedisConnectionFailureException


const val WAIT_QUEUE: String = ":user-queue:wait"
const val ALLOW_QUEUE: String = ":user-queue:allow"
const val CHANNEL_NAME = "queueing_system"
const val ACTIVE_QUEUE_KEY = "active-allow-queue"
const val SCHEDULING_KEY = "scheduling-key"

// 큐별 누적 승격 카운터(절대 커서). 실제 키는 "queue:admitted:{queueType}", TTL 없이 영구 보존된다.
const val ADMITTED_COUNTER_KEY_PREFIX = "queue:admitted:"

inline fun <reified T> ObjectMapper.readValueFromJson(json: String): T {
    return readValue(json, T::class.java)
}

fun isRedisConnectionException(e: Throwable): Boolean =
    e is RedisCommandTimeoutException ||
    e is RedisConnectionException ||
    e is RedisConnectionFailureException ||
    e.cause?.let { isRedisConnectionException(it) } == true