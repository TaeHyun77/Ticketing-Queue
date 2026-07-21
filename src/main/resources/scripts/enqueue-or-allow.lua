-- 통합 Lua 스크립트: 중복 체크 + score 생성 + 참가열/대기열 삽입을 원자적으로 수행

-- KEYS[1] : 참가열 키 (ZSet)
-- KEYS[2] : 대기열 키 (ZSet)
-- KEYS[3] : 시퀀스 카운터 키 (queue:seq:{queueType}, 영구 보존)

-- ARGV[1] : userId
-- ARGV[2] : 참가열 최대 수용 인원 (maxCapacity)
-- ARGV[3] : 현재 시각 밀리초 (참가열 만료 정리용)
-- ARGV[4] : 참가열 score (expireAt)

-- 반환 값: 단일 정수
--   -1 : 대기열 중복
--   -2 : 참가열 중복
--    0 : 대기열 신규 삽입
--    1 : 참가열 직접 삽입

local allowKey = KEYS[1]
local waitKey = KEYS[2]
local seqKey = KEYS[3]

local userId = ARGV[1]
local maxCapacity = tonumber(ARGV[2])
local nowMs = tonumber(ARGV[3])
local expireAt = tonumber(ARGV[4])

-- 1. 대기열에 이미 존재하는지 확인
if redis.call('ZSCORE', waitKey, userId) then
    return -1
end

-- 2. 참가열에 이미 존재하는지 확인
if redis.call('ZSCORE', allowKey, userId) then
    return -2
end

-- 3. 참가열에서 만료된 항목 제거 후 현재 인원 확인
redis.call('ZREMRANGEBYSCORE', allowKey, '-inf', nowMs)
local currentCount = redis.call('ZCARD', allowKey)

-- 4. 참가열에 여유가 있으면 참가열에 직접 삽입
if currentCount < maxCapacity then
    redis.call('ZADD', allowKey, expireAt, userId)
    return 1
end

-- 5. 참가열 여유 없음 → 대기열에 삽입 (INCR 결과를 score로 그대로 사용)
local seq = redis.call('INCR', seqKey)
redis.call('ZADD', waitKey, seq, userId)

return 0
