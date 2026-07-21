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
--    0 : 대기열 신규 삽입 (참가열 만석 또는 대기자 존재)
--    1 : 참가열 직접 삽입 (여유 O + 대기자 X)

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

-- 3. 참가열에서 만료된 항목 제거 후 현재 인원 및 대기 인원 확인
redis.call('ZREMRANGEBYSCORE', allowKey, '-inf', nowMs)
local currentCount = redis.call('ZCARD', allowKey)
local waitCount = redis.call('ZCARD', waitKey)

-- 4. 참가열에 여유가 있고 '대기자가 없을 때만' 참가열에 직접 삽입
--    대기자가 있으면 빈자리는 반드시 대기열 선두에게 양보해야 FIFO가 유지된다.
--    (여기서 신규 유저를 직행시키면 먼저 온 대기자를 추월 → 선착순 위반)
if currentCount < maxCapacity and waitCount == 0 then
    redis.call('ZADD', allowKey, expireAt, userId)
    return 1
end

-- 5. 참가열 만석 또는 대기자 존재 → 대기열에 삽입 (INCR 결과를 score로 그대로 사용)
--    빈자리는 다음 스케줄러 틱의 ZPOPMIN(선두)이 FIFO 순서대로 채운다.
local seq = redis.call('INCR', seqKey)
redis.call('ZADD', waitKey, seq, userId)

return 0
