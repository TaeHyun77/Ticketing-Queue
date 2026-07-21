-- 스케줄러 전용 Lua 스크립트: 만료 정리 + 빈 자리 계산 + 승격을 원자적으로 수행
--
-- KEYS[1] : 참가열 키 (ZSet)
-- KEYS[2] : 대기열 키 (ZSet)
-- KEYS[3] : 활성 큐 레지스트리 키 (active-allow-queue, Set)
--
-- ARGV[1] : 참가열 최대 수용 인원 (maxCapacity)
-- ARGV[2] : 현재 시각 밀리초 (만료 판단용)
-- ARGV[3] : 참가열 score (expireAt)
-- ARGV[4] : queueType (레지스트리 멤버)
--
-- 반환 값: { count, ids } 구조의 JSON 문자열
--   count : 승격된 사용자 수
--   ids   : 승격된 사용자 ID 배열

local allowKey = KEYS[1]
local waitKey = KEYS[2]
local activeKey = KEYS[3]

local maxCapacity = tonumber(ARGV[1])
local nowMs = tonumber(ARGV[2])
local expireAt = tonumber(ARGV[3])
local queueType = ARGV[4]

local function emptyResult()
    return cjson.encode({ count = 0, ids = {} })
end

-- 1. 참가열에서 만료된 사용자 정리
redis.call('ZREMRANGEBYSCORE', allowKey, '-inf', nowMs)

-- 2. 빈 자리 계산
local currentCount = redis.call('ZCARD', allowKey)
local vacancy = maxCapacity - currentCount
if vacancy <= 0 then return emptyResult() end

-- 3. 대기열에서 빈 자리만큼 꺼내서 참가열에 삽입
local waitUsers = redis.call('ZPOPMIN', waitKey, vacancy)
if #waitUsers == 0 then
    -- 대기자 없음. 참가열까지 비었으면(모두 만료) 이 큐는 완전히 소진 → 레지스트리에서 제거
    if currentCount == 0 then
        redis.call('SREM', activeKey, queueType)
    end
    return emptyResult()
end

local promotedIds = {}
for i = 1, #waitUsers, 2 do
    redis.call('ZADD', allowKey, expireAt, waitUsers[i])
    table.insert(promotedIds, waitUsers[i])
end

return cjson.encode({ count = #promotedIds, ids = promotedIds })
