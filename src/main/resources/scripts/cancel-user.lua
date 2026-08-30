-- 취소 원자적 처리 : wait → allow 순서로 ZREM, 위치 반환
--
-- KEYS[1] : 대기열 키 (ZSet)
-- KEYS[2] : 참가열 키 (ZSet)
-- KEYS[3] : 활성 큐 레지스트리 키 (active-allow-queue, Set)
--
-- ARGV[1] : userId
-- ARGV[2] : queueType (레지스트리 멤버)
--
-- 반환 값:
-- 'wait'  : 대기열에서 제거됨
-- 'allow' : 참가열에서 제거됨
-- 'none'  : 양쪽 모두 없음 (이미 취소/만료/미등록)

local waitKey = KEYS[1]
local allowKey = KEYS[2]
local activeKey = KEYS[3]
local userId = ARGV[1]
local queueType = ARGV[2]

-- 취소로 대기열·참가열이 모두 비면 이 큐는 완전히 소진 → 레지스트리에서 제거
local function removeIfDrained()
    if redis.call('ZCARD', waitKey) == 0 and redis.call('ZCARD', allowKey) == 0 then
        redis.call('SREM', activeKey, queueType)
    end
end

if redis.call('ZREM', waitKey, userId) > 0 then
    removeIfDrained()
    return 'wait'
end

if redis.call('ZREM', allowKey, userId) > 0 then
    removeIfDrained()
    return 'allow'
end

return 'none'
