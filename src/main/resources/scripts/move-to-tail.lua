-- 대기열 내 사용자를 맨 뒤로 이동시키는 스크립트 ( 새로고침 시 맨 뒤로 순번 밀기 용도 )
-- score = 단조 증가 seq이므로, INCR로 새 최댓값을 뽑아 ZADD로 제자리 갱신하면 tail 이동이 원자적으로 진행됨
-- cancel과 달리 pub/sub을 발행하지 않는다 : 본인 cancel 이벤트가 sink에 남아 재접속 시 self-bounce로 이어지는 레이스를 차단하기 위함

-- KEYS[1] : 대기열 키 (ZSet)
-- KEYS[2] : 시퀀스 카운터 키 (queue:seq:{queueType}, enqueue와 동일 소스)

-- ARGV[1] : userId

-- 반환 값: 단일 정수
--   -1 : 대기열에 없음 → no-op (참가열 입장자/이미 이탈한 사용자는 순번을 건드리지 않는다)
--   >0 : 이동 성공 (새로 부여된 score = seq)

local waitKey = KEYS[1]
local seqKey = KEYS[2]

local userId = ARGV[1]

-- 대기열에 있을 때만 이동. 참가열(allow) 입장자를 대기열 뒤로 강등시키지 않기 위한 가드.
if not redis.call('ZSCORE', waitKey, userId) then
    return -1
end

local seq = redis.call('INCR', seqKey)
redis.call('ZADD', waitKey, seq, userId)

return seq