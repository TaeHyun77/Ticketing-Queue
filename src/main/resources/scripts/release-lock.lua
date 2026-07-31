-- 락 해제: 소유 토큰이 일치할 때만 삭제한다.
-- lease(TTL) 만료 후 다른 서버가 새로 획득한 락을 실수로 삭제하는 것을 방지하기 위해,
-- GET 값이 자신이 저장한 토큰과 같을 때만 DEL 한다.
--
-- KEYS[1] : 락 키
-- ARGV[1] : 획득 시 저장한 소유 토큰
--
-- 반환: 1 (해제됨) | 0 (소유자 불일치 또는 이미 만료)

if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
