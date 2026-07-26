#!/bin/bash
# 대기열 정합성 테스트 — 동시성 하에서 불변식 3가지 검증
#
# 검증 불변식:
#   [1] 정원      : 참가열(allow) 인원이 max-capacity(400)를 절대 초과하지 않는다.
#   [2] 무유실·무중복 : 성공 등록 건수 == ZCARD(wait)+ZCARD(allow), 그리고 wait∩allow = 0.
#   [3] 선착순     : 대기자가 존재하는 상태에서 들어온 신규 유저는 단 한 명도
#                    참가열로 직행(REGISTERED_ALLOW)하지 못한다. (추월 방지)
#
# 흐름:
#   A. FLUSHALL 후 SEED명 대량 등록 → 참가열 400 만석 + 대기열 백로그 형성
#      (등록 중 ZCARD allow 를 연속 샘플링해 순간 초과 감시)
#   B. 대기자가 있는 상태에서 BURST명 신규 등록 → REGISTERED_ALLOW 가 0 이어야 함
#   C. 정원/무유실/무중복 최종 대조
#
# 사전 조건:
#   docker compose -f docker-compose.1node.yml up -d
#
# 사용법:
#   ./queue-test/queue-correctness-test.sh

set -uo pipefail

# ---- 설정 ----
QUEUE="concert"
CAPACITY=400                       # application.properties: queue.allow.max-capacity
REDIS="queue-redis-master"
TARGET_URL="http://localhost:8079/queue/register"
SEED=2000                          # 초기 씨딩 인원 (>> CAPACITY 여야 백로그 형성)
BURST=1000                         # 선착순 검증용 신규 버스트 인원

WAIT_KEY="${QUEUE}:user-queue:wait"
ALLOW_KEY="${QUEUE}:user-queue:allow"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOAD_TEST="$SCRIPT_DIR/queue-load-test.js"
RESULT_DIR="$PROJECT_DIR/results/correctness"
mkdir -p "$RESULT_DIR"

FAILED=0
pass() { echo "  ✅ PASS  $1"; }
fail() { echo "  ❌ FAIL  $1"; FAILED=1; }

rget() { docker exec "$REDIS" redis-cli "$@" 2>/dev/null | tr -d '\r'; }
kval() { grep "^$2=" "$1" 2>/dev/null | cut -d= -f2 | tr -d '\r'; }

echo ""
echo "======================================================="
echo "  대기열 정합성 테스트 (단일 서버, 정원=${CAPACITY})"
echo "======================================================="

# ---- 사전 점검 ----
if ! curl -sf -o /dev/null "http://localhost:8079/actuator/health" 2>/dev/null \
   && ! rget PING >/dev/null 2>&1; then
    echo "  환경 미준비: nginx(:8079) 또는 Redis 접근 불가. compose 기동을 확인하세요."
    exit 1
fi

# ---- 초기화 ----
echo ""
echo "[reset] FLUSHALL"
rget FLUSHALL >/dev/null

# =====================================================================
# Phase A — 씨딩 + 정원 연속 감시
# =====================================================================
echo ""
echo "[A] 씨딩 ${SEED}명 (참가열 만석 + 대기 백로그 형성), 정원 연속 감시"

# 백그라운드 정원 샘플러: ZCARD allow 최댓값 기록
SAMPLE_FILE="$RESULT_DIR/allow_samples.txt"
: > "$SAMPLE_FILE"
(
    while true; do
        rget ZCARD "$ALLOW_KEY" >> "$SAMPLE_FILE"
        sleep 0.2
    done
) &
SAMPLER_PID=$!

K6_RATE=1000 K6_TEST_DURATION=2 K6_UID_PREFIX="seed" \
    K6_RESULT_FILE="$RESULT_DIR/seed_result.txt" \
    k6 run "$LOAD_TEST" >/dev/null 2>&1 || true

kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true

# 스케줄러 안정화
sleep 3

MAX_ALLOW=$(sort -n "$SAMPLE_FILE" | tail -1)
MAX_ALLOW=${MAX_ALLOW:-0}
SEED_SUCCESS=$(kval "$RESULT_DIR/seed_result.txt" success); SEED_SUCCESS=${SEED_SUCCESS:-0}
A_ALLOW=$(rget ZCARD "$ALLOW_KEY"); A_ALLOW=${A_ALLOW:-0}
A_WAIT=$(rget ZCARD "$WAIT_KEY"); A_WAIT=${A_WAIT:-0}

echo "    성공=${SEED_SUCCESS}  allow=${A_ALLOW}  wait=${A_WAIT}  (감시 중 allow 최댓값=${MAX_ALLOW})"

# 백로그가 실제로 형성됐는지 (테스트 전제) 확인
if [ "${A_WAIT:-0}" -le 0 ]; then
    echo "  ⚠️  대기 백로그가 형성되지 않음 — SEED 를 늘리거나 앱 상태를 확인하세요. 선착순 검증 불가."
    FAILED=1
fi

# =====================================================================
# Phase B — 선착순(FIFO) 검증: 대기자 존재 상태에서 신규 버스트
# =====================================================================
echo ""
echo "[B] 대기자 존재 상태에서 신규 ${BURST}명 버스트 → 참가열 직행 0건이어야 함"

K6_RATE=1000 K6_TEST_DURATION=1 K6_UID_PREFIX="burst" \
    K6_RESULT_FILE="$RESULT_DIR/burst_result.txt" \
    k6 run "$LOAD_TEST" >/dev/null 2>&1 || true

BURST_SUCCESS=$(kval "$RESULT_DIR/burst_result.txt" success); BURST_SUCCESS=${BURST_SUCCESS:-0}
BURST_ALLOW=$(kval "$RESULT_DIR/burst_result.txt" allow);   BURST_ALLOW=${BURST_ALLOW:-0}
BURST_WAIT=$(kval "$RESULT_DIR/burst_result.txt" wait);     BURST_WAIT=${BURST_WAIT:-0}

echo "    버스트 성공=${BURST_SUCCESS}  참가열직행(allow)=${BURST_ALLOW}  대기열(wait)=${BURST_WAIT}"

# =====================================================================
# Phase C — 최종 대조
# =====================================================================
sleep 2
F_ALLOW=$(rget ZCARD "$ALLOW_KEY"); F_ALLOW=${F_ALLOW:-0}
F_WAIT=$(rget ZCARD "$WAIT_KEY");   F_WAIT=${F_WAIT:-0}

# 교집합 (wait ∩ allow)
rget ZINTERSTORE "tmp:inter" 2 "$WAIT_KEY" "$ALLOW_KEY" >/dev/null
INTER=$(rget ZCARD "tmp:inter"); INTER=${INTER:-0}
rget DEL "tmp:inter" >/dev/null

TOTAL_SUCCESS=$(( ${SEED_SUCCESS:-0} + ${BURST_SUCCESS:-0} ))
TOTAL_REDIS=$(( ${F_ALLOW:-0} + ${F_WAIT:-0} ))

echo ""
echo "-------------------------------------------------------"
echo "  검증 결과"
echo "-------------------------------------------------------"

# [1] 정원
if [ "${MAX_ALLOW:-0}" -le "$CAPACITY" ] && [ "${F_ALLOW:-0}" -le "$CAPACITY" ]; then
    pass "정원: allow 최댓값 ${MAX_ALLOW} ≤ ${CAPACITY} (최종 ${F_ALLOW})"
else
    fail "정원 초과: 감시 최댓값 ${MAX_ALLOW}, 최종 ${F_ALLOW} (한도 ${CAPACITY})"
fi

# [2] 무중복
if [ "${INTER:-0}" -eq 0 ]; then
    pass "무중복: wait ∩ allow = 0"
else
    fail "중복 존재: ${INTER}명이 wait/allow 양쪽에 존재"
fi

# [2] 무유실
if [ "$TOTAL_SUCCESS" -eq "$TOTAL_REDIS" ]; then
    pass "무유실: 성공 ${TOTAL_SUCCESS} == Redis(wait+allow) ${TOTAL_REDIS}"
else
    fail "유실/불일치: 성공 ${TOTAL_SUCCESS} ≠ Redis ${TOTAL_REDIS} (wait=${F_WAIT}, allow=${F_ALLOW})"
fi

# [3] 선착순
if [ "${BURST_ALLOW:-0}" -eq 0 ] && [ "${BURST_SUCCESS:-0}" -gt 0 ]; then
    pass "선착순: 대기자 존재 중 신규 ${BURST_SUCCESS}명 전원 대기열行, 참가열 직행 0건 (추월 없음)"
elif [ "${BURST_SUCCESS:-0}" -le 0 ]; then
    fail "선착순 검증 불가: 버스트 성공 0건 (부하/환경 확인)"
else
    fail "선착순 위반: 신규 유저 ${BURST_ALLOW}명이 대기자를 추월해 참가열 직행"
fi

echo "-------------------------------------------------------"
if [ "$FAILED" -eq 0 ]; then
    echo "  🎉 전체 통과"
else
    echo "  ⛔ 실패 항목 있음"
fi
echo "  결과: $RESULT_DIR/"
echo ""
exit $FAILED
