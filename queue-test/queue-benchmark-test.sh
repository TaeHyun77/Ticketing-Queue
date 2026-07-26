#!/bin/bash
# 대기열 부하 테스트 — 단일 인스턴스 베이스라인 측정
#
# 한 RPS 단계 측정 흐름:
#   1. 워밍업: 목표 RPS의 30%로 60초 (JVM JIT)
#   2. 안정화: 60초 (JIT 완료 대기)
#   3. FLUSHALL + SLOWLOG RESET (Redis 초기화)
#   4. docker stats 백그라운드 샘플링 시작
#   5. 본 측정: 목표 RPS × 60초
#   6. docker stats 종료 + SLOWLOG / commandstats 덤프
#   7. Redis 삽입 확인 (소비 시간 측정)
#   8. 회차 휴식: 60초
#
# 회차마다 결과 디렉터리: results/1node_rps{RPS}_run{N}/
#   k6_result.txt, docker_stats.csv, slowlog.txt, commandstats.txt
#
# RPS × 3회 측정 후 중앙값을 results/1node_rps{RPS}_median.txt로 저장.
#
# 사용법:
#   ./queue-test/queue-benchmark-test.sh                     → 기본 RPS 단계
#   ./queue-test/queue-benchmark-test.sh "100 200 300"       → 커스텀
#
# 사전 조건:
#   docker compose -f docker-compose.1node.yml up -d

set -eo pipefail

# ---- 설정 (환경변수로 override 가능, 기본값은 정밀 측정용) ----
RPS_LEVELS=(${1:-50 100 200 300 500 700 1000})
DURATION=${DURATION:-60}
WARMUP_DURATION=${WARMUP_DURATION:-60}
STABILIZE_WAIT=${STABILIZE_WAIT:-60}
COOLDOWN_WAIT=${COOLDOWN_WAIT:-60}
RUNS=${RUNS:-3}
RESET_WAIT=${RESET_WAIT:-2}

REDIS="queue-redis-master"
QUEUE="concert"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULT_BASE="$PROJECT_DIR/results"
# 결과 파일 접두어(1node/3node 등). 환경만 바꿔 비교 시 결과가 덮어써지지 않도록 분리.
LABEL="${LABEL:-1node}"
mkdir -p "$RESULT_BASE"

COLLECT="$SCRIPT_DIR/collect-metrics.sh"
LOAD_TEST="$SCRIPT_DIR/queue-load-test.js"

# ---- 헬퍼: 중앙값 (빈 회차 슬롯 제외, RUNS 개수 무관) ----
#   RUNS=1 일 때 미설정 슬롯(빈 문자열)을 0으로 세면 중앙값이 0으로 왜곡되므로 빈 값은 제외한다.
median3() {
    printf '%s\n' "$@" | awk 'NF' | sort -n | \
        awk '{ a[NR]=$0 } END { if (NR==0) print 0; else print a[int((NR+1)/2)] }'
}

echo ""
echo "======================================================="
echo "  대기열 부하 테스트 — 단일 인스턴스 베이스라인"
echo "  RPS 단계: ${RPS_LEVELS[*]}"
echo "  각 ${DURATION}초 × ${RUNS}회 (중앙값)"
echo "  워밍업: 목표 RPS의 30% × ${WARMUP_DURATION}초"
echo "======================================================="

for RATE in "${RPS_LEVELS[@]}"; do
    WARMUP_RATE=$(( RATE * 30 / 100 ))
    [ $WARMUP_RATE -lt 1 ] && WARMUP_RATE=1
    EXPECTED=$((RATE * DURATION))
    POLL_TIMEOUT=$((60 + EXPECTED / 100))

    echo ""
    echo "  ══ RPS=${RATE} (${EXPECTED}건 / ${DURATION}초) × ${RUNS}회 ══"

    R_SUCCESS=(); R_FAIL=(); R_DUP=(); R_HTTP_AVG=(); R_HTTP_P95=(); R_HTTP_P99=(); R_HTTP_MAX=(); R_CONSUME=()

    for ((run=0; run<RUNS; run++)); do
        RUN_NUM=$((run + 1))
        RUN_DIR="$RESULT_BASE/${LABEL}_rps${RATE}_run${RUN_NUM}"
        mkdir -p "$RUN_DIR"

        echo ""
        echo "    [${RUN_NUM}/${RUNS}회차]  → $RUN_DIR"

        # 워밍업
        echo "      [warmup]    ${WARMUP_RATE} RPS × ${WARMUP_DURATION}s"
        GOMAXPROCS=2 K6_RATE=$WARMUP_RATE K6_TEST_DURATION=$WARMUP_DURATION K6_RESULT_FILE=/dev/null \
            k6 run "$LOAD_TEST" > /dev/null 2>&1 || true

        # 안정화
        echo "      [stabilize] ${STABILIZE_WAIT}s"
        sleep $STABILIZE_WAIT

        # Redis 초기화
        echo "      [reset]     FLUSHALL + SLOWLOG RESET"
        docker exec $REDIS redis-cli FLUSHALL > /dev/null 2>&1 || true
        docker exec $REDIS redis-cli SLOWLOG RESET > /dev/null 2>&1 || true
        sleep $RESET_WAIT

        # docker stats 백그라운드 샘플링 시작
        "$COLLECT" "$RUN_DIR" start &
        STATS_PID=$!

        # 본 측정 시작 시각
        START=$(date +%s)

        # 본 측정
        echo "      [measure]   ${RATE} RPS × ${DURATION}s"
        GOMAXPROCS=2 K6_RATE=$RATE K6_TEST_DURATION=$DURATION K6_RESULT_FILE="$RUN_DIR/k6_result.txt" \
            k6 run "$LOAD_TEST"

        # docker stats 종료
        kill $STATS_PID 2>/dev/null || true
        wait $STATS_PID 2>/dev/null || true

        # 결과 파싱
        SUCCESS=0; FAILV=0; DUPV=0
        H_AVG=0; H_P95=0; H_P99=0; H_MAX=0
        if [ -f "$RUN_DIR/k6_result.txt" ]; then
            SUCCESS=$(grep "^success=" "$RUN_DIR/k6_result.txt" | cut -d= -f2 | tr -d '\r')
            FAILV=$(grep "^fail=" "$RUN_DIR/k6_result.txt" | cut -d= -f2 | tr -d '\r')
            DUPV=$(grep "^duplicate=" "$RUN_DIR/k6_result.txt" | cut -d= -f2 | tr -d '\r')
            H_AVG=$(grep "^http_avg=" "$RUN_DIR/k6_result.txt" | cut -d= -f2 | tr -d '\r')
            H_P95=$(grep "^http_p95=" "$RUN_DIR/k6_result.txt" | cut -d= -f2 | tr -d '\r')
            H_P99=$(grep "^http_p99=" "$RUN_DIR/k6_result.txt" | cut -d= -f2 | tr -d '\r')
            H_MAX=$(grep "^http_max=" "$RUN_DIR/k6_result.txt" | cut -d= -f2 | tr -d '\r')
        else
            echo "      k6_result.txt 없음"
        fi

        R_SUCCESS[$run]=${SUCCESS:-0}
        R_FAIL[$run]=${FAILV:-0}
        R_DUP[$run]=${DUPV:-0}
        R_HTTP_AVG[$run]=${H_AVG:-0}
        R_HTTP_P95[$run]=${H_P95:-0}
        R_HTTP_P99[$run]=${H_P99:-0}
        R_HTTP_MAX[$run]=${H_MAX:-0}

        # Redis 삽입 확인
        CONSUME=0
        REGISTERED=${SUCCESS:-0}
        if [ "${REGISTERED:-0}" -gt 0 ] 2>/dev/null; then
            echo "      [verify]    Redis 삽입 확인 중..."
            POLL_START=$(date +%s)
            TOTAL=0
            while true; do
                W=$(docker exec $REDIS redis-cli ZCARD "${QUEUE}:user-queue:wait" 2>/dev/null | tr -d '\r')
                A=$(docker exec $REDIS redis-cli ZCARD "${QUEUE}:user-queue:allow" 2>/dev/null | tr -d '\r')
                TOTAL=$(( ${W:-0} + ${A:-0} ))
                [ "$TOTAL" -ge "$REGISTERED" ] && break
                NOW=$(date +%s)
                if [ $((NOW - POLL_START)) -ge $POLL_TIMEOUT ]; then
                    echo "      타임아웃 (${POLL_TIMEOUT}초), 현재 ${TOTAL}/${REGISTERED}건"
                    break
                fi
                sleep 0.5
            done
            END=$(date +%s)
            CONSUME=$((END - START))
        fi
        R_CONSUME[$run]=$CONSUME

        # SLOWLOG / commandstats 덤프
        "$COLLECT" "$RUN_DIR" dump

        echo "      [완료]      성공=${SUCCESS}  p95=${H_P95}ms  소비=${CONSUME}s"

        # 회차 휴식
        if [ $run -lt $((RUNS - 1)) ]; then
            echo "      [cooldown]  ${COOLDOWN_WAIT}s"
            sleep $COOLDOWN_WAIT
        fi
    done

    # 3회 중앙값
    MED_SUCCESS=$(median3 "${R_SUCCESS[0]}" "${R_SUCCESS[1]}" "${R_SUCCESS[2]}")
    MED_FAIL=$(median3 "${R_FAIL[0]}" "${R_FAIL[1]}" "${R_FAIL[2]}")
    MED_DUP=$(median3 "${R_DUP[0]}" "${R_DUP[1]}" "${R_DUP[2]}")
    MED_HTTP_AVG=$(median3 "${R_HTTP_AVG[0]}" "${R_HTTP_AVG[1]}" "${R_HTTP_AVG[2]}")
    MED_HTTP_P95=$(median3 "${R_HTTP_P95[0]}" "${R_HTTP_P95[1]}" "${R_HTTP_P95[2]}")
    MED_HTTP_P99=$(median3 "${R_HTTP_P99[0]}" "${R_HTTP_P99[1]}" "${R_HTTP_P99[2]}")
    MED_HTTP_MAX=$(median3 "${R_HTTP_MAX[0]}" "${R_HTTP_MAX[1]}" "${R_HTTP_MAX[2]}")
    MED_CONSUME=$(median3 "${R_CONSUME[0]}" "${R_CONSUME[1]}" "${R_CONSUME[2]}")

    TOTAL_MED=$(awk "BEGIN { printf \"%.0f\", $MED_SUCCESS + $MED_FAIL + $MED_DUP }")
    if [ "$TOTAL_MED" -gt 0 ] 2>/dev/null; then
        SR=$(awk "BEGIN { printf \"%.1f\", ($MED_SUCCESS / $TOTAL_MED) * 100 }")
    else
        SR="0.0"
    fi
    THROUGHPUT=$(awk "BEGIN { if($MED_CONSUME>0) printf \"%.1f\", $MED_SUCCESS / $MED_CONSUME; else print \"0\" }")

    SUMMARY_FILE="$RESULT_BASE/${LABEL}_rps${RATE}_median.txt"
    cat > "$SUMMARY_FILE" <<RESULT_EOF
rps=$RATE
runs=$RUNS
success=$MED_SUCCESS
fail=$MED_FAIL
duplicate=$MED_DUP
success_rate=$SR
http_avg=$MED_HTTP_AVG
http_p95=$MED_HTTP_P95
http_p99=$MED_HTTP_P99
http_max=$MED_HTTP_MAX
consume_sec=$MED_CONSUME
throughput=$THROUGHPUT
RESULT_EOF

    echo ""
    echo "    [중앙값] 성공=${MED_SUCCESS}  HTTP avg=${MED_HTTP_AVG}ms  p95=${MED_HTTP_P95}ms  p99=${MED_HTTP_P99}ms  소비=${MED_CONSUME}s"
done

# ---- 결과 비교표 ----
echo ""
echo "[결과] 단일 인스턴스 베이스라인 (RPS별 ${RUNS}회 중앙값)"
echo ""

L="==========================================================================================="
D="-------------------------------------------------------------------------------------------"

echo "$L"
printf "  %-6s  %-8s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
       "RPS" "성공률" "avg(ms)" "p95(ms)" "p99(ms)" "소비(초)" "처리량"
echo "$D"

RECOMMENDED=""
P95_THRESHOLD=500

for RATE in "${RPS_LEVELS[@]}"; do
    FILE="$RESULT_BASE/${LABEL}_rps${RATE}_median.txt"
    if [ ! -f "$FILE" ]; then
        printf "  %-6s  %-8s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
               "$RATE" "N/A" "N/A" "N/A" "N/A" "N/A" "N/A"
        continue
    fi
    SR=$(grep "^success_rate=" "$FILE" | cut -d= -f2 | tr -d '\r')
    HTTP_AVG=$(grep "^http_avg=" "$FILE" | cut -d= -f2 | tr -d '\r')
    HTTP_P95=$(grep "^http_p95=" "$FILE" | cut -d= -f2 | tr -d '\r')
    HTTP_P99=$(grep "^http_p99=" "$FILE" | cut -d= -f2 | tr -d '\r')
    CONSUME=$(grep "^consume_sec=" "$FILE" | cut -d= -f2 | tr -d '\r')
    THRU=$(grep "^throughput=" "$FILE" | cut -d= -f2 | tr -d '\r')

    printf "  %-6s  %-8s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
           "$RATE" "${SR:-0}%" "${HTTP_AVG:-N/A}" "${HTTP_P95:-N/A}" \
           "${HTTP_P99:-N/A}" "${CONSUME:-N/A}s" "${THRU:-N/A}/s"

    if [ -n "$HTTP_P95" ]; then
        IS_UNDER=$(awk "BEGIN { print (${HTTP_P95} < ${P95_THRESHOLD}) ? 1 : 0 }")
        [ "$IS_UNDER" = "1" ] && RECOMMENDED=$RATE
    fi
done

echo "$D"
echo ""
if [ -n "$RECOMMENDED" ]; then
    echo "  안정적 처리 가능 RPS: ${RECOMMENDED}  (기준: HTTP p95 < ${P95_THRESHOLD}ms)"
else
    echo "  모든 RPS에서 p95 > ${P95_THRESHOLD}ms — 더 낮은 RPS 측정 필요"
fi
echo ""
echo "$L"
echo "  결과 디렉터리: $RESULT_BASE/"
echo ""
