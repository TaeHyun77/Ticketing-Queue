#!/bin/bash
# 무릎(knee) 측정 — 단일 RPS 지점을 계획서 v2 프로토콜로 N회 측정
#
# 회차 절차 (LOAD-TEST-PLAN.md 6절):
#   FLUSHALL → 워밍업 500 RPS×60s → 재FLUSHALL → cpu.stat 전 스냅샷
#   → 본 측정 60s (측정 중 k6/nginx/앱/redis CPU 샘플링 + k6 cgroup throttled 수집)
#   → cpu.stat 후 스냅샷 → 앱 재시작(healthy 대기) → 냉각 150s
#
# 판정 기준 (지점 단위, 3회 중앙값):
#   에러율 0% / dropped_iterations 0 / p99 ≤ 200ms / 앱 throttled_usec 증가 0
# 회차 유효성: dropped>0 또는 k6 CPU ≥ 상한의 90% 또는 k6 throttled 증가 → 부하 생성 한계 회차
#
# 사용법:
#   ./queue-test/queue-knee-test.sh <RPS> <세션라벨>
#   예) ./queue-test/queue-knee-test.sh 3000 knee_20260724_1node
#
# 환경변수 override:
#   RUNS(3) DURATION(60) WARMUP_RATE(500) WARMUP_DURATION(60) COOLDOWN(150)
#   APPS("queueing01")  K6_CPUSET("2,3,8,9")  K6_CPUS(4.0)

set -uo pipefail

RATE=${1:?RPS 필요}
SESSION=${2:?세션 라벨 필요}
RUNS=${RUNS:-3}
DURATION=${DURATION:-60}
WARMUP_RATE=${WARMUP_RATE:-500}
WARMUP_DURATION=${WARMUP_DURATION:-60}
COOLDOWN=${COOLDOWN:-150}
APPS=${APPS:-queueing01}
NGINX=integrated-nginx
REDIS=queue-redis-master
NET=integrated-queueing-system_integrated-net
K6_IMG=grafana/k6:1.6.1
K6_CPUSET=${K6_CPUSET:-2,3,8,9}
K6_CPUS=${K6_CPUS:-4.0}
# k6 CPU 상한의 90% (코어수×100×0.9) — 이상 관측 시 부하 생성 한계 회차
K6_CPU_LIMIT=$(awk "BEGIN { printf \"%.0f\", $K6_CPUS * 90 }")

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="$SCRIPT_DIR/results/$SESSION/rps${RATE}"
mkdir -p "$OUT"

median() {
    printf '%s\n' "$@" | awk 'NF' | sort -n | \
        awk '{ a[NR]=$0 } END { if (NR==0) print 0; else print a[int((NR+1)/2)] }'
}

flushall() {
    docker exec $REDIS redis-cli FLUSHALL > /dev/null 2>&1
    docker exec $REDIS redis-cli SLOWLOG RESET > /dev/null 2>&1
}

# k6 컨테이너 실행: $1=이름 $2=rate $3=duration $4=결과파일(세션상대) $5=stdout 저장 파일
k6run() {
    docker rm -f "$1" > /dev/null 2>&1 || true
    docker run --rm -i --name "$1" --network $NET \
        --cpuset-cpus="$K6_CPUSET" --cpus="$K6_CPUS" -m 2g \
        -e GOMAXPROCS=${K6_CPUS%%.*} \
        -v "$SCRIPT_DIR:/scripts" \
        -e K6_TARGET_URL="http://nginx:90/queue/register" \
        -e K6_RATE=$2 -e K6_TEST_DURATION=$3 \
        -e K6_RESULT_FILE="/scripts/results/$SESSION/rps${RATE}/$4" \
        $K6_IMG run /scripts/queue-load-test.js > "$5" 2>&1
}

# 본 측정 중 CPU 샘플링: $1=CSV 경로 (k6-run 이 사라지면 종료)
sampler() {
    local csv=$1
    echo "ts,name,cpu,mem" > "$csv"
    # k6-run 등장 대기 (최대 20초)
    for _ in $(seq 1 20); do
        docker ps --format '{{.Names}}' | grep -q '^k6-run$' && break
        sleep 1
    done
    while docker ps --format '{{.Names}}' | grep -q '^k6-run$'; do
        local ts=$(date +%s)
        docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' \
            k6-run $NGINX $APPS $REDIS 2>/dev/null | sed "s/^/$ts,/" >> "$csv"
        docker exec k6-run sh -c 'cat /sys/fs/cgroup/cpu.stat 2>/dev/null' 2>/dev/null | \
            awk -v ts=$ts '{print ts",k6cgroup,"$1","$2}' >> "${csv%.csv}_k6stat.csv"
    done
}

wait_healthy() {
    local app deadline=$(( $(date +%s) + 180 ))
    for app in $APPS; do
        while true; do
            local st=$(docker inspect -f '{{.State.Health.Status}}' "$app" 2>/dev/null)
            [ "$st" = "healthy" ] && break
            if [ $(date +%s) -gt $deadline ]; then
                echo "      [경고] $app healthcheck 타임아웃 (state=$st)"
                break
            fi
            sleep 3
        done
    done
}

echo ""
echo "══════ 무릎 측정  RPS=${RATE} × ${DURATION}s × ${RUNS}회  (세션: $SESSION) ══════"
echo "  앱: $APPS / k6: cpuset=$K6_CPUSET cpus=$K6_CPUS (유효 상한 ${K6_CPU_LIMIT}%)"

R_P99=(); R_FAIL=(); R_DROP=(); R_THROTTLE=(); R_K6CPU=(); R_NGCPU=()

for ((run=1; run<=RUNS; run++)); do
    echo ""
    echo "  ── [${run}/${RUNS}회차] ──"

    echo "      [reset]    FLUSHALL + SLOWLOG RESET"
    flushall
    sleep 2

    echo "      [warmup]   ${WARMUP_RATE} RPS × ${WARMUP_DURATION}s"
    k6run k6-warmup $WARMUP_RATE $WARMUP_DURATION "r${run}_warmup.txt" "$OUT/r${run}_warmup_stdout.txt" || \
        echo "      [경고] 워밍업 k6 비정상 종료"

    echo "      [reset]    재FLUSHALL (빈 큐 상태 고정)"
    flushall
    sleep 2

    for app in $APPS; do
        docker exec "$app" cat /sys/fs/cgroup/cpu.stat > "$OUT/r${run}_${app}_cpustat_before.txt" 2>/dev/null
    done

    echo "      [measure]  ${RATE} RPS × ${DURATION}s"
    sampler "$OUT/r${run}_stats.csv" &
    SAMPLER_PID=$!
    k6run k6-run $RATE $DURATION "r${run}_result.txt" "$OUT/r${run}_stdout.txt" || \
        echo "      [경고] 본 측정 k6 비정상 종료"
    wait $SAMPLER_PID 2>/dev/null || true

    for app in $APPS; do
        docker exec "$app" cat /sys/fs/cgroup/cpu.stat > "$OUT/r${run}_${app}_cpustat_after.txt" 2>/dev/null
    done

    # ---- 회차 파싱 ----
    RES="$OUT/r${run}_result.txt"
    P99=$(grep '^http_p99=' "$RES" 2>/dev/null | cut -d= -f2)
    FAILV=$(grep '^fail=' "$RES" 2>/dev/null | cut -d= -f2)
    SUCCESS=$(grep '^success=' "$RES" 2>/dev/null | cut -d= -f2)
    DUP=$(grep '^duplicate=' "$RES" 2>/dev/null | cut -d= -f2)
    DROP=$(grep -E 'dropped_iterations' "$OUT/r${run}_stdout.txt" 2>/dev/null | \
        grep -oE '[0-9]+' | head -1)
    DROP=${DROP:-0}

    # 앱 throttled_usec 증가 (다중 앱은 합산)
    THROTTLE=0
    for app in $APPS; do
        B=$(grep '^throttled_usec' "$OUT/r${run}_${app}_cpustat_before.txt" 2>/dev/null | awk '{print $2}')
        A=$(grep '^throttled_usec' "$OUT/r${run}_${app}_cpustat_after.txt" 2>/dev/null | awk '{print $2}')
        THROTTLE=$(( THROTTLE + ${A:-0} - ${B:-0} ))
    done

    K6CPU=$(awk -F',' '$2=="k6-run"{gsub(/%/,"",$3); if($3+0>m) m=$3+0} END{printf "%.0f", m}' "$OUT/r${run}_stats.csv")
    NGCPU=$(awk -F',' -v n="$NGINX" '$2==n{gsub(/%/,"",$3); if($3+0>m) m=$3+0} END{printf "%.0f", m}' "$OUT/r${run}_stats.csv")
    K6THROTTLE=$(awk -F',' '$3=="throttled_usec"{v=$4} END{print v+0}' "$OUT/r${run}_stats_k6stat.csv" 2>/dev/null)
    K6THROTTLE=${K6THROTTLE:-0}

    VALID="유효"
    [ "${DROP:-0}" -gt 0 ] && VALID="부하생성한계(dropped)"
    [ "${K6CPU:-0}" -ge "$K6_CPU_LIMIT" ] && VALID="부하생성한계(k6 CPU ${K6CPU}%)"
    [ "${K6THROTTLE:-0}" -gt 0 ] && VALID="부하생성한계(k6 throttled)"

    R_P99[$run]=${P99:-0}; R_FAIL[$run]=${FAILV:-0}; R_DROP[$run]=${DROP:-0}
    R_THROTTLE[$run]=$THROTTLE; R_K6CPU[$run]=${K6CPU:-0}; R_NGCPU[$run]=${NGCPU:-0}

    LINE="run=$run rate=$RATE success=${SUCCESS:-0} fail=${FAILV:-0} dup=${DUP:-0} p99=${P99:-0} dropped=${DROP:-0} app_throttled_delta=$THROTTLE k6_cpu_max=${K6CPU:-0} nginx_cpu_max=${NGCPU:-0} k6_throttled=$K6THROTTLE valid=$VALID"
    echo "$LINE" >> "$OUT/summary.txt"
    echo "      [결과]     p99=${P99}ms fail=${FAILV} dropped=${DROP} 앱스로틀Δ=${THROTTLE} k6CPU=${K6CPU}% nginxCPU=${NGCPU}% → $VALID"

    echo "      [restart]  앱 재시작 + healthy 대기"
    docker restart $APPS > /dev/null 2>&1
    wait_healthy

    if [ $run -lt $RUNS ]; then
        echo "      [cooldown] ${COOLDOWN}s"
        sleep $COOLDOWN
    fi
done

# ---- 지점 판정 (3회 중앙값) ----
MED_P99=$(median "${R_P99[@]}")
MED_FAIL=$(median "${R_FAIL[@]}")
MED_DROP=$(median "${R_DROP[@]}")
MED_THROTTLE=$(median "${R_THROTTLE[@]}")
MAX_K6CPU=$(printf '%s\n' "${R_K6CPU[@]}" | sort -n | tail -1)
MAX_NGCPU=$(printf '%s\n' "${R_NGCPU[@]}" | sort -n | tail -1)

VERDICT="통과"
REASONS=""
awk "BEGIN{exit !($MED_P99 > 200)}" && { VERDICT="초과"; REASONS="$REASONS p99>${MED_P99}ms"; }
[ "$MED_FAIL" -gt 0 ] && { VERDICT="초과"; REASONS="$REASONS 에러${MED_FAIL}건"; }
[ "$MED_DROP" -gt 0 ] && { VERDICT="초과"; REASONS="$REASONS dropped${MED_DROP}"; }
[ "$MED_THROTTLE" -gt 0 ] && { VERDICT="초과"; REASONS="$REASONS 앱스로틀Δ${MED_THROTTLE}"; }

{
    echo "---"
    echo "point rate=$RATE med_p99=$MED_P99 med_fail=$MED_FAIL med_dropped=$MED_DROP med_app_throttled=$MED_THROTTLE max_k6_cpu=$MAX_K6CPU max_nginx_cpu=$MAX_NGCPU verdict=$VERDICT reasons=${REASONS:-없음}"
} >> "$OUT/summary.txt"

echo ""
echo "══════ RPS=${RATE} 지점 판정: $VERDICT ${REASONS:+(사유:$REASONS)} ══════"
echo "  중앙값 p99=${MED_P99}ms fail=${MED_FAIL} dropped=${MED_DROP} 앱스로틀Δ=${MED_THROTTLE}"
echo "  최대 k6 CPU=${MAX_K6CPU}% / nginx CPU=${MAX_NGCPU}%"
echo "  요약: $OUT/summary.txt"
