#!/bin/bash
# 두 머신 테스트용 맥북(서버) 측 헬퍼
#   데스크탑에서 k6 를 쏘기 '직전' 맥북에서 이 스크립트 하나만 실행하면:
#     1) Redis FLUSHALL (큐 상태 초기화)
#     2) 실행 중인 queueing 컨테이너 + Redis 의 CPU/메모리를 1초 간격으로 샘플링
#     3) 종료 후 앱별 peak CPU / Redis / 호스트 총합 요약 출력
#
# 사용법:
#   ./queue-test/run-monitor.sh [라벨] [샘플링초]
#   예) ./queue-test/run-monitor.sh spike_2machine 75
#
# 권장 흐름:
#   맥북> ./queue-test/run-monitor.sh spike_2machine 75   # 먼저 실행(샘플링 시작)
#   데스크탑> K6_TARGET_URL=http://192.168.0.38:8079/queue/register k6 run queue-spike-test.js

set -uo pipefail

LABEL="${1:-monitor}"
DURATION="${2:-75}"
REDIS="queue-redis-master"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUT_DIR="$PROJECT_DIR/results/$LABEL"
mkdir -p "$OUT_DIR"
CSV="$OUT_DIR/docker_stats.csv"

# 실행 중인 queueing 컨테이너 자동 감지(1node/3node 공용)
APPS=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -E '^queueing0[0-9]$' | sort | tr '\n' ' ')
[ -z "$APPS" ] && { echo "실행 중인 queueing 컨테이너가 없습니다. 스택을 먼저 기동하세요."; exit 1; }

echo "대상 앱: $APPS"
echo "[reset] Redis FLUSHALL"
docker exec "$REDIS" redis-cli FLUSHALL >/dev/null 2>&1 && echo "  완료" || echo "  실패(계속 진행)"

echo "[sample] ${DURATION}초 동안 1초 간격 CPU 샘플링 시작 → 지금 데스크탑에서 k6 를 실행하세요."
echo "timestamp,container,cpu_perc,mem_usage" > "$CSV"
for ((i=0; i<DURATION; i++)); do
    TS=$(date +%s)
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' $APPS "$REDIS" 2>/dev/null | \
        while IFS=',' read -r name cpu mem; do
            echo "$TS,$name,$cpu,\"$mem\"" >> "$CSV"
        done
done

echo ""
echo "-------------------------------------------------------"
echo "  CPU 요약 ($LABEL)"
echo "-------------------------------------------------------"
awk -F',' 'NR>1{
    gsub(/%/,"",$3); c=$3+0;
    peak[$2]=(c>peak[$2]?c:peak[$2]);
    if ($2 !~ /redis/) { ts_app[$1]+=c }   # 타임스탬프별 앱 합계 → 호스트 앱부하 peak
}
END{
    for (n in peak) if (n !~ /redis/) printf "  %-12s peak %.0f%%\n", n, peak[n];
    for (n in peak) if (n ~ /redis/)  printf "  %-12s peak %.0f%%\n", n, peak[n];
    maxsum=0; for (t in ts_app) if (ts_app[t]>maxsum) maxsum=ts_app[t];
    printf "  ----\n  앱 합계 최대 %.0f%% (3앱×2코어=600%% 상한)\n", maxsum;
}' "$CSV"
echo "  결과 CSV: $CSV"
echo "-------------------------------------------------------"
