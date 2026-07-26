#!/bin/bash
# 측정 보조 — docker stats 샘플링 + Redis SLOWLOG/commandstats 덤프
#
# 사용법:
#   collect-metrics.sh <RUN_DIR> start     → 1초 간격 docker stats CSV (무한 루프, 외부에서 kill)
#   collect-metrics.sh <RUN_DIR> dump      → SLOWLOG GET 100 / INFO commandstats / memory / stats 단발 덤프

set -eo pipefail

RUN_DIR="$1"
MODE="$2"

REDIS="queue-redis-master"
# 실행 중인 queueing 컨테이너를 자동 감지(1node/3node 공용) — 앱별 CPU 분산 수집용
APPS=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -E '^queueing0[0-9]$' | sort | tr '\n' ' ')
[ -z "$APPS" ] && APPS="queueing01"

if [ -z "$RUN_DIR" ] || [ -z "$MODE" ]; then
    echo "Usage: $0 <RUN_DIR> {start|dump}"
    exit 1
fi

mkdir -p "$RUN_DIR"

case "$MODE" in
    start)
        STATS_FILE="$RUN_DIR/docker_stats.csv"
        echo "timestamp,container,cpu_perc,mem_usage,mem_perc,net_io,block_io" > "$STATS_FILE"
        trap "exit 0" TERM INT
        while true; do
            TS=$(date +%s)
            docker stats --no-stream \
                --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}}' \
                $APPS "$REDIS" 2>/dev/null | \
                while IFS=',' read -r name cpu mem memp net block; do
                    echo "$TS,$name,$cpu,\"$mem\",$memp,\"$net\",\"$block\"" >> "$STATS_FILE"
                done
            sleep 1
        done
        ;;
    dump)
        docker exec $REDIS redis-cli SLOWLOG GET 100 > "$RUN_DIR/slowlog.txt"      2>&1 || true
        docker exec $REDIS redis-cli INFO commandstats > "$RUN_DIR/commandstats.txt" 2>&1 || true
        docker exec $REDIS redis-cli INFO memory       > "$RUN_DIR/redis_memory.txt" 2>&1 || true
        docker exec $REDIS redis-cli INFO stats        > "$RUN_DIR/redis_stats.txt"  2>&1 || true
        ;;
    *)
        echo "Unknown mode: $MODE (use 'start' or 'dump')"
        exit 1
        ;;
esac
