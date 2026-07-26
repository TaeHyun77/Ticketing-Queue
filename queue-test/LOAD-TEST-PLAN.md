# 대기열 시스템 부하 테스트 재수행 계획서 (v2)

### 개정 이력
---
v1 → v2 반영 사항 (코드 실측 검증 기반 8건)

1. 회차 절차에 Redis 초기화(FLUSHALL) 단계 신설 — uid 재사용·데이터 누적 오염 차단 (6절)
2. k6 실행 명령의 마운트 경로·결과 파일 보존 경로 수정 (5절)
3. 메모리 예산 항목 신설 — k6 메모리 제한, VM 메모리 실측 (2절, 5절, 6절)
4. dropped_iterations 규칙에 '전달 불가능한 부하' 분기 추가 (1절)
5. 유효성 판정에 k6 자체 CPU 포화 검증 추가 (6절)
6. 에러율 0% SLO의 적용 범위 명시 — spike 구간은 결과로 보고 (1절)
7. Redis 재조정 판정 기준을 master 단독이 아닌 core 6 합산 사용률로 변경 (3절)
8. 한계 절에 dockerd·VM 시스템 프로세스의 비통제 항목 추가 (8절)

부수 반영: 스트레스 스크립트 점검 결과 확정 기입 — 분류 시점 문제 없음, keep-alive 기본 사용,
preAllocatedVUs 기준 충족 확인. 남은 조치는 워밍업 확보뿐 (4절)

### 0. 문서 목적과 배경
---
이 문서는 대기열 시스템 부하 테스트를 신뢰성 있게 재수행하기 위한 전체 실행 계획입니다
기존 측정에서 발견된 문제(k6 co-location 경합 오염, 스트레스/스파이크 간 측정 조건 불일치, 표기 기준 혼재, 스파이크 스크립트의 구간 분류 편향)를 통제한 상태에서 전 테스트를 재실행하는 것이 목표입니다
절대 수치는 macOS Docker Desktop 환경 한정 값이며, 이 계획의 신뢰성 목표는 상대 비교(1노드 vs 3노드, 부하 프로파일 간 비교)의 정합성 확보입니다

### 1. SLO 사전 선언
---
**합격 기준 (테스트 실행 전 확정, 사후 변경 금지)**

[ 요청 품질 ]
- 에러율 0% — 적용 범위: 스트레스 전 구간, 스파이크의 baseline/recovery 구간
  (스파이크의 spike 구간 에러는 회차 무효 사유가 아니라 측정 결과로 보고한다)
- dropped_iterations = 0 (0이 아니면 해당 회차 무효 처리, 단 아래 분기 적용)

[ dropped 처리 분기 ]
- dropped > 0 발생 시 1차 조치: maxVUs 상향 후 재실행
- maxVUs 상향(및 k6 메모리 한도 내)으로도 dropped가 0이 되지 않으면, 해당 스파이크 배율은
  **'전달 불가능한 부하'로 판정**하고 목표 유입 대비 실제 발사율(achieved/offered)을 병기해 보고한다
- 이 분기는 사전 규칙이며, 측정 후 임의로 완화하지 않는다

[ 지연 기준 ]
- p99 ≤ 200ms (스트레스 지속 부하 기준)
- 스파이크 회복 판정: recovery p99 ≤ baseline p99 × 2 (기존 스크립트 기준 유지)

[ 자원 기준 ]
- 측정 창 동안 앱 컨테이너 cgroup throttled_usec 증가 없음 (증가 시 해당 RPS는 포화 판정)
- 측정 창 동안 k6 컨테이너 CPU < 180% (2코어 대비) 및 throttled_usec 증가 없음
  (초과 시 클라이언트 측 병목으로 해당 회차 무효 — 5절 k6 검증 참조)

[ 적정 한계의 정의 ]
위 기준을 3회 측정 중앙값으로 모두 만족하는 최대 RPS를 단일 노드의 적정 한계로 정의합니다

### 2. 환경 정의서
---
**실측 후 기입 (추정치 금지)**

[ 수집 명령 ]
```bash
sysctl -n machdep.cpu.brand_string      # 칩 모델
sysctl -n hw.ncpu                        # 논리 코어 수
sysctl -n hw.memsize                     # 물리 메모리
docker info | grep -E "CPUs|Total Memory|Server Version"
java -version && redis-server --version && nginx -v && k6 version
docker exec queueing01 sh -c 'head -3 /proc/meminfo'   # VM 전체 메모리·가용량 (기동 후)
```

[ 기입 표 ]
| 항목 | 값 |
| --- | --- |
| 호스트 칩 / 코어 / 메모리 | (실측 기입) |
| Docker Desktop VM | 10 vCPU / 7.65GB |
| VM 가용 메모리 (3노드 기동 + k6 대기 상태) | (실측 기입 — 스모크 단계에서 측정) |
| 앱 인스턴스 | cpus 2.0 (CFS 쿼터) + cpuset, mem 1.5g, -Xms512m -Xmx512m G1GC |
| Redis | master 1코어 + slave×2 + sentinel×3, Sentinel 모드 |
| 진입점 | nginx :90 (호스트 8079 매핑) |
| 부하 도구 | k6 1.6.1 (컨테이너, grafana/k6:1.6.1, mem 2g 제한) |
| 제외 대상 | reserve / MySQL (대기열 격리) |

[ 메모리 예산 (신설) ]
- VM 7.65GB 대비 고정 소비: 앱 1.5g×3 = 4.5g (3노드) + redis master 1g + slave·sentinel (무제한, 실측 기입)
- k6는 VU당 대략 0.5~1MB를 소비하므로 스파이크(preAllocatedVUs 3,000, maxVUs 10,000)에서 수 GB까지 커질 수 있다
- 통제: k6 컨테이너에 `-m 2g` 제한을 건다. k6가 OOM으로 죽으면 해당 배율의 VU 수요가
  메모리 한도를 초과한 것이므로 1절의 '전달 불가능한 부하' 분기로 처리한다
- VM 스왑은 cpuset으로 잡히지 않는 오염원이므로, 스모크 단계에서 스파이크 프로파일 기준
  VM 가용 메모리를 실측해 위 표에 기입하고, 여유가 1GB 미만이면 본 측정 진입을 중단한다

[ 환경 특성 명시 사항 ]
- macOS Docker Desktop은 하이퍼바이저 VM 위에서 동작하는 2단 구조입니다 (맥 → VM 10 vCPU → 컨테이너 cgroup)
- Apple Silicon의 P/E 코어 비대칭은 VM 레벨에서 통제 불가하며 보고서 한계 절에 기재합니다
- 절대 수치는 이 환경 한정이며 프로덕션 용량 산정에 직접 사용할 수 없습니다

### 3. CPU 배치 (cpuset 전면 적용)
---
**배치표 (1노드·3노드 공통, 앱 대수만 차이)**

[ 코어 할당 ]
| 컴포넌트 | cpuset | 쿼터 | 비고 |
| --- | --- | --- | --- |
| queueing01 | 0,1 | 2.0 | 측정 대상 |
| queueing02 | 2,3 | 2.0 | 3노드만 |
| queueing03 | 4,5 | 2.0 | 3노드만 |
| redis-master | 6 | 1.0 | 기존 2.0에서 하향 (실측 최대 49%) |
| redis-slave×2, sentinel×3 | 6 | - | master와 합산 배치 |
| nginx | 7 | 1.0 | 신규 제한 (기존 무제한) |
| prometheus, grafana | 7 | - | 측정 중 docker stop |
| k6 | 8,9 | 2.0 | docker run 시 지정 |

[ 배치 원칙 ]
- 1노드 compose는 queueing01만 남기고 나머지 배치 값을 글자 그대로 유지합니다 (코어 2~5가 유휴가 되는 것이 정상)
- cpuset이 비어 있는 컨테이너가 하나라도 있으면 격리 구멍이므로 기동 후 전수 검증합니다
- **Redis 재조정 판정은 master 단독이 아니라 core 6 합산 사용률로 한다.**
  master 쓰기마다 slave 2대가 같은 코어에서 복제를 적용하므로 master 단독 수치는
  slave에게 뺏긴 시간을 보여주지 못한다. 첫 측정에서 core 6 합산(master + slave×2 + sentinel×3의
  docker stats CPU 합)이 1코어 기준 90% 이상이면 재조정을 논의합니다

### 4. 코드·설정 수정 사항
---
**compose 수정 (두 파일 공통)**

[ 수정 목록 ]
- 각 서비스에 3절 배치표대로 cpuset 추가
- redis-master: cpus 2.0 → 1.0
- nginx: cpus 1.0 신규 추가
- 그 외 기존 설정(mem_limit, JAVA_OPTS, healthcheck) 변경 금지

**스파이크 스크립트 패치 (필수)**

[ 문제 ]
구간 분류가 응답 수신 시점 기준이라 (queue-spike-test.js:88, `bucket()` 호출이 `http.post` 이후)
스파이크 막판의 느린 응답이 전이 구간으로 빠져 spike p99가 좋게 나오는 생존자 편향이 있습니다

[ 수정 ]
```javascript
export default function () {
    var t0 = exec.instance.currentTestRunDuration;  // 발사 시점 기록
    // ... http.post 및 success 판정 (기존 유지) ...
    var b = bucket(t0);                             // 발사 시점으로 분류
    // ... 이하 기존 유지 ...
}
```

[ 유지할 부분 ]
콜드스타트 3초 제외, 전이 구간 집계 배제, dropped_iterations 출력, 회복 판정 기준은 잘 설계되어 있으므로 그대로 유지합니다

**스트레스 스크립트 점검 (실측 확정)**

[ 점검 결과 ]
- 분류 시점 문제: **없음** — 구간 분류 없이 전체 http_req_duration을 집계하므로 해당 없음
- noConnectionReuse: **없음** — keep-alive 기본 사용, 조치 불요
- preAllocatedVUs: **충족** — 3,000 RPS 기준 min(6000, 2000) = 2000 ≥ 기준치 1,200
- 남은 조치: 워밍업 500 RPS × 60s를 별도 실행으로 확보하고 집계에서 제외한다 (6절 회차 절차에 포함)

### 5. k6 컨테이너 실행 방법
---
**실행 명령 (스파이크 예시)**

[ 명령 ]
```bash
docker run --rm -i \
  --name k6-run \
  --network <네트워크명>_integrated-net \
  --cpuset-cpus="8,9" --cpus="2.0" -m 2g \
  -v "$PWD/queue-test:/scripts" \
  -e K6_TARGET_URL="http://nginx:90/queue/register" \
  -e K6_SPIKE_RATE="3000" -e K6_HOLD="30" \
  -e K6_RESULT_FILE="/scripts/results/<타임스탬프>/spike_r1.txt" \
  grafana/k6:1.6.1 run /scripts/queue-spike-test.js
```

[ 주의 사항 ]
- 스크립트 위치는 `queue-test/` 이므로 마운트는 `$PWD/queue-test:/scripts` 입니다 (`$PWD/k6` 아님)
- `--rm` 컨테이너 내부에 결과를 쓰면 종료와 함께 사라지므로, K6_RESULT_FILE은 반드시
  마운트된 경로(`/scripts/results/...`) 하위로 지정하고 실행 전 `mkdir -p queue-test/results/<타임스탬프>` 로 디렉터리를 만들어 둡니다
- `--name k6-run` 은 측정 중 k6 자체 CPU 검증(docker stats 샘플링)에 필요합니다
- `-m 2g` 는 메모리 예산(2절)에 따른 제한입니다
- 대상 포트는 8079가 아니라 90입니다 (8079는 호스트 매핑 포트이며 docker 내부망에서는 컨테이너 포트 90으로 접속)
- localhost는 k6 컨테이너 자기 자신을 가리키므로 반드시 서비스명 nginx를 사용합니다
- 네트워크명은 docker network ls로 확인합니다 (통상 폴더명 소문자 기준 integrated-queueing-system_integrated-net)
- 호스트 네이티브 k6와 컨테이너 k6를 혼용하지 않습니다 (전 테스트 동일 경로 원칙)

**k6 자체 병목 검증 (신설)**

[ 방법 ]
- 본 측정 중 별도 터미널에서 `docker stats --no-stream k6-run` 을 2~3회 샘플링한다
- CPU가 180%(2코어 대비) 이상으로 관측되거나, k6 컨테이너의 cgroup throttled_usec이 증가하면
  클라이언트 측 병목으로 해당 회차를 무효 처리한다 (1절 자원 기준)
- 이번 재측정의 존재 이유가 k6 경합 오염 제거이므로, 전용 코어 부여만으로 충분하다고
  가정하지 않고 매 회차 검증한다

### 6. 측정 프로토콜 (매 회차 공통)
---
**사전 준비**

[ 맥 상태 통제 ]
- 전원 어댑터 연결, 타 앱 종료, caffeinate -dims 실행
- docker compose up -d 후 전 컨테이너 healthcheck 통과 확인

[ 격리 검증 ]
```bash
docker ps -q | xargs docker inspect \
  --format '{{.Name}} cpuset={{.HostConfig.CpusetCpus}} nano={{.HostConfig.NanoCpus}}'
```
전 컨테이너의 cpuset이 배치표와 일치해야 하며 빈 값이 있으면 진행 중단

[ 모니터링 중지 ]
docker stop prometheus grafana (지표는 cgroup 직독과 k6 출력으로 수집)

**회차 절차**

[ 실행 순서 ]
1. **Redis 초기화 (신설, 필수)**: `docker exec queue-redis-master redis-cli FLUSHALL` 및
   `docker exec queue-redis-master redis-cli SLOWLOG RESET`
   - 근거: 두 스크립트 모두 uid를 `prefix-__VU-__ITER` 로 생성하므로 k6를 회차마다 새로 실행하면
     같은 uid가 재사용된다. Redis를 비우지 않으면 대량 ALREADY_EXISTS(중복 체크 조기 반환 =
     훨씬 싼 경로)로 응답 시간이 오염되고, 회차 간 ZSET 크기 누적으로 회차 독립성이 깨진다
   - **주의: 호스트 brew redis가 6379를 선점하고 있으므로 호스트에서 `redis-cli FLUSHALL` 을
     치면 엉뚱한 redis를 비운다. 반드시 `docker exec` 경유로 실행한다**
2. 측정 전 스냅샷: 각 앱에서 cat /sys/fs/cgroup/cpu.stat 저장 (v1이면 /sys/fs/cgroup/cpu/cpu.stat)
3. 워밍업: 500 RPS × 60s (집계 제외)
4. **워밍업 후 Redis 재초기화 (신설)**: 1번과 동일한 FLUSHALL을 다시 실행한다
   - 본 측정은 항상 **빈 큐 상태**에서 시작하는 것으로 고정한다 (JIT은 유지되고 데이터만 리셋)
   - 이렇게 해야 회차 간·프로파일 간 데이터 상태 조건이 동일해진다
5. 본 측정: 목표 RPS × 60s (스파이크는 프로파일대로). 측정 중 k6 CPU 샘플링 (5절)
6. 측정 후 스냅샷: cpu.stat 재저장, nr_throttled / throttled_usec 차분 기록
7. 유효성 판정: dropped_iterations = 0 확인 (아니면 1절 dropped 분기 적용),
   k6 CPU < 180% 확인 (아니면 회차 무효)
8. 앱 컨테이너 재시작 (JIT/GC 상태 초기화로 회차 독립성 확보)
9. 냉각 대기 150초 (열 스로틀링 방지)

**반복·집계 규칙**

[ 규칙 ]
- 전 지표(p95, p99, CPU, 스로틀링) 공통으로 3회 실행, 중앙값 + [최소~최대] 병기
- 회차 간 p99가 2배 이상 벌어지면 5회로 확대하거나 원인 규명 후 채택
- RPS 오름차순 고정 실행 시 열 축적이 고부하에 불리하므로 회차별 순서 셔플 또는 냉각 준수
- CPU 포화 판정은 docker stats가 아니라 throttled_usec 증가 여부로 판정합니다

### 7. 테스트 실행 순서 (우선순위)
---
**Phase 1 — 스모크 (필수 관문)**

[ 내용 ]
3노드 기동 후 k6 컨테이너로 50 RPS × 60s 실행
에러 0%, p99 한 자릿수 ms 확인 (이상 시 격리 미비 신호이므로 본 측정 진입 금지)
**추가 관문 (신설)**: 스파이크 프로파일 기준 VM 가용 메모리 실측
(`docker exec queueing01 sh -c 'head -3 /proc/meminfo'`) — 여유 1GB 미만이면 본 측정 진입 금지 (2절 메모리 예산)

**Phase 2 — 1노드 스파이크 재측정 (최우선)**

[ 내용 ]
1노드 compose + 격리 조건으로 스파이크 3회
목적: 기존 13.8배 수치가 오염 조건(비격리 142.7ms) 대비 비교였으므로 공정한 분자 확보
예상: 기존보다 개선 배율이 줄어들 수 있으며 그것이 정직한 값입니다

**Phase 3 — 3노드 스파이크 재측정**

[ 내용 ]
동일 격리 조건 + 패치된 스크립트로 3회 (기존 (B) 결과와 경향 대조)

**Phase 4 — 스트레스 재측정**

[ 내용 ]
1노드: 1,500 / 2,000 / 2,500 / 3,000 RPS 각 3회 (2,000→2,500 비단조 구간 재검증 포함)
3노드: 1,500 / 3,000 RPS 각 3회
시간이 허용되면 전 RPS 지점 수행, 부족하면 위 핵심 지점만으로 충분합니다

**Phase 5 — 정합성 테스트**

[ 내용 ]
기존 정합성 테스트(씨딩 2,000 + 버스트 1,000, 정원 400, 무유실·무중복·선착순)를 격리 조건에서 1회 재확인

### 8. 결과 정리와 보고서 구조
---
**보고서 목차**

[ 구성 ]
1. 목표와 SLO (1절 내용)
2. 환경 정의서 (2절 표)
3. 측정 프로토콜 요약 (6절 요약)
4. 측정 오염의 발견과 통제 — 초기 측정에서 k6 co-location 경합으로 p99가 최대 수 배 오염됨을 (A)/(B) 대조로 확인하고, cpuset 격리 + k6 컨테이너화를 표준 조건으로 전환한 과정을 서술합니다
5. 결과 — 표마다 집계 기준(중앙값/범위, 대당/합산, 스로틀링 유무) 각주 필수
6. 한계 —
   - macOS VM 2단 구조, P/E 코어 비대칭, 절대 수치의 환경 종속성
   - 무선 분리 시도의 기각 사유 (지터가 측정 신호를 상회)
   - **dockerd·VM 시스템 프로세스는 cpuset 대상이 아니므로 측정 코어 어디에든 스케줄될 수 있다 (신설)**
     — 코어 0~9 전량을 워크로드에 할당한 구조상 통제 불가능하며, 잔여 오염원으로 명시한다

**표기 통일 규칙**

[ 규칙 ]
- CPU는 대당 기준 % (2코어 상한 200% 명시), 합산 표기 금지
  (예외: 3절 core 6 합산 사용률은 Redis 재조정 판정 전용 내부 지표이며 보고서 결과 표에는 쓰지 않는다)
- docker stats 순간값이 쿼터를 초과 표기될 수 있음을 각주로 명시합니다
- 개선 배율은 동일 조건 쌍끼리만 산출합니다 (격리 vs 격리, 비격리 vs 비격리)

### 9. 체크리스트
---
**실행 전 최종 점검**

[ 항목 ]
- [ ] compose 두 파일에 cpuset 반영, redis-master 1.0, nginx 1.0
- [ ] 스파이크 스크립트 t0 발사 시점 분류 패치
- [ ] ~~스트레스 스크립트 분류 시점·preAllocatedVUs·keep-alive 점검~~ → 점검 완료 (4절), 워밍업 절차만 확보
- [ ] queue-test/results/<타임스탬프>/ 디렉터리 생성 및 K6_RESULT_FILE 마운트 경로 지정
- [ ] 기동 후 inspect 전수 검증 통과
- [ ] 모니터링 컨테이너 중지
- [ ] 맥 전원 연결 + caffeinate
- [ ] 스모크 통과 (50 RPS, 에러 0%) + VM 가용 메모리 1GB 이상 확인
- [ ] 회차마다: FLUSHALL(docker exec 경유) → cpu.stat 스냅샷 → 워밍업 → FLUSHALL → 본 측정
      → k6 CPU 샘플 → dropped 검증 → 앱 재시작 → 냉각 150초
- [ ] 결과 JSON/로그 회차별 보존 (queue-test/results/ 하위 타임스탬프 폴더)
