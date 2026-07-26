// 대기열 부하 테스트 — 고정 RPS로 등록 요청을 전송
//
// 측정 범위 (HTTP 응답 시간 = E2E):
//   [1] enqueue-or-allow.lua — 중복 체크 + score 생성 + 대기열/참가열 삽입 (원자적)
//   [2] addActiveQueue + Redis Pub/Sub 알림
//
// 한 번의 k6 실행은 한 phase(워밍업/본/쿨다운)만 담당.
// phase 간 FLUSHALL은 queue-benchmark-test.sh에서 처리.
//
// [ 실행 ]
//   K6_RATE=300 K6_DURATION=60 k6 run queue-load-test.js              (본 측정)
//   K6_RATE=90  K6_DURATION=60 k6 run queue-load-test.js              (워밍업: 본 RPS의 30%)
//
// [ 환경 변수 ]
//   K6_RATE          : 초당 요청 수 (기본값 300)
//   K6_TEST_DURATION : 테스트 지속 시간 초 (기본값 60) — K6_DURATION 은 k6 예약어라 사용 불가
//   K6_RESULT_FILE : 결과 파일 경로 (기본값 'k6_result.txt')
//   K6_TARGET_URL  : 등록 엔드포인트 URL (기본값 nginx 경유 'http://localhost:8079/queue/register')

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

var successCount = new Counter('success_count');     // 대기열 + 참가열 신규 진입 합계
var waitCount    = new Counter('wait_count');        // 대기열 신규 진입 (REGISTERED_WAIT)
var allowCount   = new Counter('allow_count');       // 참가열 직접 삽입 (REGISTERED_ALLOW)
var duplicateCount = new Counter('duplicate_count');
var failCount    = new Counter('fail_count');

var RATE = parseInt(__ENV.K6_RATE || '300', 10);
// K6_DURATION 은 k6 예약 env(네이티브 duration 옵션)라 시나리오를 덮어써 충돌 → K6_TEST_DURATION 사용
var DURATION = parseInt(__ENV.K6_TEST_DURATION || '60', 10);
var TOTAL_EXPECTED = RATE * DURATION;
var RESULT_FILE = __ENV.K6_RESULT_FILE || 'k6_result.txt';
var TARGET_URL = __ENV.K6_TARGET_URL || 'http://localhost:8079/queue/register';
// userId 네임스페이스 프리픽스. 정합성 테스트에서 phase별로 겹치지 않게 분리하는 용도.
var UID_PREFIX = __ENV.K6_UID_PREFIX || 'user';

export var options = {
    scenarios: {
        load_test: {
            executor: 'constant-arrival-rate',
            rate: RATE,
            timeUnit: '1s',
            duration: DURATION + 's',
            // 무릎 탐색 상한(5,000 RPS)에서도 preAllocated ≥ RATE/2, maxVUs ≥ preAllocated×2 충족
            preAllocatedVUs: Math.min(Math.ceil(RATE * 2), 3000),
            maxVUs: Math.min(Math.ceil(RATE * 5), 10000),
            gracefulStop: '30s',
        },
    },
    summaryTrendStats: ['min', 'avg', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'count'],
};

export default function () {
    var uid = UID_PREFIX + '-' + __VU + '-' + __ITER;
    var payload = JSON.stringify({
        queueType: 'concert',
        userId: uid,
    });
    var params = {
        headers: {
            'Content-Type': 'application/json',
        },
        timeout: '30s',
    };

    var res = http.post(TARGET_URL, payload, params);
    var body = (res.body || '').replace(/"/g, '').trim();

    if (res.status === 200 && (body === 'REGISTERED_WAIT' || body === 'REGISTERED_ALLOW')) {
        successCount.add(1);
        // 응답 enum으로 대기열/참가열 구분
        if (body === 'REGISTERED_WAIT') {
            waitCount.add(1);
        } else {
            allowCount.add(1);
        }
    } else if (res.status === 200 && body === 'ALREADY_EXISTS') {
        duplicateCount.add(1);
    } else {
        failCount.add(1);
    }

    check(res, {
        '등록 성공 (200)': function (r) { return r.status === 200; },
    });
}

export function handleSummary(data) {
    var d = data.metrics.http_req_duration.values;
    var success = data.metrics.success_count ? data.metrics.success_count.values.count : 0;
    var wait = data.metrics.wait_count ? data.metrics.wait_count.values.count : 0;
    var allow = data.metrics.allow_count ? data.metrics.allow_count.values.count : 0;
    var fail = data.metrics.fail_count ? data.metrics.fail_count.values.count : 0;
    var dup = data.metrics.duplicate_count ? data.metrics.duplicate_count.values.count : 0;
    var total = success + fail + dup;

    var line = '='.repeat(55);
    var dash = '-'.repeat(55);
    var successRate = total > 0 ? ((success / total) * 100).toFixed(1) : '0.0';

    var output = '\n' + dash + '\n';
    output += '  HTTP 응답 측정 — ' + TOTAL_EXPECTED + '건 / ' + DURATION + '초 (' + RATE + ' RPS)\n';
    output += dash + '\n';
    output += '  성공: ' + success + '건 (' + successRate + '%) [대기열 ' + wait + ', 참가열 ' + allow + ']  ';
    output += '중복: ' + dup + '건  실패: ' + fail + '건\n';
    output += '  응답 시간  min=' + d.min.toFixed(2) + 'ms  avg=' + d.avg.toFixed(2) + 'ms  max=' + d.max.toFixed(2) + 'ms\n';
    output += '  p(95)=' + d['p(95)'].toFixed(2) + 'ms  p(99)=' + d['p(99)'].toFixed(2) + 'ms\n';
    output += dash + '\n';

    // benchmark_test.sh가 파싱할 결과 파일
    var result = [
        'success=' + success,
        'wait=' + wait,
        'allow=' + allow,
        'fail=' + fail,
        'duplicate=' + dup,
        'http_min=' + d.min.toFixed(2),
        'http_avg=' + d.avg.toFixed(2),
        'http_med=' + d.med.toFixed(2),
        'http_max=' + d.max.toFixed(2),
        'http_p95=' + d['p(95)'].toFixed(2),
        'http_p99=' + d['p(99)'].toFixed(2),
    ].join('\n') + '\n';

    var ret = { stdout: output };
    ret[RESULT_FILE] = result;
    return ret;
}
