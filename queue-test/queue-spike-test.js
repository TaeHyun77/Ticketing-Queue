// 대기열 스파이크 테스트 — 0→피크 급상승 후 유지, 3구간(baseline/spike/recovery) 분리 측정
//
// 프로파일: baseline(10s) → 1s 급상승 → 피크 유지(HOLD) → 5s 하강 → 복구 관찰(15s)
// 목적: 단일 노드에 한계 초과 폭주를 가했을 때
//   - Spike 구간: 지연이 얼마나 폭증하는가 / 에러·dropped 발생하는가
//   - Recovery 구간: 폭풍이 지난 뒤 지연이 baseline 수준으로 '즉시 회복'하는가
//
// [ 실행 ]
//   K6_SPIKE_RATE=3000 K6_HOLD=30 k6 run queue-spike-test.js
//
// [ 환경 변수 ]
//   K6_SPIKE_RATE : 피크 RPS (기본 3000)
//   K6_BASE_RATE  : baseline/복구 RPS (기본 50)
//   K6_HOLD       : 피크 유지 초 (기본 30)
//   K6_TARGET_URL : 등록 엔드포인트 (기본 nginx 'http://localhost:8079/queue/register')
//   K6_RESULT_FILE: 결과 파일 (기본 'spike_result.txt')

import http from 'k6/http';
import { Trend, Counter } from 'k6/metrics';
import exec from 'k6/execution';

var SPIKE_RATE = parseInt(__ENV.K6_SPIKE_RATE || '3000', 10);
var BASE_RATE = parseInt(__ENV.K6_BASE_RATE || '50', 10);
var HOLD = parseInt(__ENV.K6_HOLD || '30', 10);
var TARGET_URL = __ENV.K6_TARGET_URL || 'http://localhost:8079/queue/register';
var RESULT_FILE = __ENV.K6_RESULT_FILE || 'spike_result.txt';

// 구간 경계(ms). stages: baseline 10s / ramp 1s / hold HOLD / drop 5s / recovery 15s
var BASELINE_START = 3000,  BASELINE_END = 10000;             // 콜드스타트 2~3s 제외
var SPIKE_START = 11000,    SPIKE_END = 11000 + HOLD * 1000;  // 급상승 직후 ~ 유지 종료
var RECOVERY_START = SPIKE_END + 7000;                        // 하강(5s)+안정(2s) 후
var RECOVERY_END = SPIKE_END + 20000;

function bucket(t) {
    if (t >= BASELINE_START && t <= BASELINE_END) return 'base';
    if (t >= SPIKE_START && t <= SPIKE_END) return 'spike';
    if (t >= RECOVERY_START && t <= RECOVERY_END) return 'recovery';
    return null; // ramp/drop 전이 구간은 집계 제외
}

var dur = {
    base: new Trend('base_duration', true),
    spike: new Trend('spike_duration', true),
    recovery: new Trend('recovery_duration', true),
};
var ok = {
    base: new Counter('base_success'),
    spike: new Counter('spike_success'),
    recovery: new Counter('recovery_success'),
};
var ng = {
    base: new Counter('base_fail'),
    spike: new Counter('spike_fail'),
    recovery: new Counter('recovery_fail'),
};

export var options = {
    scenarios: {
        spike: {
            executor: 'ramping-arrival-rate',
            startRate: BASE_RATE,
            timeUnit: '1s',
            preAllocatedVUs: Math.min(SPIKE_RATE * 2, 3000),
            maxVUs: Math.min(SPIKE_RATE * 8, 10000),
            stages: [
                { target: BASE_RATE, duration: '10s' },       // baseline
                { target: SPIKE_RATE, duration: '1s' },       // 급상승 (거의 즉시)
                { target: SPIKE_RATE, duration: HOLD + 's' }, // 피크 유지
                { target: BASE_RATE, duration: '5s' },        // 급하강
                { target: BASE_RATE, duration: '15s' },       // 복구 관찰
            ],
            gracefulStop: '30s',
        },
    },
    summaryTrendStats: ['min', 'avg', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

export default function () {
    // 발사 시점 기록 — 응답 수신 시점으로 분류하면 스파이크 막판의 느린 응답이
    // 전이 구간으로 빠져 spike p99가 좋게 나오는 생존자 편향이 생긴다
    var t0 = exec.instance.currentTestRunDuration;
    var uid = 'spike-' + __VU + '-' + __ITER;
    var payload = JSON.stringify({ queueType: 'concert', userId: uid });
    var params = { headers: { 'Content-Type': 'application/json' }, timeout: '30s' };

    var res = http.post(TARGET_URL, payload, params);
    var body = (res.body || '').replace(/"/g, '').trim();
    var success = res.status === 200 &&
        (body === 'REGISTERED_WAIT' || body === 'REGISTERED_ALLOW' || body === 'ALREADY_EXISTS');

    var b = bucket(t0);
    if (b) {
        dur[b].add(res.timings.duration);
        if (success) { ok[b].add(1); } else { ng[b].add(1); }
    }
}

export function handleSummary(data) {
    var m = data.metrics;
    var dropped = m.dropped_iterations ? m.dropped_iterations.values.count : 0;

    function seg(name) {
        var d = m[name + '_duration'] ? m[name + '_duration'].values : null;
        var s = m[name + '_success'] ? m[name + '_success'].values.count : 0;
        var f = m[name + '_fail'] ? m[name + '_fail'].values.count : 0;
        var total = s + f;
        var err = total > 0 ? ((f / total) * 100).toFixed(2) : '0.00';
        return { d: d, s: s, f: f, total: total, err: err };
    }
    var base = seg('base'), spike = seg('spike'), rec = seg('recovery');

    function line(label, x) {
        if (!x.d) return '  ' + label + ': 표본 없음\n';
        return '  ' + label +
            ' | 요청 ' + x.total + '  에러율 ' + x.err + '%' +
            '  p95=' + x.d['p(95)'].toFixed(1) + 'ms' +
            '  p99=' + x.d['p(99)'].toFixed(1) + 'ms' +
            '  max=' + x.d.max.toFixed(1) + 'ms\n';
    }

    var dash = '-'.repeat(72);
    var out = '\n' + dash + '\n';
    out += '  스파이크 3구간 측정 — ' + BASE_RATE + ' → ' + SPIKE_RATE + ' → ' + BASE_RATE + ' RPS (피크 ' + HOLD + 's)\n';
    out += dash + '\n';
    out += line('Baseline', base);
    out += line('Spike   ', spike);
    out += line('Recovery', rec);
    out += dash + '\n';
    out += '  dropped_iterations=' + dropped + ' (목표 유입 대비 발사 밀린 요청 — 과부하 신호)\n';
    // 회복 판정: recovery p99 가 baseline p99 의 2배 이내면 '즉시 회복'으로 간주
    if (base.d && rec.d) {
        var recovered = rec.d['p(99)'] <= base.d['p(99)'] * 2;
        out += '  회복 판정: recovery p99(' + rec.d['p(99)'].toFixed(1) + 'ms) vs baseline p99(' +
            base.d['p(99)'].toFixed(1) + 'ms) → ' + (recovered ? '✅ 즉시 회복' : '⚠️ 미회복') + '\n';
    }
    out += dash + '\n';

    var result = [
        'spike_rate=' + SPIKE_RATE,
        'base_p99=' + (base.d ? base.d['p(99)'].toFixed(1) : 'NA'),
        'base_err=' + base.err,
        'spike_total=' + spike.total,
        'spike_err=' + spike.err,
        'spike_p95=' + (spike.d ? spike.d['p(95)'].toFixed(1) : 'NA'),
        'spike_p99=' + (spike.d ? spike.d['p(99)'].toFixed(1) : 'NA'),
        'spike_max=' + (spike.d ? spike.d.max.toFixed(1) : 'NA'),
        'recovery_p99=' + (rec.d ? rec.d['p(99)'].toFixed(1) : 'NA'),
        'recovery_err=' + rec.err,
        'dropped_iterations=' + dropped,
    ].join('\n') + '\n';

    var ret = { stdout: out };
    ret[RESULT_FILE] = result;
    return ret;
}
