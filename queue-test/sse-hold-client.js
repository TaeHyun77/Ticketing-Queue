// SSE 연결 유지(Hold) 테스트 클라이언트 — Node.js 전용 (k6는 SSE 미지원이라 대체)
//
// N개의 SSE 연결(/queue/stream)을 서서히 맺고 HOLD초 동안 유지하며,
//   - 연결 성립/실패/끊김 수
//   - 각 연결이 받은 이벤트 수 (init/update/confirmed/cancelled → fan-out 도달 검증)
// 을 집계한다. 앱 메모리는 외부에서 docker stats 로 별도 샘플링.
//
// 실행:
//   SSE_N=3000 SSE_HOLD=180 node queue-test/sse-hold-client.js
//
// 환경변수:
//   SSE_N      : 연결 수 (기본 3000)
//   SSE_HOLD   : 유지 시간 초 (기본 180)
//   SSE_BASE   : 앱 베이스 URL (기본 http://localhost:8081 — 앱 직접, nginx 우회)
//   SSE_QUEUE  : queueType (기본 concert)
//   SSE_RAMP_MS: 연결 오픈 간 배치 지연 ms (기본 20, 배치당 50개)

const http = require('http');

const N = parseInt(process.env.SSE_N || '3000', 10);
const HOLD = parseInt(process.env.SSE_HOLD || '180', 10);
const BASE = process.env.SSE_BASE || 'http://localhost:8081';
const QUEUE = process.env.SSE_QUEUE || 'concert';
const RAMP_MS = parseInt(process.env.SSE_RAMP_MS || '20', 10);
const BATCH = 50;

const base = new URL(BASE);
const agent = new http.Agent({ keepAlive: false, maxSockets: Infinity });

let established = 0;
let failed = 0;
let closedEarly = 0;
let totalEvents = 0;
const eventsByType = {};
const reqs = [];

function openOne(i) {
    const path = `/queue/stream?queueType=${encodeURIComponent(QUEUE)}&userId=sse-${i}`;
    const req = http.get({
        host: base.hostname, port: base.port, path, agent,
        headers: { Accept: 'text/event-stream' },
    }, (res) => {
        if (res.statusCode !== 200) { failed++; res.destroy(); return; }
        established++;
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
            const lines = chunk.split('\n');
            for (const ln of lines) {
                if (ln.startsWith('event:')) {
                    totalEvents++;
                    const t = ln.slice(6).trim();
                    eventsByType[t] = (eventsByType[t] || 0) + 1;
                }
            }
        });
        res.on('end', () => { closedEarly++; });
    });
    req.on('error', () => { failed++; });
    reqs.push(req);
}

async function ramp() {
    for (let i = 0; i < N; i += BATCH) {
        for (let j = i; j < Math.min(i + BATCH, N); j++) openOne(j);
        await new Promise(r => setTimeout(r, RAMP_MS));
    }
}

function snapshot(label) {
    const eventStr = Object.entries(eventsByType).map(([k, v]) => `${k}=${v}`).join(' ') || '(없음)';
    console.log(`[${label}] 성립=${established}/${N} 실패=${failed} 조기종료=${closedEarly} | 이벤트 총 ${totalEvents} { ${eventStr} }`);
}

(async () => {
    console.log(`SSE Hold 테스트: N=${N} HOLD=${HOLD}s BASE=${BASE} queue=${QUEUE}`);
    const t0 = Date.now();
    await ramp();
    console.log(`램프업 완료: ${((Date.now() - t0) / 1000).toFixed(1)}s 만에 오픈 시도 ${N}건`);

    // HOLD 동안 주기적으로 상태 출력 (fan-out 이벤트 도달 관찰)
    const interval = setInterval(() => snapshot('hold'), 15000);
    await new Promise(r => setTimeout(r, HOLD * 1000));
    clearInterval(interval);

    snapshot('final');
    for (const r of reqs) { try { r.destroy(); } catch (e) {} }
    // 판정
    const ok = established >= N * 0.99 && closedEarly <= N * 0.01;
    console.log(ok
        ? `✅ 유지 성공: ${established}건 안정적으로 ${HOLD}s 유지`
        : `⚠️ 확인 필요: 성립 ${established}, 조기종료 ${closedEarly}`);
    process.exit(0);
})();
