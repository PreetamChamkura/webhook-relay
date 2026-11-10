// k6 load test: sustained publish at a fixed arrival rate.
//   k6 run -e RATE=1000 -e DURATION=2m loadtest/publish.js
// Publish latency comes from k6 (http_req_duration). End-to-end delivery latency
// comes from Prometheus: webhook_end_to_end_seconds / receiver_end_to_end_seconds.
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = parseInt(__ENV.RATE || '500');

export const options = {
  scenarios: {
    publish: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s',
      duration: __ENV.DURATION || '2m',
      preAllocatedVUs: 200, maxVUs: 2000,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<250'],
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  const res = http.post(`${BASE}/v1/subscriptions`, JSON.stringify({
    url: __ENV.RECEIVER_URL || 'http://receiver:8090/hook',
    event_types: ['load.test'],
    rate_limit_per_sec: 100000,
  }), { headers: { 'Content-Type': 'application/json' } });
  check(res, { 'subscription created': (r) => r.status === 201 });
  return { sub: res.json('id') };
}

export default function () {
  const res = http.post(`${BASE}/v1/events`, JSON.stringify({
    event_type: 'load.test',
    payload: { n: __ITER, vu: __VU, at: Date.now() },
  }), { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `${__VU}-${__ITER}` } });
  check(res, { 'accepted': (r) => r.status === 202 });
}
