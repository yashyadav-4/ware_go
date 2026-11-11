import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';

const reservationLatency = new Trend('reservation_p95_latency');
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    sustained_throughput: {
      executor: 'constant-arrival-rate',
      rate: 500,
      timeUnit: '1s',
      duration: '20s',
      preAllocatedVUs: 100,
      maxVUs: 300,
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<1000'],
  },
};

export function setup() {
  const rand = Math.floor(Math.random() * 90000) + 10000;
  const sku = `SKU-LAT-${rand}`;

  const whRes = http.post(
    `${BASE_URL}/api/v1/warehouses`,
    JSON.stringify({
      code: `WH-LAT-${rand}`,
      name: `Latency Hub ${rand}`,
      latitude: 37.7749,
      longitude: -122.4194,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  const wh = JSON.parse(whRes.body);

  http.post(
    `${BASE_URL}/api/v1/products`,
    JSON.stringify({ sku: sku, name: `Latency Product ${rand}`, unit_weight_grams: 250 }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  http.post(
    `${BASE_URL}/api/v1/inventory/restock`,
    JSON.stringify({ warehouse_id: wh.id, sku: sku, quantity: 20000 }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  return { sku };
}

export default function (data) {
  const payload = JSON.stringify({
    dest_latitude: 37.7749,
    dest_longitude: -122.4194,
    items: [{ sku: data.sku, quantity: 1 }],
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': `lat-${__VU}-${__ITER}-${Date.now()}-${Math.random()}`,
    },
    tags: { name: 'LatencyBenchmark' },
  };

  const res = http.post(`${BASE_URL}/api/v1/orders/reserve`, payload, params);

  if (res.status === 201) {
    reservationLatency.add(res.timings.duration);
  }

  check(res, {
    'status 201 or 429': (r) => r.status === 201 || r.status === 429,
  });
}

export function teardown() {
}
