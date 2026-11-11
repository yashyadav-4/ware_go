import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const hotSkuLatency = new Trend('hot_sku_high_contention_latency');
const coldSkuLatency = new Trend('cold_sku_low_contention_latency');
const totalBenchmarkOrders = new Counter('total_benchmark_orders');

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    high_contention_hot_item: {
      executor: 'constant-vus',
      vus: 50,
      duration: '10s',
      tags: { test_type: 'high_contention' },
    },
    low_contention_catalog: {
      executor: 'constant-vus',
      vus: 50,
      duration: '10s',
      startTime: '11s',
      tags: { test_type: 'low_contention' },
    },
  },
  thresholds: {
    'total_benchmark_orders': ['count>0'],
  },
};

export function setup() {
  const rand = Math.floor(Math.random() * 90000) + 10000;
  const hotSku = `HOT-SKU-${rand}`;
  const coldSkus = [];

  const whRes = http.post(
    `${BASE_URL}/api/v1/warehouses`,
    JSON.stringify({
      code: `WH-BM-${rand}`,
      name: `Benchmark Hub ${rand}`,
      latitude: 37.7749,
      longitude: -122.4194,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  const wh = JSON.parse(whRes.body);

  http.post(`${BASE_URL}/api/v1/products`, JSON.stringify({ sku: hotSku, name: `Hot Item`, unit_weight_grams: 100 }), { headers: { 'Content-Type': 'application/json' } });
  http.post(`${BASE_URL}/api/v1/inventory/restock`, JSON.stringify({ warehouse_id: wh.id, sku: hotSku, quantity: 50000 }), { headers: { 'Content-Type': 'application/json' } });

  for (let i = 0; i < 20; i++) {
    const sku = `COLD-SKU-${rand}-${i}`;
    coldSkus.push(sku);
    http.post(`${BASE_URL}/api/v1/products`, JSON.stringify({ sku: sku, name: `Cold Item ${i}`, unit_weight_grams: 100 }), { headers: { 'Content-Type': 'application/json' } });
    http.post(`${BASE_URL}/api/v1/inventory/restock`, JSON.stringify({ warehouse_id: wh.id, sku: sku, quantity: 1000 }), { headers: { 'Content-Type': 'application/json' } });
  }

  return { hotSku, coldSkus };
}

export default function (data) {
  const isHighContention = (__ENV.SCENARIO_NAME === 'high_contention_hot_item' || !__ENV.SCENARIO_NAME);

  let targetSku = data.hotSku;
  if (!isHighContention && data.coldSkus && data.coldSkus.length > 0) {
    const idx = Math.floor(Math.random() * data.coldSkus.length);
    targetSku = data.coldSkus[idx];
  }

  const payload = JSON.stringify({
    dest_latitude: 37.7749,
    dest_longitude: -122.4194,
    items: [{ sku: targetSku, quantity: 1 }],
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': `bm-${__VU}-${__ITER}-${Date.now()}-${Math.random()}`,
    },
    tags: { name: 'BenchmarkReserve' },
  };

  const res = http.post(`${BASE_URL}/api/v1/orders/reserve`, payload, params);

  if (res.status === 201) {
    totalBenchmarkOrders.add(1);
    if (isHighContention) {
      hotSkuLatency.add(res.timings.duration);
    } else {
      coldSkuLatency.add(res.timings.duration);
    }
  }

  check(res, {
    'valid response (201 or 429)': (r) => r.status === 201 || r.status === 429,
  });
}

export function teardown() {
}
