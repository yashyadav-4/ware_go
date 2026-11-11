import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const successfulOrders = new Counter('successful_deadlock_free_orders');
const deadlockErrors = new Counter('deadlock_errors');
const otherErrors = new Counter('other_errors');

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    cross_warehouse_deadlock_stress: {
      executor: 'shared-iterations',
      vus: 200,
      iterations: 1000,
      maxDuration: '30s',
    },
  },
  thresholds: {
    'deadlock_errors': ['count==0'],
    'successful_deadlock_free_orders': ['count>0'],
  },
};

export function setup() {
  const rand = Math.floor(Math.random() * 90000) + 10000;
  const skuA = `SKU-DL-A-${rand}`;
  const skuB = `SKU-DL-B-${rand}`;

  const wh1Res = http.post(
    `${BASE_URL}/api/v1/warehouses`,
    JSON.stringify({
      code: `WH-1-${rand}`,
      name: `SF Hub ${rand}`,
      latitude: 37.7749,
      longitude: -122.4194,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  const wh1 = JSON.parse(wh1Res.body);

  const wh2Res = http.post(
    `${BASE_URL}/api/v1/warehouses`,
    JSON.stringify({
      code: `WH-2-${rand}`,
      name: `Oakland Hub ${rand}`,
      latitude: 37.8044,
      longitude: -122.2712,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  const wh2 = JSON.parse(wh2Res.body);

  http.post(`${BASE_URL}/api/v1/products`, JSON.stringify({ sku: skuA, name: `Product A ${rand}`, unit_weight_grams: 500 }), { headers: { 'Content-Type': 'application/json' } });
  http.post(`${BASE_URL}/api/v1/products`, JSON.stringify({ sku: skuB, name: `Product B ${rand}`, unit_weight_grams: 500 }), { headers: { 'Content-Type': 'application/json' } });

  http.post(`${BASE_URL}/api/v1/inventory/restock`, JSON.stringify({ warehouse_id: wh1.id, sku: skuA, quantity: 1000 }), { headers: { 'Content-Type': 'application/json' } });
  http.post(`${BASE_URL}/api/v1/inventory/restock`, JSON.stringify({ warehouse_id: wh1.id, sku: skuB, quantity: 1000 }), { headers: { 'Content-Type': 'application/json' } });
  http.post(`${BASE_URL}/api/v1/inventory/restock`, JSON.stringify({ warehouse_id: wh2.id, sku: skuA, quantity: 1000 }), { headers: { 'Content-Type': 'application/json' } });
  http.post(`${BASE_URL}/api/v1/inventory/restock`, JSON.stringify({ warehouse_id: wh2.id, sku: skuB, quantity: 1000 }), { headers: { 'Content-Type': 'application/json' } });

  return { skuA, skuB };
}

export default function (data) {
  const isEvenVU = (__VU % 2 === 0);

  const items = isEvenVU
    ? [
        { sku: data.skuA, quantity: 1 },
        { sku: data.skuB, quantity: 1 },
      ]
    : [
        { sku: data.skuB, quantity: 1 },
        { sku: data.skuA, quantity: 1 },
      ];

  const payload = JSON.stringify({
    dest_latitude: 37.7749,
    dest_longitude: -122.4194,
    items: items,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': `dl-${__VU}-${__ITER}-${Date.now()}-${Math.random()}`,
    },
    tags: { name: 'DeadlockOrder' },
  };

  const res = http.post(`${BASE_URL}/api/v1/orders/reserve`, payload, params);

  if (res.status === 201) {
    successfulOrders.add(1);
  } else if (res.body && (res.body.includes('deadlock detected') || res.body.includes('40P01'))) {
    deadlockErrors.add(1);
    console.error(`DEADLOCK DETECTED! ${res.body}`);
  } else if (res.status === 429) {
  } else {
    otherErrors.add(1);
  }

  check(res, {
    'no deadlocks': () => !res.body || (!res.body.includes('deadlock') && !res.body.includes('40P01')),
  });
}

export function teardown() {
}
