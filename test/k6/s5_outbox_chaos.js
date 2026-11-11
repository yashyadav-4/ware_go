import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const outboxOrdersReserved = new Counter('outbox_orders_reserved');
const outboxOrdersConfirmed = new Counter('outbox_orders_confirmed');

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    order_lifecycle_pipeline: {
      executor: 'shared-iterations',
      vus: 10,
      iterations: 50,
      maxDuration: '20s',
    },
  },
  thresholds: {
    'outbox_orders_reserved': ['count==50'],
    'outbox_orders_confirmed': ['count==50'],
  },
};

export function setup() {
  const rand = Math.floor(Math.random() * 90000) + 10000;
  const sku = `SKU-OUTBOX-${rand}`;

  const whRes = http.post(
    `${BASE_URL}/api/v1/warehouses`,
    JSON.stringify({
      code: `WH-OB-${rand}`,
      name: `Outbox Hub ${rand}`,
      latitude: 37.7749,
      longitude: -122.4194,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  const wh = JSON.parse(whRes.body);

  http.post(
    `${BASE_URL}/api/v1/products`,
    JSON.stringify({ sku: sku, name: `Outbox Product ${rand}`, unit_weight_grams: 100 }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  http.post(
    `${BASE_URL}/api/v1/inventory/restock`,
    JSON.stringify({ warehouse_id: wh.id, sku: sku, quantity: 1000 }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  return { sku };
}

export default function (data) {
  const reservePayload = JSON.stringify({
    dest_latitude: 37.7749,
    dest_longitude: -122.4194,
    items: [{ sku: data.sku, quantity: 1 }],
  });

  const reserveRes = http.post(
    `${BASE_URL}/api/v1/orders/reserve`,
    reservePayload,
    {
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `ob-${__VU}-${__ITER}-${Date.now()}-${Math.random()}`,
      },
    }
  );

  if (reserveRes.status === 201) {
    outboxOrdersReserved.add(1);
    const orderData = JSON.parse(reserveRes.body);
    const orderId = orderData.order.id;

    const confirmRes = http.post(`${BASE_URL}/api/v1/orders/${orderId}/confirm`);

    if (confirmRes.status === 200) {
      outboxOrdersConfirmed.add(1);
    }

    check(confirmRes, {
      'order confirmed (200)': (r) => r.status === 200,
    });
  }
}

export function teardown() {
  sleep(2);
}
