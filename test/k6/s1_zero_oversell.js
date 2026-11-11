import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const successfulReservations = new Counter('successful_reservations');
const outOfStockResponses = new Counter('out_of_stock_responses');
const semaphoreRejections = new Counter('semaphore_rejections');
const unexpectedErrors = new Counter('unexpected_errors');

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    flash_sale_spike: {
      executor: 'shared-iterations',
      vus: 500,
      iterations: 2000,
      maxDuration: '30s',
    },
  },
  thresholds: {
    'successful_reservations': ['count==100'],
  },
};

export function setup() {
  const rand = Math.floor(Math.random() * 90000) + 10000;
  const sku = `SKU-FLASH-${rand}`;
  const whCode = `WH-${rand}`;

  const whRes = http.post(
    `${BASE_URL}/api/v1/warehouses`,
    JSON.stringify({
      code: whCode,
      name: `Warehouse ${rand}`,
      latitude: 37.7749,
      longitude: -122.4194,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  if (whRes.status !== 201 && whRes.status !== 200) {
    throw new Error(`Failed to create warehouse: ${whRes.body}`);
  }
  const warehouse = JSON.parse(whRes.body);

  const prodRes = http.post(
    `${BASE_URL}/api/v1/products`,
    JSON.stringify({
      sku: sku,
      name: `Limited Edition Flash Item ${rand}`,
      unit_weight_grams: 500,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  if (prodRes.status !== 201 && prodRes.status !== 200) {
    throw new Error(`Failed to create product: ${prodRes.body}`);
  }

  const restockRes = http.post(
    `${BASE_URL}/api/v1/inventory/restock`,
    JSON.stringify({
      warehouse_id: warehouse.id,
      sku: sku,
      quantity: 100,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  if (restockRes.status !== 200 && restockRes.status !== 201) {
    throw new Error(`Failed to restock inventory: ${restockRes.body}`);
  }

  return { warehouseId: warehouse.id, sku: sku };
}

export default function (data) {
  const lat = 37.7749 + (Math.random() - 0.5) * 0.1;
  const lon = -122.4194 + (Math.random() - 0.5) * 0.1;

  const payload = JSON.stringify({
    dest_latitude: lat,
    dest_longitude: lon,
    items: [
      {
        sku: data.sku,
        quantity: 1,
      },
    ],
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': `k6-${__VU}-${__ITER}-${Date.now()}-${Math.random()}`,
    },
    tags: { name: 'ReserveOrder' },
  };

  const res = http.post(`${BASE_URL}/api/v1/orders/reserve`, payload, params);

  if (res.status === 201) {
    successfulReservations.add(1);
  } else if (res.status === 409) {
    outOfStockResponses.add(1);
  } else if (res.status === 429 || res.status === 503) {
    semaphoreRejections.add(1);
  } else {
    unexpectedErrors.add(1);
    console.error(`Unexpected HTTP ${res.status}: ${res.body}`);
  }

  check(res, {
    'valid status (201, 409, 429)': (r) => r.status === 201 || r.status === 409 || r.status === 429,
  });
}

export function teardown(data) {
  sleep(1);

  const stockRes = http.get(`${BASE_URL}/api/v1/inventory/${data.sku}`);

  if (stockRes.status === 200) {
    const stockData = JSON.parse(stockRes.body);
    console.log(`Final Available Stock in DB: ${stockData.total_available}`);
    if (stockData.total_available === 0) {
      console.log(`CLAIM 1 PROVEN: ZERO OVERSELL! All 100 units reserved, 0 remaining.`);
    } else {
      console.error(`INVENTORY MISMATCH! Expected 0, found ${stockData.total_available}`);
    }
  }
}
