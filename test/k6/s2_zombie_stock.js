import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const reclaimSlaMs = new Trend('reclaim_sla_ms');
const zombieReservationsCreated = new Counter('zombie_reservations_created');

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    reserve_zombies: {
      executor: 'shared-iterations',
      vus: 10,
      iterations: 50,
      maxDuration: '10s',
    },
  },
  thresholds: {
    'zombie_reservations_created': ['count==50'],
    'reclaim_sla_ms': ['p(95)<5000'],
  },
};

export function setup() {
  const rand = Math.floor(Math.random() * 90000) + 10000;
  const sku = `SKU-ZOMBIE-${rand}`;
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
      name: `Zombie Stock Item ${rand}`,
      unit_weight_grams: 300,
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
      quantity: 50,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  if (restockRes.status !== 200 && restockRes.status !== 201) {
    throw new Error(`Failed to restock inventory: ${restockRes.body}`);
  }

  return { warehouseId: warehouse.id, sku: sku };
}

export default function (data) {
  const payload = JSON.stringify({
    dest_latitude: 37.7749,
    dest_longitude: -122.4194,
    ttl_seconds: 2,
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
      'Idempotency-Key': `zombie-${__VU}-${__ITER}-${Date.now()}-${Math.random()}`,
    },
    tags: { name: 'ReserveZombieOrder' },
  };

  const res = http.post(`${BASE_URL}/api/v1/orders/reserve`, payload, params);

  if (res.status === 201) {
    zombieReservationsCreated.add(1);
  }

  check(res, {
    'reservation succeeded (201)': (r) => r.status === 201,
  });
}

export function teardown(data) {
  const initialStock = http.get(`${BASE_URL}/api/v1/inventory/${data.sku}`);
  const initialData = JSON.parse(initialStock.body);

  sleep(2.5);

  const startTime = Date.now();
  let reclaimed = false;
  let finalAvailable = 0;
  const maxWaitMs = 8000;

  while (Date.now() - startTime < maxWaitMs) {
    const pollRes = http.get(`${BASE_URL}/api/v1/inventory/${data.sku}`);
    if (pollRes.status === 200) {
      const pollData = JSON.parse(pollRes.body);
      finalAvailable = pollData.total_available;
      if (finalAvailable === 50) {
        const elapsedMs = Date.now() - startTime;
        reclaimSlaMs.add(elapsedMs);
        reclaimed = true;
        console.log(`REAPER SLA PROVEN: All 50 units reclaimed in ${elapsedMs} ms!`);
        break;
      }
    }
    sleep(0.3);
  }

  if (!reclaimed) {
    console.error(`SLA BREACH: After 8 seconds, stock reached ${finalAvailable} / 50`);
  }
}
