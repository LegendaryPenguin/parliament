/**
 * The API process entry point.
 *
 * Separate from `server.ts` so that importing the server does not start one — the
 * tests and the CLI both import it, and a module with a side effect at import time
 * would make that impossible.
 */

import { createApiServer } from './server.js';
import { defaultWiring, seedDemo } from './wiring.js';

const port = Number(process.env['PORT'] ?? 8787);
const demo = process.argv.includes('--demo');

// A ceiling low enough that the demo tenant is visibly near it, because a budget that
// is never approached is a budget nobody has tested.
const budgets = new Map<string, number>([
  ['acme', 500],
  ['globex', 50],
]);

const wiring = defaultWiring({ budgets });

if (demo) {
  await seedDemo(wiring);
}

const server = createApiServer({ deps: wiring.deps, cost: wiring.cost, serveDashboard: true });

server.listen(port, () => {
  process.stdout.write(`parliament api on http://localhost:${port}\n`);
  process.stdout.write(`dashboard      http://localhost:${port}/ui/\n`);
  if (demo) process.stdout.write('seeded two demo executions\n');
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    server.close(() => process.exit(0));
  });
}
