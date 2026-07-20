import { describe, test, expect } from 'vitest';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const repoRoot = fileURLToPath(new URL('../..', import.meta.url));
const startedAt = performance.now();
// --smoke is the contract for a small, CI-runnable workload the fixed script must support.
const smokeRun = spawnSync('node', ['benchmark-coordinate-lookup.js', '--smoke'], {
  cwd: repoRoot,
  encoding: 'utf8',
  timeout: 60_000
});
const durationMs = performance.now() - startedAt;

describe('root coordinate benchmark smoke mode', () => {
  test('exits successfully when run as an ES module', () => {
    expect(smokeRun.status).toBe(0);
  });

  test('does not fail with a CommonJS ReferenceError', () => {
    expect(smokeRun.stderr).not.toContain('ReferenceError');
  });
});

describe('perf', () => {
  test('--smoke completes within the CI runtime budget', () => {
    console.info(`[root-bench-esm perf] --smoke completed in ${durationMs.toFixed(2)} ms`);

    expect((smokeRun.error as NodeJS.ErrnoException | undefined)?.code).not.toBe('ETIMEDOUT');
    // Calibrated on Apple M1 Max; the 30s threshold leaves generous margins.
    expect(durationMs).toBeLessThan(30_000);
  });
});
