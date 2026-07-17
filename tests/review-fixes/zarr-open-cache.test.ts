import { describe, expect, test } from 'vitest';
import { ZarrBackend } from '../../src/backends/zarr';
import { MemoryZarrStore } from '../helpers/MemoryZarrStore';

function normalizeKey(key: string): string {
  return key.startsWith('/') ? key.slice(1) : key;
}

class CountingMemoryZarrStore extends MemoryZarrStore {
  private getCounts = new Map<string, number>();
  private hasCounts = new Map<string, number>();

  async get(key: string): Promise<Uint8Array | undefined> {
    this.increment(this.getCounts, key);
    return super.get(key);
  }

  async has(key: string): Promise<boolean> {
    this.increment(this.hasCounts, key);
    return super.has(key);
  }

  resetReadCounts(): void {
    this.getCounts.clear();
    this.hasCounts.clear();
  }

  getFetchCount(key: string): number {
    return this.getCounts.get(normalizeKey(key)) ?? 0;
  }

  get totalFetches(): number {
    return Array.from(this.getCounts.values()).reduce((total, count) => total + count, 0);
  }

  get fetchCounts(): Record<string, number> {
    return Object.fromEntries(this.getCounts);
  }

  private increment(counts: Map<string, number>, key: string): void {
    const normalizedKey = normalizeKey(key);
    counts.set(normalizedKey, (counts.get(normalizedKey) ?? 0) + 1);
  }
}

function createStore(): CountingMemoryZarrStore {
  const store = new CountingMemoryZarrStore({
    'zarr.json': { node_type: 'group', attributes: {} },
    'x/zarr.json': {
      node_type: 'array',
      shape: [4],
      data_type: 'float64',
      dimension_names: ['x']
    },
    'temp/zarr.json': {
      node_type: 'array',
      shape: [4],
      data_type: 'float64',
      dimension_names: ['x']
    }
  });

  const x = new Float64Array([10, 20, 30, 40]);
  const temp = new Float64Array([11, 22, 33, 44]);
  store.set('x/c/0', new Uint8Array(x.buffer.slice(0)));
  store.set('temp/c/0', new Uint8Array(temp.buffer.slice(0)));

  return store;
}

async function readTwoSelections(store: CountingMemoryZarrStore) {
  const dataset = await ZarrBackend.open(store);
  const temp = dataset.getVariable('temp');

  store.resetReadCounts();

  const first = await (await temp.sel({ x: 10 })).compute();
  const second = await (await temp.sel({ x: 30 })).compute();

  return { first, second };
}

describe('ZarrBackend lazy array metadata cache', () => {
  test('two selections from the same array return the expected values', async () => {
    const { first, second } = await readTwoSelections(createStore());

    expect(first.data).toBe(11);
    expect(second.data).toBe(33);
  });

  test('fetches array metadata at most once across two reads after open', async () => {
    const store = createStore();
    const { first, second } = await readTwoSelections(store);

    expect(first.data).toBe(11);
    expect(second.data).toBe(33);

    const metadataKey = 'temp/zarr.json';
    const metadataFetches = store.getFetchCount(metadataKey);
    const message = [
      `Expected ${metadataKey} to be fetched at most once after ZarrBackend.open,`,
      `but observed ${metadataFetches} metadata fetches across two reads`,
      `(${store.totalFetches} total store fetches).`,
      `Per-key fetch counts: ${JSON.stringify(store.fetchCounts)}`
    ].join(' ');

    expect(metadataFetches, message).toBeLessThanOrEqual(1);
  });
});
