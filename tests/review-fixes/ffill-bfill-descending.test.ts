import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('BUG 7: ffill/bfill on descending coordinates diverge between code paths', () => {
  // pandas (which xarray.sel delegates to) on a DESCENDING index:
  //   pd.Index([30,20,10]).get_indexer([25], method='pad')      -> [0]  (coord 30)
  //   pd.Index([30,20,10]).get_indexer([25], method='backfill') -> [1]  (coord 20)
  //   pd.Index([30,20,11]).get_indexer([25], method='pad')      -> [0]  (coord 30)
  //   pd.Index([30,20,11]).get_indexer([25], method='backfill') -> [1]  (coord 20)
  // The evenly-spaced arithmetic fast path matches pandas; the linear fallback (and the
  // binary-search descending branches) use the opposite, value-based semantics.

  test('ffill on descending coords follows pandas positional semantics', async () => {
    // Evenly spaced (arithmetic path): matches pandas, coord 30 -> value 1 -- passes
    const even = new DataArray([1, 2, 3], { dims: ['x'], coords: { x: [30, 20, 10] } });
    expect((await even.sel({ x: 25 }, { method: 'ffill' })).data).toBe(1);

    // Not evenly spaced (fallback path): should also give coord 30 -> value 1, gives 20 -> 2
    const uneven = new DataArray([1, 2, 3], { dims: ['x'], coords: { x: [30, 20, 11] } });
    expect((await uneven.sel({ x: 25 }, { method: 'ffill' })).data).toBe(1);
  });

  test('bfill on descending coords follows pandas positional semantics', async () => {
    const even = new DataArray([1, 2, 3], { dims: ['x'], coords: { x: [30, 20, 10] } });
    expect((await even.sel({ x: 25 }, { method: 'bfill' })).data).toBe(2);

    // Fallback path: should give coord 20 -> value 2, gives 30 -> 1
    const uneven = new DataArray([1, 2, 3], { dims: ['x'], coords: { x: [30, 20, 11] } });
    expect((await uneven.sel({ x: 25 }, { method: 'bfill' })).data).toBe(2);
  });
});
