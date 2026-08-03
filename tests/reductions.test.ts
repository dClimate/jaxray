import { describe, test, expect } from 'vitest';
import { DataArray } from '../src/DataArray';

/**
 * min/max/std/median alongside the pre-existing sum/mean.
 *
 * The contract they all share is xarray's skipna default: masked and
 * non-numeric leaves are ignored, and a slice with nothing numeric in it
 * reduces to NaN rather than to 0 or Infinity. Climate grids are full of
 * masked cells (ocean under a land variable, and vice versa), so this is the
 * behaviour that actually matters in practice.
 */

const grid = () =>
  new DataArray(
    [
      [1, 2, 3],
      [4, 5, 6]
    ],
    { dims: ['y', 'x'], coords: { y: [0, 1], x: [0, 1, 2] } }
  );

describe('whole-array reductions', () => {
  test('reduce every value to a scalar', () => {
    const da = grid();
    expect(da.min()).toBe(1);
    expect(da.max()).toBe(6);
    expect(da.median()).toBe(3.5);
  });

  test('std is the population form (ddof=0), matching xarray', () => {
    // mean 3.5, squared deviations 6.25+2.25+0.25+0.25+2.25+6.25 = 17.5
    // population variance = 17.5 / 6, NOT 17.5 / 5 (the sample form).
    const da = grid();
    expect(da.std() as number).toBeCloseTo(Math.sqrt(17.5 / 6), 12);
  });

  test('median averages the middle pair for an even count', () => {
    const da = new DataArray([4, 1, 3, 2], { dims: ['x'], coords: { x: [0, 1, 2, 3] } });
    expect(da.median()).toBe(2.5);
  });

  test('median picks the middle value for an odd count', () => {
    const da = new DataArray([5, 1, 3], { dims: ['x'], coords: { x: [0, 1, 2] } });
    expect(da.median()).toBe(3);
  });

  test('median does not assume the input is sorted', () => {
    const ascending = new DataArray([1, 2, 3, 4, 100], {
      dims: ['x'],
      coords: { x: [0, 1, 2, 3, 4] }
    });
    const shuffled = new DataArray([100, 3, 1, 4, 2], {
      dims: ['x'],
      coords: { x: [0, 1, 2, 3, 4] }
    });
    expect(ascending.median()).toBe(3);
    expect(shuffled.median()).toBe(3);
  });

  test('a single value reduces to itself, with zero spread', () => {
    const da = new DataArray([7], { dims: ['x'], coords: { x: [0] } });
    expect(da.min()).toBe(7);
    expect(da.max()).toBe(7);
    expect(da.median()).toBe(7);
    expect(da.std()).toBe(0);
  });

  test('negative values do not confuse min/max', () => {
    // A min seeded at 0 rather than at the first observed value would wrongly
    // report 0 here; likewise a max over an all-negative array.
    const da = new DataArray([-5, -2, -9], { dims: ['x'], coords: { x: [0, 1, 2] } });
    expect(da.min()).toBe(-9);
    expect(da.max()).toBe(-2);
  });
});

describe('reductions along a dimension', () => {
  test('collapse the named dimension and keep the other coords', () => {
    const reduced = grid().max('x') as DataArray;
    expect(reduced.data).toEqual([3, 6]);
    expect(reduced.dims).toEqual(['y']);
    expect(reduced.coords.y).toEqual([0, 1]);
  });

  test('reduce the outer dimension', () => {
    expect((grid().min('y') as DataArray).data).toEqual([1, 2, 3]);
    expect((grid().max('y') as DataArray).data).toEqual([4, 5, 6]);
  });

  test('median and std along a dimension', () => {
    const da = new DataArray(
      [
        [1, 2, 3],
        [10, 20, 30]
      ],
      { dims: ['y', 'x'], coords: { y: [0, 1], x: [0, 1, 2] } }
    );
    expect((da.median('x') as DataArray).data).toEqual([2, 20]);

    const std = (da.std('x') as DataArray).data as number[];
    expect(std[0]).toBeCloseTo(Math.sqrt(2 / 3), 12);
    expect(std[1]).toBeCloseTo(Math.sqrt(200 / 3), 12);
  });

  test('reducing the only dimension yields a scalar, not a DataArray', () => {
    const da = new DataArray([3, 1, 2], { dims: ['x'], coords: { x: [0, 1, 2] } });
    expect(da.min('x')).toBe(1);
    expect(da.max('x')).toBe(3);
    expect(da.median('x')).toBe(2);
  });

  test('an unknown dimension throws rather than silently reducing nothing', () => {
    expect(() => grid().min('nope')).toThrow(/not found/);
    expect(() => grid().median('nope')).toThrow(/not found/);
  });
});

describe('masked values are skipped (xarray skipna)', () => {
  const masked = () => {
    const da = new DataArray([1, 2, 3, 4], { dims: ['x'], coords: { x: [0, 1, 2, 3] } });
    const cond = new DataArray([true, true, false, false], {
      dims: ['x'],
      coords: { x: [0, 1, 2, 3] }
    });
    return da.where(cond); // [1, 2, null, null]
  };

  test('whole-array reductions ignore masked leaves', () => {
    const m = masked();
    expect(m.data).toEqual([1, 2, null, null]);
    expect(m.min()).toBe(1);
    expect(m.max()).toBe(2);
    expect(m.median()).toBe(1.5);
    expect(m.std() as number).toBeCloseTo(0.5, 12);
  });

  test('dimension reductions ignore masked leaves', () => {
    const m = masked();
    expect(m.min('x')).toBe(1);
    expect(m.max('x')).toBe(2);
    expect(m.median('x')).toBe(1.5);
  });

  test('an all-masked slice reduces to NaN, not 0 or Infinity', () => {
    // The important one for gridded data: a box entirely over masked cells must
    // read as "no data" downstream. Infinity (from a min seeded at +Inf) or 0
    // would both be silently plausible-looking numbers.
    const da = new DataArray([1, 2], { dims: ['x'], coords: { x: [0, 1] } });
    const none = new DataArray([false, false], { dims: ['x'], coords: { x: [0, 1] } });
    const m = da.where(none);

    expect(Number.isNaN(m.min() as number)).toBe(true);
    expect(Number.isNaN(m.max() as number)).toBe(true);
    expect(Number.isNaN(m.median() as number)).toBe(true);
    expect(Number.isNaN(m.std() as number)).toBe(true);
  });

  test('a partially masked row reduces over only its valid cells', () => {
    const da = new DataArray(
      [
        [1, 2],
        [3, 4]
      ],
      { dims: ['y', 'x'], coords: { y: [0, 1], x: [0, 1] } }
    );
    const cond = new DataArray(
      [
        [true, false],
        [true, true]
      ],
      { dims: ['y', 'x'], coords: { y: [0, 1], x: [0, 1] } }
    );

    const reduced = da.where(cond).max('x') as DataArray;
    // Row 0 keeps only 1; row 1 keeps 3 and 4.
    expect(reduced.data).toEqual([1, 4]);
  });
});

describe("'' is a real dimension name, not an absent one", () => {
  // `!dim` treats the empty string as "no dimension given" and reduces the whole
  // array instead of the named one. Zarr stores can carry an unnamed dimension,
  // so this is reachable rather than theoretical.
  const grid3x2 = () =>
    new DataArray(
      [
        [1, 2],
        [3, 4],
        [5, 6]
      ],
      { dims: ['', 'x'], coords: { '': [0, 1, 2], x: [0, 1] } }
    );

  test('reducing a dimension named "" collapses only that dimension', () => {
    const reduced = grid3x2().min('') as DataArray;
    expect(reduced.data).toEqual([1, 2]);
    expect(reduced.dims).toEqual(['x']);

    expect((grid3x2().max('') as DataArray).data).toEqual([5, 6]);
    expect((grid3x2().median('') as DataArray).data).toEqual([3, 4]);
  });

  test('sum and mean honour it too', () => {
    expect((grid3x2().sum('') as DataArray).data).toEqual([9, 12]);
    expect((grid3x2().mean('') as DataArray).data).toEqual([3, 4]);
  });

  test('omitting the dimension still reduces everything', () => {
    expect(grid3x2().min()).toBe(1);
    expect(grid3x2().max()).toBe(6);
  });
});

describe('arrays built from nested data', () => {
  // These have no row-major storage, so the dimensional reductions take the
  // nested path. It must agree with the flat one rather than being a
  // second implementation that drifts.
  const nested = () =>
    new DataArray(
      [
        [1, 2, 3],
        [4, 5, 6]
      ],
      { dims: ['y', 'x'], coords: { y: [0, 1], x: [0, 1, 2] } }
    );

  test('the nested path has no flat storage to reduce', () => {
    expect(nested().flatData).toBeNull();
  });

  test('dimensional reductions match the flat path', () => {
    expect((nested().min('x') as DataArray).data).toEqual([1, 4]);
    expect((nested().max('x') as DataArray).data).toEqual([3, 6]);
    expect((nested().median('x') as DataArray).data).toEqual([2, 5]);

    expect((nested().min('y') as DataArray).data).toEqual([1, 2, 3]);
    expect((nested().max('y') as DataArray).data).toEqual([4, 5, 6]);
  });

  test('reducing the only dimension of a nested 1D array yields a scalar', () => {
    const da = new DataArray([3, 1, 2], { dims: ['x'], coords: { x: [0, 1, 2] } });
    expect(da.flatData).toBeNull();
    expect(da.min('x')).toBe(1);
    expect(da.max('x')).toBe(3);
    expect(da.median('x')).toBe(2);
  });

  test('masked cells are skipped on the nested path as well', () => {
    const da = new DataArray(
      [
        [1, 2],
        [3, 4]
      ],
      { dims: ['y', 'x'], coords: { y: [0, 1], x: [0, 1] } }
    );
    const cond = new DataArray(
      [
        [true, false],
        [false, false]
      ],
      { dims: ['y', 'x'], coords: { y: [0, 1], x: [0, 1] } }
    );

    const reduced = da.where(cond).max('x') as DataArray;
    const values = reduced.data as number[];
    expect(values[0]).toBe(1);
    // Row 1 is entirely masked: NaN, not 0 or -Infinity.
    expect(Number.isNaN(values[1])).toBe(true);
  });
});

describe('numerical robustness', () => {
  test('median of two huge values does not overflow to Infinity', () => {
    // (a + b) / 2 overflows once the pair sums past Number.MAX_VALUE, even
    // though the median itself is perfectly representable.
    const da = new DataArray([Number.MAX_VALUE, Number.MAX_VALUE], {
      dims: ['x'],
      coords: { x: [0, 1] }
    });
    expect(da.median()).toBe(Number.MAX_VALUE);

    const asymmetric = new DataArray([1e308, 1.5e308], { dims: ['x'], coords: { x: [0, 1] } });
    expect(asymmetric.median()).toBe(1.25e308);

    const negative = new DataArray([-Number.MAX_VALUE, -Number.MAX_VALUE], {
      dims: ['x'],
      coords: { x: [0, 1] }
    });
    expect(negative.median()).toBe(-Number.MAX_VALUE);
  });

  test('the overflow-safe midpoint keeps ordinary medians exact', () => {
    // Halving before adding must not cost precision on normal-magnitude data.
    const da = new DataArray([1, 2, 3, 4], { dims: ['x'], coords: { x: [0, 1, 2, 3] } });
    expect(da.median()).toBe(2.5);

    const decimals = new DataArray([0.1, 0.30000000000000004], {
      dims: ['x'],
      coords: { x: [0, 1] }
    });
    expect(decimals.median()).toBeCloseTo(0.2, 15);
  });

  test('a large flat array reduces without a per-element stack', () => {
    // The traversal keeps one frame per level of nesting rather than pushing
    // every child, so extra space is O(depth), not O(n).
    //
    // Asserted by correctness at a size where the old per-element stack was
    // measurably costly (3M elements held ~66 MB of references, vs ~4 MB now).
    // A heap-delta assertion would be too GC-dependent to trust in a parallel
    // suite, so this pins behaviour and leaves the memory claim to the comment.
    const n = 1_000_000;
    const values = Array.from({ length: n }, (_, i) => i);
    const da = new DataArray(values, { dims: ['x'], coords: { x: values } });

    expect(da.min()).toBe(0);
    expect(da.max()).toBe(n - 1);
    expect(da.mean()).toBeCloseTo((n - 1) / 2, 6);
  });

  test('deeply nested arrays still reduce correctly', () => {
    // Frame-based traversal must handle nesting, not just the flat case.
    const da = new DataArray(
      [
        [
          [1, 2],
          [3, 4]
        ],
        [
          [5, 6],
          [7, 8]
        ]
      ],
      { dims: ['z', 'y', 'x'], coords: { z: [0, 1], y: [0, 1], x: [0, 1] } }
    );
    expect(da.min()).toBe(1);
    expect(da.max()).toBe(8);
    expect(da.median()).toBe(4.5);
  });

  test('std stays accurate for values with a large offset', () => {
    // Kelvin temperatures: the naive E[x^2] - E[x]^2 form subtracts two nearly
    // equal ~85000 magnitudes and loses most of its precision. Welford does not.
    const values = [292.15, 293.15, 294.15, 295.15, 296.15];
    const da = new DataArray(values, { dims: ['x'], coords: { x: [0, 1, 2, 3, 4] } });

    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const expected = Math.sqrt(
      values.reduce((acc, v) => acc + (v - mean) ** 2, 0) / values.length
    );

    expect(da.std() as number).toBeCloseTo(expected, 10);
    // Same spread as the 0-based series, just shifted.
    expect(da.std() as number).toBeCloseTo(Math.sqrt(2), 10);
  });

  test('reductions agree whether or not the array was sliced first', async () => {
    // sel() can leave the block lazy, which takes the materialize path rather
    // than the row-major one. Both must agree.
    const da = new DataArray(
      [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9]
      ],
      { dims: ['y', 'x'], coords: { y: [0, 1, 2], x: [0, 1, 2] } }
    );

    const sliced = await da.isel({ y: [0, 1] });
    expect(sliced.min()).toBe(1);
    expect(sliced.max()).toBe(6);
    expect(sliced.median()).toBe(3.5);
  });
});
