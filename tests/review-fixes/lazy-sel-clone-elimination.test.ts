import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

const TIME_LENGTH = 50_000;
const STATION_COORDS = ['north', 'central', 'south'];
const TIME_COORDS = Array.from(
  { length: TIME_LENGTH },
  (_, index) => `time-${index.toString().padStart(5, '0')}`
);
const SOURCE_DATA = Array.from({ length: TIME_LENGTH }, (_, time) =>
  STATION_COORDS.map((_, station) => time * 10 + station)
);

const ZARR_STYLE_ATTRS = {
  long_name: 'synthetic observations',
  _zarr_path: 'observations',
  _zarr_shape: [TIME_LENGTH, STATION_COORDS.length],
  _zarr_coords: {
    time: TIME_COORDS,
    station: STATION_COORDS
  },
  _coordAttrs: {
    time: { long_name: 'sample time' },
    station: { long_name: 'station name' }
  },
  _zarr_data_type: 'float64',
  codecs: []
};

type IndexRange = { start: number; stop: number } | number;

function makeLazyArray() {
  const loadState = { calls: 0 };
  const loader = async (ranges: Record<string, IndexRange>) => {
    loadState.calls++;

    const timeRange = ranges.time ?? { start: 0, stop: TIME_LENGTH };
    const stationRange = ranges.station ?? { start: 0, stop: STATION_COORDS.length };
    const rows = typeof timeRange === 'number'
      ? [SOURCE_DATA[timeRange]]
      : SOURCE_DATA.slice(timeRange.start, timeRange.stop);
    const selected = rows.map(row => typeof stationRange === 'number'
      ? [row[stationRange]]
      : row.slice(stationRange.start, stationRange.stop));

    if (typeof timeRange === 'number' && typeof stationRange === 'number') {
      return selected[0][0];
    }
    if (typeof timeRange === 'number') return selected[0];
    if (typeof stationRange === 'number') return selected.map(row => row[0]);
    return selected;
  };

  const array = new DataArray(null, {
    lazy: true,
    virtualShape: [TIME_LENGTH, STATION_COORDS.length],
    lazyLoader: loader,
    dims: ['time', 'station'],
    coords: { time: TIME_COORDS, station: STATION_COORDS },
    attrs: ZARR_STYLE_ATTRS,
    name: 'observations'
  });

  return { array, loadState };
}

function makeEagerArray() {
  return new DataArray(SOURCE_DATA, {
    dims: ['time', 'station'],
    coords: { time: TIME_COORDS, station: STATION_COORDS },
    attrs: ZARR_STYLE_ATTRS,
    name: 'observations'
  });
}

describe('lazy sel clone elimination correctness', () => {
  test('chained sel -> isel -> compute matches the eager selection', async () => {
    const { array: lazy } = makeLazyArray();
    const eager = makeEagerArray();
    const selection = { time: TIME_COORDS[12_345] };
    const positionalSelection = { station: [2, 0] };

    const lazySelected = await lazy.sel(selection);
    const lazyComputed = await (await lazySelected.isel(positionalSelection)).compute();
    const eagerComputed = await (await eager.sel(selection)).isel(positionalSelection);

    expect(lazyComputed.data).toEqual(eagerComputed.data);
    expect(lazyComputed.coords).toEqual(eagerComputed.coords);
    expect(lazyComputed.dims).toEqual(eagerComputed.dims);
    expect(lazyComputed.shape).toEqual(eagerComputed.shape);
  });

  test('sel results isolate public attrs and coords from the source', async () => {
    const { array: lazy } = makeLazyArray();
    const selected = await lazy.sel({ time: TIME_COORDS[0] });

    selected.attrs.long_name = 'selected observations';
    selected.coords.station[0] = 'selected north';

    expect(lazy.attrs.long_name).toBe('synthetic observations');
    expect(lazy.coords.station[0]).toBe('north');

    lazy.attrs.long_name = 'original observations';
    lazy.coords.station[1] = 'original central';

    expect(selected.attrs.long_name).toBe('selected observations');
    expect(selected.coords.station[1]).toBe('central');
  });
});

describe('perf', () => {
  // Thresholds assume an Apple M1 Max-class machine, with generous margins.
  test('200 sequential metadata-only point sels stay under 1000ms', async () => {
    const { array: lazy, loadState } = makeLazyArray();
    let allResultsStayedLazy = true;

    const start = performance.now();
    for (let index = 0; index < 200; index++) {
      const selected = await lazy.sel({ time: TIME_COORDS[0] });
      allResultsStayedLazy = allResultsStayedLazy && selected.isLazy;
    }
    const elapsed = performance.now() - start;
    const perSel = elapsed / 200;

    console.info(
      `200 metadata-only lazy point sels: ${elapsed.toFixed(2)}ms total, ` +
      `${perSel.toFixed(3)}ms/sel`
    );
    expect(allResultsStayedLazy).toBe(true);
    expect(loadState.calls).toBe(0);
    expect(
      elapsed,
      `expected 200 sels to take <1000ms; measured ${elapsed.toFixed(2)}ms ` +
      `(${perSel.toFixed(3)}ms/sel)`
    ).toBeLessThan(1000);
  }, 30_000);
});
