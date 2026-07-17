import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

const SOURCE_LENGTH = 5_000;
const CHUNK = 100;
const SOURCE = Array.from({ length: SOURCE_LENGTH }, (_, index) => index * 2);
const TIME_COORDS = Array.from({ length: SOURCE_LENGTH }, (_, index) => index);

type IndexRange = { start: number; stop: number } | number;

function makeInstrumentedLazyArray() {
  const fetchWork = {
    loaderRequestCount: 0,
    totalElementsFetched: 0,
    distinctChunksTouched: new Set<number>()
  };

  const resetFetchWork = () => {
    fetchWork.loaderRequestCount = 0;
    fetchWork.totalElementsFetched = 0;
    fetchWork.distinctChunksTouched.clear();
  };

  const lazyLoader = async (ranges: Record<string, IndexRange>) => {
    fetchWork.loaderRequestCount++;
    const timeRange = ranges.time ?? { start: 0, stop: SOURCE_LENGTH };

    if (typeof timeRange === 'number') {
      fetchWork.totalElementsFetched++;
      fetchWork.distinctChunksTouched.add(Math.floor(timeRange / CHUNK));
      return SOURCE[timeRange];
    }

    fetchWork.totalElementsFetched += timeRange.stop - timeRange.start;
    if (timeRange.stop > timeRange.start) {
      const firstChunk = Math.floor(timeRange.start / CHUNK);
      const lastChunk = Math.floor((timeRange.stop - 1) / CHUNK);
      for (let chunk = firstChunk; chunk <= lastChunk; chunk++) {
        fetchWork.distinctChunksTouched.add(chunk);
      }
    }
    return SOURCE.slice(timeRange.start, timeRange.stop);
  };

  const lazy = new DataArray(null, {
    lazy: true,
    virtualShape: [SOURCE_LENGTH],
    lazyLoader,
    dims: ['time'],
    coords: { time: TIME_COORDS }
  });

  return { lazy, fetchWork, resetFetchWork };
}

describe('lazy discrete isel correctness', () => {
  test('materializes two far-apart indices with their coordinates', async () => {
    const { lazy } = makeInstrumentedLazyArray();

    const selected = await lazy.isel({ time: [0, 4999] });
    const computed = await selected.compute();

    expect(computed.data).toEqual([0, 9998]);
    expect(computed.coords.time).toEqual([0, 4999]);
  });

  test('preserves the order of scattered indices', async () => {
    const { lazy } = makeInstrumentedLazyArray();

    const selected = await lazy.isel({ time: [3, 250, 4999] });
    const computed = await selected.compute();

    expect(computed.data).toEqual([6, 500, 9998]);
    expect(computed.coords.time).toEqual([3, 250, 4999]);
  });
});

describe('perf', () => {
  // Thresholds assume an Apple M1 Max-class machine with generous margins. These
  // assertions count fetch work rather than time, so they are machine-independent.
  test('fetches work proportional to the chunks containing discrete indices', async () => {
    const { lazy, fetchWork, resetFetchWork } = makeInstrumentedLazyArray();
    resetFetchWork();

    const selected = await lazy.isel({ time: [0, 4999] });
    await selected.compute();

    console.info(
      'discrete isel fetch work: ' +
      `${fetchWork.totalElementsFetched} elements, ` +
      `${fetchWork.distinctChunksTouched.size} chunks, ` +
      `${fetchWork.loaderRequestCount} loader requests`
    );
    expect.soft(fetchWork.totalElementsFetched).toBeLessThanOrEqual(400);
    expect.soft(fetchWork.distinctChunksTouched.size).toBeLessThanOrEqual(4);
  });
});
