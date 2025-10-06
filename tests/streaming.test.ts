/**
 * Tests for streaming functionality
 */

import { describe, test, expect } from 'vitest';
import { DataArray, Dataset } from '../src';

describe('DataArray streaming', () => {
  test('should stream large selection in chunks', async () => {
    // Create a large time series
    const data = Array.from({ length: 100 }, (_, i) => i);
    const da = new DataArray(data, {
      dims: ['time'],
      coords: {
        time: Array.from({ length: 100 }, (_, i) => i)
      }
    });

    const chunks: number[] = [];
    const stream = da.selStream(
      { time: [0, 99] },
      { chunkSize: 0.0001 } // 0.1KB chunks to force multiple chunks
    );

    for await (const chunk of stream) {
      expect(chunk.data).toBeInstanceOf(DataArray);
      expect(chunk.progress).toBeGreaterThanOrEqual(0);
      expect(chunk.progress).toBeLessThanOrEqual(100);
      expect(chunk.bytesProcessed).toBeLessThanOrEqual(chunk.totalBytes);
      chunks.push(chunk.chunkIndex);
    }

    // Should have multiple chunks
    expect(chunks.length).toBeGreaterThan(1);
    // Chunks should be sequential
    expect(chunks).toEqual(Array.from({ length: chunks.length }, (_, i) => i));
  });

  test('should stream with nearest neighbor method', async () => {
    const data = [10, 20, 30, 40, 50];
    const da = new DataArray(data, {
      dims: ['x'],
      coords: {
        x: [0, 5, 10, 15, 20]
      }
    });

    const chunks: DataArray[] = [];
    const stream = da.selStream(
      { x: [2, 18] },
      { method: 'nearest', chunkSize: 0.001 }
    );

    for await (const chunk of stream) {
      chunks.push(chunk.data);
      expect(chunk.progress).toBeGreaterThan(0);
    }

    expect(chunks.length).toBeGreaterThan(0);
  });

  test('should handle single value selection', async () => {
    const data = [10, 20, 30];
    const da = new DataArray(data, {
      dims: ['x'],
      coords: {
        x: [0, 5, 10]
      }
    });

    const chunks: DataArray[] = [];
    const stream = da.selStream({ x: 5 });

    for await (const chunk of stream) {
      chunks.push(chunk.data);
      expect(chunk.progress).toBe(100);
      expect(chunk.chunkIndex).toBe(0);
      expect(chunk.totalChunks).toBe(1);
    }

    expect(chunks.length).toBe(1);
    expect(chunks[0].data).toBe(20);
  });

  test('should stream multi-dimensional data', async () => {
    const data = Array.from({ length: 10 }, (_, i) =>
      Array.from({ length: 5 }, (_, j) => i * 5 + j)
    );
    const da = new DataArray(data, {
      dims: ['time', 'x'],
      coords: {
        time: Array.from({ length: 10 }, (_, i) => i),
        x: Array.from({ length: 5 }, (_, i) => i)
      }
    });

    const chunks: DataArray[] = [];
    const stream = da.selStream(
      { time: [0, 9], x: 2 },
      { chunkSize: 0.001 }
    );

    for await (const chunk of stream) {
      chunks.push(chunk.data);
      expect(chunk.data.shape.length).toBe(1); // x dimension dropped
    }

    expect(chunks.length).toBeGreaterThan(0);
  });

  test('should report accurate progress', async () => {
    const data = Array.from({ length: 50 }, (_, i) => i);
    const da = new DataArray(data, {
      dims: ['time'],
      coords: {
        time: Array.from({ length: 50 }, (_, i) => i)
      }
    });

    const progresses: number[] = [];
    const stream = da.selStream(
      { time: [0, 49] },
      { chunkSize: 0.001 }
    );

    for await (const chunk of stream) {
      progresses.push(chunk.progress);
    }

    // Progress should be monotonically increasing
    for (let i = 1; i < progresses.length; i++) {
      expect(progresses[i]).toBeGreaterThanOrEqual(progresses[i - 1]);
    }

    // Last progress should be 100
    expect(progresses[progresses.length - 1]).toBe(100);
  });
});

describe('Dataset streaming', () => {
  test('should stream dataset in chunks', async () => {
    const temp = new DataArray(
      Array.from({ length: 50 }, (_, i) => i),
      {
        dims: ['time'],
        coords: {
          time: Array.from({ length: 50 }, (_, i) => i)
        }
      }
    );
    const pressure = new DataArray(
      Array.from({ length: 50 }, (_, i) => i * 100),
      {
        dims: ['time'],
        coords: {
          time: Array.from({ length: 50 }, (_, i) => i)
        }
      }
    );

    const ds = new Dataset({ temperature: temp, pressure: pressure });

    const chunks: Dataset[] = [];
    const stream = ds.selStream(
      { time: [0, 49] },
      { chunkSize: 0.0001 } // 0.1KB chunks
    );

    for await (const chunk of stream) {
      expect(chunk.data).toBeInstanceOf(Dataset);
      expect(chunk.data.dataVars).toContain('temperature');
      expect(chunk.data.dataVars).toContain('pressure');
      chunks.push(chunk.data);
    }

    expect(chunks.length).toBeGreaterThan(1);
  });

  test('should stream with different dimensions per variable', async () => {
    const temp = new DataArray(
      Array.from({ length: 20 }, (_, i) =>
        Array.from({ length: 3 }, (_, j) => i * 3 + j)
      ),
      {
        dims: ['time', 'x'],
        coords: {
          time: Array.from({ length: 20 }, (_, i) => i),
          x: Array.from({ length: 3 }, (_, i) => i)
        }
      }
    );
    const pressure = new DataArray(
      Array.from({ length: 20 }, (_, i) => i * 100),
      {
        dims: ['time'],
        coords: {
          time: Array.from({ length: 20 }, (_, i) => i)
        }
      }
    );

    const ds = new Dataset({ temperature: temp, pressure: pressure });

    const chunks: Dataset[] = [];
    const stream = ds.selStream(
      { time: [0, 19], x: 1 },
      { chunkSize: 0.001 }
    );

    for await (const chunk of stream) {
      chunks.push(chunk.data);
      // Temperature should have x dimension dropped
      expect(chunk.data.getVariable('temperature').dims).toEqual(['time']);
      // Pressure should only have time dimension
      expect(chunk.data.getVariable('pressure').dims).toEqual(['time']);
    }

    expect(chunks.length).toBeGreaterThan(0);
  });

  test('should handle streaming with method options', async () => {
    const temp = new DataArray([10, 20, 30, 40, 50], {
      dims: ['x'],
      coords: {
        x: [0, 5, 10, 15, 20]
      }
    });

    const ds = new Dataset({ temperature: temp });

    const chunks: Dataset[] = [];
    const stream = ds.selStream(
      { x: [2, 18] },
      { method: 'nearest', chunkSize: 0.001 }
    );

    for await (const chunk of stream) {
      chunks.push(chunk.data);
    }

    expect(chunks.length).toBeGreaterThan(0);
  });
});
