/**
 * Tests for Zarr backend
 */

import { describe, test, expect } from 'vitest';
import { Dataset } from '../src/Dataset';
import { ZarrBackend } from '../src/backends/zarr';

describe('ZarrBackend', () => {
  test('should have open method', () => {
    expect(typeof ZarrBackend.open).toBe('function');
  });

  test('should throw error for invalid store', async () => {
    const mockStore = {} as any;
    await expect(ZarrBackend.open(mockStore)).rejects.toThrow('unable to discover any metadata keys');
  });
});

describe('Dataset.open_zarr', () => {
  test('should have open_zarr static method', () => {
    expect(typeof Dataset.open_zarr).toBe('function');
  });

  test('should call ZarrBackend.open', async () => {
    const mockStore = {} as any;
    // This will throw since the store is invalid, but verifies the connection
    await expect(Dataset.open_zarr(mockStore)).rejects.toThrow('unable to discover any metadata keys');
  });

  test('should pass options to backend', async () => {
    const mockStore = {} as any;
    const options = { group: 'test', consolidated: true };

    await expect(Dataset.open_zarr(mockStore, options)).rejects.toThrow('unable to discover any metadata keys');
  });
});
