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

  test('should throw error when not implemented', async () => {
    const mockStore = {};
    await expect(ZarrBackend.open(mockStore)).rejects.toThrow('not yet implemented');
  });
});

describe('Dataset.open_zarr', () => {
  test('should have open_zarr static method', () => {
    expect(typeof Dataset.open_zarr).toBe('function');
  });

  test('should call ZarrBackend.open', async () => {
    const mockStore = {};
    // This will throw since it's not implemented yet, but verifies the connection
    await expect(Dataset.open_zarr(mockStore)).rejects.toThrow('not yet implemented');
  });

  test('should pass options to backend', async () => {
    const mockStore = {};
    const options = { group: 'test', consolidated: true };

    await expect(Dataset.open_zarr(mockStore, options)).rejects.toThrow('not yet implemented');
  });
});
