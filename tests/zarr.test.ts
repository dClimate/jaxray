/**
 * Tests for Zarr backend
 */

import { describe, test, expect } from 'vitest';
import { Dataset } from '../src/Dataset';
import { ZarrBackend } from '../src/backends/zarr';
import { MemoryZarrStore } from './helpers/MemoryZarrStore';

describe('ZarrBackend', () => {
  test('should have open method', () => {
    expect(typeof ZarrBackend.open).toBe('function');
  });

  test('should throw error for invalid store', async () => {
    const mockStore = {} as any;
    await expect(ZarrBackend.open(mockStore)).rejects.toThrow('unable to discover any metadata keys');
  });

  test('should throw error when no zarr.json files found under specified group', async () => {
    const store = new MemoryZarrStore({
      'other-group/zarr.json': { node_type: 'group', attributes: {} }
    });

    await expect(ZarrBackend.open(store, { group: 'missing-group' }))
      .rejects.toThrow('no zarr.json under group "missing-group"');
  });

  test('should throw error when zarr.json files exist but none are arrays', async () => {
    const store = new MemoryZarrStore({
      'mygroup/zarr.json': { node_type: 'group', attributes: {} }
    });

    await expect(ZarrBackend.open(store, { group: 'mygroup' }))
      .rejects.toThrow('found zarr.json files, but none were arrays');
  });

  test('should handle invalid JSON in zarr.json files gracefully', async () => {
    const store = new MemoryZarrStore({
      'zarr.json': { node_type: 'group', attributes: {} },
      'array2/zarr.json': {
        node_type: 'array',
        shape: [10],
        dimension_names: ['x'],
        attributes: {}
      }
    });

    // Add invalid JSON manually
    store.set('array1/zarr.json', 'invalid json{{{');

    // Should skip invalid JSON and continue with valid array
    const dataset = await ZarrBackend.open(store);
    expect(dataset).toBeDefined();
  });

  test('should use fallback dimension names when dimension_names not provided', async () => {
    const store = new MemoryZarrStore({
      'zarr.json': { node_type: 'group', attributes: {} },
      'data/zarr.json': {
        node_type: 'array',
        shape: [5, 10],
        attributes: { test: 'value' }
      }
    });

    const dataset = await ZarrBackend.open(store);
    const dataVar = dataset.getVariable('data');
    expect(dataVar.dims).toEqual(['dim_0', 'dim_1']);
  });

  test('should handle coordinate-only datasets', async () => {
    const store = new MemoryZarrStore({
      'zarr.json': { node_type: 'group', attributes: {} },
      'x/zarr.json': {
        node_type: 'array',
        shape: [5],
        dimension_names: ['x'],
        attributes: { units: 'meters' }
      },
      'y/zarr.json': {
        node_type: 'array',
        shape: [3],
        dimension_names: ['y'],
        attributes: { units: 'meters' }
      }
    });

    const dataset = await ZarrBackend.open(store);

    // Should promote coords to data variables
    expect(dataset.dataVars).toContain('x');
    expect(dataset.dataVars).toContain('y');
  });

  test('should use fallback positional coords when named coord does not exist', async () => {
    const store = new MemoryZarrStore({
      'zarr.json': { node_type: 'group', attributes: {} },
      'data/zarr.json': {
        node_type: 'array',
        shape: [5, 10],
        dimension_names: ['time', 'space'],
        attributes: {}
      }
    });

    const dataset = await ZarrBackend.open(store);
    const dataVar = dataset.getVariable('data');

    // Should have fallback positional coords [0, 1, 2, 3, 4] for time and [0..9] for space
    expect(dataVar.attrs._zarr_coords.time).toEqual([0, 1, 2, 3, 4]);
    expect(dataVar.attrs._zarr_coords.space).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  test('should create lazy arrays with correct metadata', async () => {
    const store = new MemoryZarrStore({
      'zarr.json': { node_type: 'group', attributes: {} },
      'data/zarr.json': {
        node_type: 'array',
        shape: [5, 10, 3],
        dimension_names: ['x', 'y', 'z'],
        attributes: {}
      }
    });

    const dataset = await ZarrBackend.open(store);
    const dataVar = dataset.getVariable('data');

    // Test lazy array metadata
    expect(dataVar.attrs._lazy).toBe(true);
    expect(dataVar.attrs._zarr_path).toBe('data');
  });

  test('should store lazy loading metadata', async () => {
    const store = new MemoryZarrStore({
      'zarr.json': { node_type: 'group', attributes: {} },
      'data/zarr.json': {
        node_type: 'array',
        shape: [5, 10],
        dimension_names: ['x', 'y'],
        attributes: { custom: 'attribute' }
      }
    });

    const dataset = await ZarrBackend.open(store);
    const dataVar = dataset.getVariable('data');

    // Verify lazy loading metadata is stored correctly
    expect(dataVar.attrs._lazy).toBe(true);
    expect(dataVar.attrs._zarr_shape).toEqual([5, 10]);
    expect(dataVar.attrs.custom).toBe('attribute');
  });

  test('should correctly set virtualShape for lazy arrays', async () => {
    const store = new MemoryZarrStore({
      'zarr.json': { node_type: 'group', attributes: {} },
      'data/zarr.json': {
        node_type: 'array',
        shape: [5, 10],
        dimension_names: ['x', 'y'],
        attributes: {}
      }
    });

    const dataset = await ZarrBackend.open(store);
    const dataVar = dataset.getVariable('data');

    // Verify shape is set correctly
    expect(dataVar.shape).toEqual([5, 10]);
  });

  test('should parse group attributes from root zarr.json', async () => {
    const store = new MemoryZarrStore({
      'zarr.json': {
        node_type: 'group',
        attributes: {
          title: 'Test Dataset',
          version: '1.0'
        }
      },
      'data/zarr.json': {
        node_type: 'array',
        shape: [10],
        dimension_names: ['x'],
        attributes: {}
      }
    });

    const dataset = await ZarrBackend.open(store);

    expect(dataset.attrs.title).toBe('Test Dataset');
    expect(dataset.attrs.version).toBe('1.0');
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
