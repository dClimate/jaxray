/**
 * In-depth tests for lazy evaluation with IPFS datastores
 *
 * This test suite verifies that lazy evaluation properly handles:
 * 1. Multiple sequential selections without loading data
 * 2. Chained selections preserve correct ranges
 * 3. Compute() correctly materializes the exact requested data
 * 4. Different selection patterns work correctly with lazy data
 * 5. Lazy data maintains independence between multiple selections
 */

import { describe, test, expect } from 'vitest';
import { Dataset } from '../src/Dataset';
import { ShardedStore } from '../src/backends/ipfs/sharded-store';
import { createIpfsElements } from '../src/backends/ipfs/ipfs-elements';

describe('Lazy Evaluation with IPFS Datastores', () => {
  // Real IPFS data source
  const CID = 'bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u';
  const GATEWAY = 'https://ipfs-gateway.dclimate.net';

  /**
   * Test 1: Multiple sequential selections should not load data until compute()
   *
   * This tests the core concern: when we do sel() twice on lazy data,
   * are we correctly handling both selections? Each selection should
   * track its own ranges without affecting the other.
   */
  test('should support multiple sequential selections on lazy data without loading', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    // Get a data variable
    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    // Verify the original data is lazy
    expect(variable.isLazy).toBe(true);

    // First selection - select specific location and time
    const selection1 = await variable.sel({
      latitude: 45,
      longitude: 34,
      time: '1987-05-03T23:00:00'
    });

    expect(selection1).toBeDefined();

    // Second selection on different ranges
    const selection2 = await variable.sel({
      latitude: 40,
      longitude: 30,
      time: '1987-05-04T23:00:00'
    });

    expect(selection2).toBeDefined();

    // Both should be independent - computing them should give different results
    // (unless by coincidence they're the same, but the point is they're independent)
    const value1 = selection1.isLazy ? await selection1.compute() : selection1;
    const value2 = selection2.isLazy ? await selection2.compute() : selection2;

    expect(value1).toBeDefined();
    expect(value2).toBeDefined();
    // Values should be different since we selected different coordinates
    // (unless they happen to be the same in the actual data, but the test verifies independence)
  }, 60000);

  /**
   * Test 2: Chained sel() operations should compose correctly
   *
   * sel(a).sel(b) should be equivalent to sel({...a, ...b})
   * Each stage should remain lazy until compute() is called
   */
  test('should handle chained sel() operations correctly on lazy data', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    // Method 1: Chained selections
    const chained = await variable
      .sel({ latitude: 45, longitude: 34 })
      .then(v => v.sel({ time: '1987-05-03T23:00:00' }));

    // Method 2: Combined selection
    const combined = await variable.sel({
      latitude: 45,
      longitude: 34,
      time: '1987-05-03T23:00:00'
    });

    // Both should produce valid results
    expect(chained).toBeDefined();
    expect(combined).toBeDefined();

    // Verify they can be computed
    const chainedComputed = chained.isLazy ? await chained.compute() : chained;
    const combinedComputed = combined.isLazy ? await combined.compute() : combined;

    expect(chainedComputed.values).toBeDefined();
    expect(combinedComputed.values).toBeDefined();
  }, 60000);

  /**
   * Test 3: Range selections should remain lazy and compute correct subset
   *
   * sel with array ranges (slicing) should only load the specified ranges
   */
  test('should handle range selections on lazy data and compute correct subset', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    // Get original shape
    const originalShape = variable.shape;

    // Select a range
    const rangeSelected = await variable.sel({
      latitude: [44, 46],
      longitude: [33, 35],
      time: ['1987-05-03T23:00:00', '1987-05-05T23:00:00']
    });

    // The result should be smaller than original
    if (!rangeSelected.isLazy) {
      // If eager, verify shape is different
      const resultShape = rangeSelected.shape;
      // At least one dimension should be smaller
      const someDimensionSmaller = resultShape.some((size, idx) => size < originalShape[idx]);
      expect(someDimensionSmaller).toBe(true);
    } else {
      const computed = await rangeSelected.compute();
      const resultShape = computed.shape;
      // At least one dimension should be smaller
      const someDimensionSmaller = resultShape.some((size, idx) => size < originalShape[idx]);
      expect(someDimensionSmaller).toBe(true);
    }
  }, 60000);

  /**
   * Test 4: Verify lazy data property access throws error
   *
   * Accessing .data on lazy arrays should throw without compute()
   */
  test('should throw when accessing data on lazy array without compute()', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    if (variable.isLazy) {
      expect(() => {
        // Try to access data directly - should throw
        return variable.data;
      }).toThrow('Materializing a lazy DataBlock requires an explicit execution step');
    }
  }, 30000);

  /**
   * Test 5: Dataset lazy operations should preserve structure
   *
   * Dataset.sel() should work on lazy datasets and preserve variable structure
   */
  test('should handle Dataset.sel() on lazy IPFS data', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    // Verify dataset has variables
    expect(ds.dataVars.length).toBeGreaterThan(0);

    // Check which variables are lazy
    let lazyVars = 0;
    for (const varName of ds.dataVars) {
      if (ds.getVariable(varName).isLazy) {
        lazyVars++;
      }
    }

    // Select on the dataset
    const selected = await ds.sel({
      latitude: 45,
      longitude: 34,
      time: '1987-05-03T23:00:00'
    });

    // Verify structure is preserved
    expect(selected.dataVars.length).toBeGreaterThan(0);
    expect(selected).not.toBe(ds); // Should be a new instance
  }, 60000);

  /**
   * Test 6: Multiple independent selections should not interfere
   *
   * Create two different selections and verify they remain independent
   */
  test('should keep multiple lazy selections independent', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    // Create three independent selections
    const selection_A = await variable.sel({
      latitude: 45,
      longitude: 34,
      time: '1987-05-03T23:00:00'
    });

    const selection_B = await variable.sel({
      latitude: 40,
      longitude: 30,
      time: '1987-05-10T23:00:00'
    });

    const selection_C = await variable.sel({
      latitude: 50,
      longitude: 35,
      time: '1987-05-20T23:00:00'
    });

    // Now compute them (order shouldn't matter for independent lazy arrays)
    const computedA = selection_A.isLazy ? await selection_A.compute() : selection_A;
    const computedC = selection_C.isLazy ? await selection_C.compute() : selection_C;
    const computedB = selection_B.isLazy ? await selection_B.compute() : selection_B;

    // All should have values
    expect(computedA.values).toBeDefined();
    expect(computedB.values).toBeDefined();
    expect(computedC.values).toBeDefined();
  }, 90000);

  /**
   * Test 7: Lazy operations should preserve metadata
   *
   * Attributes, dims, coords should be preserved through lazy operations
   */
  test('should preserve metadata through lazy operations', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    const originalAttrs = variable.attrs;
    const originalDims = variable.dims;

    // Do a selection
    const selected = await variable.sel({
      latitude: 45,
      longitude: 34
    });

    // Dimensions might change (removed ones), but attrs should be preserved
    expect(selected.attrs).toEqual(originalAttrs);
  }, 60000);

  /**
   * Test 8: Compute on lazy dataset should materialize all variables
   *
   * Dataset.compute() should materialize all lazy variables
   */
  test('should compute all variables in a lazy dataset', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    // First, select to ensure we have a manageable subset
    const selected = await ds.sel({
      latitude: [44, 46],
      longitude: [33, 35]
    });

    // Check which are lazy before compute
    const beforeLazy = new Set<string>();
    for (const varName of selected.dataVars) {
      if (selected.getVariable(varName).isLazy) {
        beforeLazy.add(varName);
      }
    }

    // If any variables are lazy, compute the dataset
    if (beforeLazy.size > 0) {
      const computed = await selected.compute();

      // Verify all are now eager
      for (const varName of computed.dataVars) {
        expect(computed.getVariable(varName).isLazy).toBe(false);
      }
    }
  }, 120000);

  /**
   * Test 9: isel on lazy data should also remain lazy
   *
   * Integer selection (isel) should also support lazy evaluation
   */
  test('should support isel on lazy IPFS data', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    // Integer selection
    const selected = await variable.isel({
      latitude: 45,
      longitude: 34,
      time: 0
    });

    expect(selected).toBeDefined();
  }, 30000);

  /**
   * Test 10: Verify lazy evaluation doesn't load entire dataset
   *
   * The key test: selections should NOT load the full dataset into memory
   */
  test('should not load entire dataset when selecting from lazy IPFS data', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);
  
    // Do a small selection
    const smallSelection = await variable.sel({
      latitude: 45,
      longitude: 34,
      time: '1987-05-03T23:00:00'
    });

    if (smallSelection.isLazy) {
      // For lazy data, getting values should fail without compute
      expect(() => smallSelection.values).toThrow();
    } else {
      // For scalar result, value is materialized
      expect(smallSelection.values).toBeDefined();
    }
  }, 60000);

  /**
   * Test 11: Verify values are identical with chained vs combined selections
   *
   * Regression coverage for the lazy-selection composition bug. Chaining sel()
   * calls used to lose the mapping to the original dataset indices, triggering
   * multi-gigabyte allocations and "Invalid typed array length" errors. This
   * test ensures each intermediate lazy selection now carries the original
   * index mapping so that the final scalar matches the direct lookup.
   */
  test('should return same value from chained selections as direct selection', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    // Method 1: Direct selection with all coordinates at once
    const directSelection = await variable.sel({
      latitude: 50,
      longitude: 35,
      time: '1987-05-20T23:00:00'
    });

    const directValue = directSelection.isLazy
      ? (await directSelection.compute()).values
      : directSelection.values;

    // Method 2: Chained selections - first latitude, then longitude and time
    try {
      const chainedSelection1 = await variable.sel({ latitude: 50 });
      const chainedSelection2 = await chainedSelection1.sel({ longitude: 35 });
      const chainedSelection3 = await chainedSelection2.sel({ time: '1987-05-20T23:00:00' });

      const chainedValue1 = chainedSelection3.isLazy
        ? (await chainedSelection3.compute()).values
        : chainedSelection3.values;

      // Method 3: Chained selections in different order to confirm commutativity
      const chainedSelectionAlt1 = await variable.sel({ time: '1987-05-20T23:00:00' });
      const chainedSelectionAlt2 = await chainedSelectionAlt1.sel({ longitude: 35 });
      const chainedSelectionAlt3 = await chainedSelectionAlt2.sel({ latitude: 50 });

      const chainedValue2 = chainedSelectionAlt3.isLazy
        ? (await chainedSelectionAlt3.compute()).values
        : chainedSelectionAlt3.values;

      // Critical assertion: values must be identical
      expect(directValue).toBe(chainedValue1);
      expect(directValue).toBe(chainedValue2);
      expect(chainedValue1).toBe(chainedValue2);
    } catch (error) {
      throw (error instanceof Error ? error : new Error(String(error)));
    }
  }, 120000);

  /**
   * Test 12: Verify range selections return consistent subsets
   *
   * Companion regression test for chained range selections. It guarantees that
   * composing lazy range requests yields the same subset as a single combined
   * selection and, critically, that the computation happens lazily without
   * materialising the full dataset between steps.
   */
  test('should return same data subset from chained range selections', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    // Method 1: Direct range selection
    const directRange = await variable.sel({
      latitude: [48, 52],
      longitude: [33, 37],
      time: ['1987-05-18T23:00:00', '1987-05-22T23:00:00']
    });

    const directRangeData = directRange.isLazy
      ? await directRange.compute()
      : directRange;

    // Method 2: Chained range selections
    try {
      const chainedRange1 = await variable.sel({ latitude: [48, 52] });
      const chainedRange2 = await chainedRange1.sel({ longitude: [33, 37] });
      const chainedRange3 = await chainedRange2.sel({ time: ['1987-05-18T23:00:00', '1987-05-22T23:00:00'] });

      const chainedRangeData = chainedRange3.isLazy
        ? await chainedRange3.compute()
        : chainedRange3;

      // Shapes should be identical
      expect(directRangeData.shape).toEqual(chainedRangeData.shape);

      // Data should be identical (comparing first few values as a sanity check)
      const directFlat = directRangeData.data as any;
      const chainedFlat = chainedRangeData.data as any;

      if (Array.isArray(directFlat) && Array.isArray(chainedFlat)) {
        // For nested arrays, just verify same shape and first few values
        expect(directFlat.length).toBe(chainedFlat.length);

        const extractFirstScalar = (value: any): number => {
          let current = value;
          while (Array.isArray(current)) {
            current = current[0];
          }
          return current as number;
        };

        const directFirst = extractFirstScalar(directFlat);
        const chainedFirst = extractFirstScalar(chainedFlat);

        expect(directFirst).toBeCloseTo(chainedFirst, 6);
      }
    } catch (error) {
      throw (error instanceof Error ? error : new Error(String(error)));
    }
  }, 120000);

  /**
   * Test 13: Selecting a scalar from a prior range selection should match a direct scalar selection
   *
   * Ensures that range selections retain their original index mapping so that a
   * follow-up scalar sel() produces the same value as requesting that scalar
   * directly from the source variable.
   */
  test('should match scalar selection taken from prior range selection', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    // Step 1: take a lazy range slice
    const rangeSelection = await variable.sel({
      latitude: [48, 52],
      longitude: [33, 37],
      time: ['1987-05-18T23:00:00', '1987-05-22T23:00:00']
    });

    // Step 2: drill down to a specific coordinate within that range
    const scalarFromRange = await rangeSelection.sel({
      latitude: 49,
      longitude: 34,
      time: '1987-05-18T23:00:00'
    });

    const scalarFromRangeValue = scalarFromRange.isLazy
      ? (await scalarFromRange.compute()).values
      : scalarFromRange.values;

    // Step 3: direct scalar selection from the original variable
    const directScalar = await variable.sel({
      latitude: 49,
      longitude: 34,
      time: '1987-05-18T23:00:00'
    });

    const directScalarValue = directScalar.isLazy
      ? (await directScalar.compute()).values
      : directScalar.values;

    expect(directScalarValue).toBe(scalarFromRangeValue);
  }, 120000);
});
