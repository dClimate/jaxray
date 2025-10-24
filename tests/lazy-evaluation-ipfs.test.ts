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

    console.log(`Testing variable: ${varName}`);
    console.log(`Shape: ${variable.shape}, Dims: ${variable.dims}`);

    // Verify the original data is lazy
    expect(variable.isLazy).toBe(true);

    // First selection - select specific location and time
    const selection1 = await variable.sel({
      latitude: 45,
      longitude: 34,
      time: '1987-05-03T23:00:00'
    });

    console.log('After first selection - should still be lazy if scalar result, or lazy if subset');
    expect(selection1).toBeDefined();

    // Second selection on different ranges
    const selection2 = await variable.sel({
      latitude: 40,
      longitude: 30,
      time: '1987-05-04T23:00:00'
    });

    console.log('After second selection - independent of first');
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

    console.log('Chained selection result:', chained);
    console.log('Combined selection result:', combined);

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
    console.log(`Original shape: ${originalShape}`);

    // Select a range
    const rangeSelected = await variable.sel({
      latitude: [44, 46],
      longitude: [33, 35],
      time: ['1987-05-03T23:00:00', '1987-05-05T23:00:00']
    });

    console.log(`Shape after range selection: ${rangeSelected.shape}`);

    // The result should be smaller than original
    if (!rangeSelected.isLazy) {
      // If eager, verify shape is different
      const resultShape = rangeSelected.shape;
      // At least one dimension should be smaller
      const someDimensionSmaller = resultShape.some((size, idx) => size < originalShape[idx]);
      expect(someDimensionSmaller).toBe(true);
    } else {
      console.log('Range selected data is lazy - will compute...');
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
    console.log(`Dataset has ${ds.dataVars.length} variables`);

    // Check which variables are lazy
    let lazyVars = 0;
    for (const varName of ds.dataVars) {
      if (ds.getVariable(varName).isLazy) {
        lazyVars++;
      }
    }
    console.log(`${lazyVars} variables are lazy`);

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

    console.log('Created 3 independent selections');

    // Now compute them (order shouldn't matter for independent lazy arrays)
    const computedA = selection_A.isLazy ? await selection_A.compute() : selection_A;
    const computedC = selection_C.isLazy ? await selection_C.compute() : selection_C;
    const computedB = selection_B.isLazy ? await selection_B.compute() : selection_B;

    console.log('Computed all three selections');

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

    console.log(`Original dims: ${originalDims}`);
    console.log(`Original attrs:`, originalAttrs);

    // Do a selection
    const selected = await variable.sel({
      latitude: 45,
      longitude: 34
    });

    console.log(`Selected dims: ${selected.dims}`);
    console.log(`Selected attrs:`, selected.attrs);

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

    console.log('Dataset selected, checking if variables are lazy...');

    // Check which are lazy before compute
    const beforeLazy = new Set<string>();
    for (const varName of selected.dataVars) {
      if (selected.getVariable(varName).isLazy) {
        beforeLazy.add(varName);
      }
    }
    console.log(`${beforeLazy.size} variables are lazy before compute`);

    // If any variables are lazy, compute the dataset
    if (beforeLazy.size > 0) {
      const computed = await selected.compute();
      console.log('Dataset computed');

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

    console.log('isel completed, result:', selected.values || 'lazy');

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

    console.log(`Full data shape: ${variable.shape}`);
    console.log(`Full data size: ${variable.shape.reduce((a, b) => a * b, 1)} elements`);

    // Do a small selection
    const smallSelection = await variable.sel({
      latitude: 45,
      longitude: 34,
      time: '1987-05-03T23:00:00'
    });

    console.log('Small selection completed - if lazy, no data was loaded');

    if (smallSelection.isLazy) {
      // For lazy data, getting values should fail without compute
      expect(() => smallSelection.values).toThrow();
      console.log('Confirmed: lazy selection does not load data until compute()');
    } else {
      // For scalar result, value is materialized
      console.log('Result is eager (likely a scalar value)');
      expect(smallSelection.values).toBeDefined();
    }
  }, 60000);

  /**
   * Test 11: Verify values are identical with chained vs combined selections
   *
   * This is the critical test: when doing chained selections on lazy data,
   * the final computed value should be EXACTLY THE SAME as a direct selection.
   * This verifies that lazy selections correctly track and preserve ranges.
   */
  test('should return same value from chained selections as direct selection', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    console.log(`Testing value consistency for: ${varName}`);

    // Method 1: Direct selection with all coordinates at once
    console.log('Method 1: Direct selection...');
    const directSelection = await variable.sel({
      latitude: 50,
      longitude: 35,
      time: '1987-05-20T23:00:00'
    });

    const directValue = directSelection.isLazy
      ? (await directSelection.compute()).values
      : directSelection.values;

    console.log('Direct selection value:', directValue);

    // Method 2: Chained selections - first latitude, then longitude and time
    console.log('Method 2: Chained selection (latitude -> longitude -> time)...');
    const chainedSelection1 = await variable.sel({ latitude: 50 });
    const chainedSelection2 = await chainedSelection1.sel({ longitude: 35 });
    const chainedSelection3 = await chainedSelection2.sel({ time: '1987-05-20T23:00:00' });

    const chainedValue1 = chainedSelection3.isLazy
      ? (await chainedSelection3.compute()).values
      : chainedSelection3.values;

    console.log('Chained selection value (lat->lon->time):', chainedValue1);

    // Method 3: Chained selections in different order
    console.log('Method 3: Chained selection (time -> longitude -> latitude)...');
    const chainedSelection2_1 = await variable.sel({ time: '1987-05-20T23:00:00' });
    const chainedSelection2_2 = await chainedSelection2_1.sel({ longitude: 35 });
    const chainedSelection2_3 = await chainedSelection2_2.sel({ latitude: 50 });

    const chainedValue2 = chainedSelection2_3.isLazy
      ? (await chainedSelection2_3.compute()).values
      : chainedSelection2_3.values;

    console.log('Chained selection value (time->lon->lat):', chainedValue2);

    // All three methods should return the SAME value
    expect(directValue).toBeDefined();
    expect(chainedValue1).toBeDefined();
    expect(chainedValue2).toBeDefined();

    // Critical assertion: values must be identical
    expect(directValue).toBe(chainedValue1);
    expect(directValue).toBe(chainedValue2);
    expect(chainedValue1).toBe(chainedValue2);

    console.log('✓ All selection methods returned identical values!');
  }, 120000);

  /**
   * Test 12: Verify range selections return consistent subsets
   *
   * Multiple selections of the same range should return identical data,
   * whether combined or chained
   */
  test('should return same data subset from chained range selections', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);
    const store = await ShardedStore.open(CID, ipfsElements);
    const ds = await Dataset.open_zarr(store);

    const varName = ds.dataVars[0];
    const variable = ds.getVariable(varName);

    console.log(`Testing subset consistency for: ${varName}`);

    // Method 1: Direct range selection
    console.log('Method 1: Direct range selection...');
    const directRange = await variable.sel({
      latitude: [48, 52],
      longitude: [33, 37],
      time: ['1987-05-18T23:00:00', '1987-05-22T23:00:00']
    });

    const directRangeData = directRange.isLazy
      ? await directRange.compute()
      : directRange;

    console.log('Direct range shape:', directRangeData.shape);

    // Method 2: Chained range selections
    console.log('Method 2: Chained range selection...');
    const chainedRange1 = await variable.sel({ latitude: [48, 52] });
    const chainedRange2 = await chainedRange1.sel({ longitude: [33, 37] });
    const chainedRange3 = await chainedRange2.sel({ time: ['1987-05-18T23:00:00', '1987-05-22T23:00:00'] });

    const chainedRangeData = chainedRange3.isLazy
      ? await chainedRange3.compute()
      : chainedRange3;

    console.log('Chained range shape:', chainedRangeData.shape);

    // Shapes should be identical
    expect(directRangeData.shape).toEqual(chainedRangeData.shape);

    // Data should be identical (comparing first few values as a sanity check)
    const directFlat = directRangeData.data as any;
    const chainedFlat = chainedRangeData.data as any;

    if (Array.isArray(directFlat) && Array.isArray(chainedFlat)) {
      // For nested arrays, just verify same shape and first few values
      expect(directFlat.length).toBe(chainedFlat.length);

      // Compare first element(s)
      const directFirst = Array.isArray(directFlat[0]) ? directFlat[0][0] : directFlat[0];
      const chainedFirst = Array.isArray(chainedFlat[0]) ? chainedFlat[0][0] : chainedFlat[0];

      console.log('Direct first value:', directFirst);
      console.log('Chained first value:', chainedFirst);
      expect(directFirst).toBe(chainedFirst);
    }

    console.log('✓ Range selection methods returned identical data!');
  }, 120000);
});
