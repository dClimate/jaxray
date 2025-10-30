/**
 * Test dataset concatenation with real IPFS data
 *
 * This test demonstrates concatenating finalized and non-finalized weather datasets
 * from IPFS, querying across the boundary where they meet.
 */

import { describe, test, expect } from 'vitest';
import { Dataset } from '../src/Dataset';
import { ShardedStore } from '../src/backends/ipfs/sharded-store';
import { createIpfsElements } from '../src/backends/ipfs/ipfs-elements';

describe('Dataset Concatenation with IPFS Data', () => {
  const FINALIZED_CID = 'bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u';
  const NON_FINALIZED_CID = 'bafyr4ihicmzx4uw4pefk7idba3mz5r5g27au3l7d62yj4gguxx6neaa5ti';
  const GATEWAY = 'https://ipfs-gateway.dclimate.net';

  /**
   * Test: Explore datasets to understand their structure
   */
  test('should explore finalized and non-finalized datasets', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);

    // Load both datasets
    const finalizedStore = await ShardedStore.open(FINALIZED_CID, ipfsElements);
    const finalizedDs = await Dataset.open_zarr(finalizedStore);

    const nonFinalizedStore = await ShardedStore.open(NON_FINALIZED_CID, ipfsElements);
    const nonFinalizedDs = await Dataset.open_zarr(nonFinalizedStore);

    // Verify datasets have compatible structure
    expect(finalizedDs.dataVars).toEqual(nonFinalizedDs.dataVars);
    expect(finalizedDs.dims).toEqual(nonFinalizedDs.dims);
  }, 120000);

  /**
   * Test: Concatenate datasets and query across the boundary
   *
   * This test:
   * 1. Gets the last week of finalized data
   * 2. Gets the first week of non-finalized data (starting 1 hour after finalized ends)
   * 3. Concatenates the datasets
   * 4. Queries across the boundary to verify correct data retrieval
   * 5. Tests a small geographic region (New York area, ~1 degree resolution)
   */
  test('should concatenate and query across finalized/non-finalized boundary', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);

    // Load both datasets
    const finalizedStore = await ShardedStore.open(FINALIZED_CID, ipfsElements);
    const finalizedDs = await Dataset.open_zarr(finalizedStore);

    const nonFinalizedStore = await ShardedStore.open(NON_FINALIZED_CID, ipfsElements);
    const nonFinalizedDs = await Dataset.open_zarr(nonFinalizedStore);

    // Get time coordinates
    const finalizedTime = finalizedDs.coords.time as string[];
    const nonFinalizedTime = nonFinalizedDs.coords.time as string[];

    // Get last 3 time points from finalized
    const finalizedTestTimes = finalizedTime.slice(-3);

    // Get first 3 time points from non-finalized
    const nonFinalizedTestTimes = nonFinalizedTime.slice(0, 3);

    const boundaryTimes = [...finalizedTestTimes, ...nonFinalizedTestTimes];

    // Define New York area coordinates (roughly 1 degree resolution)
    // NYC is approximately 40.7°N, -74°W
    const nyLatRange = [40, 41];
    const nyLonRange = [-75, -73];

    // Query finalized dataset independently
    const finalizedSelection = await finalizedDs.sel({
      latitude: nyLatRange,
      longitude: nyLonRange,
      time: finalizedTestTimes
    });
    const finalizedData = await finalizedSelection.compute();
    const finalizedVar = finalizedData.getVariable(finalizedDs.dataVars[0]);

    expect(finalizedVar.shape).toEqual([3, expect.any(Number), expect.any(Number)]);

    // Query non-finalized dataset independently
    const nonFinalizedSelection = await nonFinalizedDs.sel({
      latitude: nyLatRange,
      longitude: nyLonRange,
      time: nonFinalizedTestTimes
    });
    const nonFinalizedData = await nonFinalizedSelection.compute();
    const nonFinalizedVar = nonFinalizedData.getVariable(nonFinalizedDs.dataVars[0]);

    expect(nonFinalizedVar.shape).toEqual([3, expect.any(Number), expect.any(Number)]);

    // Concatenate datasets
    const combined = finalizedDs.concat(nonFinalizedDs, { dim: 'time' });

    // Query the combined dataset across the boundary
    const combinedSelection = await combined.sel({
      latitude: nyLatRange,
      longitude: nyLonRange,
      time: boundaryTimes
    });

    const combinedData = await combinedSelection.compute();
    const combinedVar = combinedData.getVariable(combined.dataVars[0]);;

    // Validate shape: should be [6 time points, same lat count, same lon count]
    expect(combinedVar.shape[0]).toBe(6); // 3 from finalized + 3 from non-finalized
    expect(combinedVar.shape[1]).toBe(finalizedVar.shape[1]); // Same latitude count
    expect(combinedVar.shape[2]).toBe(finalizedVar.shape[2]); // Same longitude count

    // Validate that first 3 time slices match finalized data
    const combinedDataArray = combinedVar.data as number[][][];
    const finalizedDataArray = finalizedVar.data as number[][][];
    const nonFinalizedDataArray = nonFinalizedVar.data as number[][][];

    // Check first 3 time slices (should match finalized)
    for (let t = 0; t < 3; t++) {
      for (let lat = 0; lat < combinedVar.shape[1]; lat++) {
        for (let lon = 0; lon < combinedVar.shape[2]; lon++) {
          const combinedValue = combinedDataArray[t][lat][lon];
          const finalizedValue = finalizedDataArray[t][lat][lon];

          expect(combinedValue).toBe(finalizedValue);
        }
      }
    }

    // Check last 3 time slices (should match non-finalized)
    for (let t = 0; t < 3; t++) {
      for (let lat = 0; lat < combinedVar.shape[1]; lat++) {
        for (let lon = 0; lon < combinedVar.shape[2]; lon++) {
          const combinedValue = combinedDataArray[t + 3][lat][lon];
          const nonFinalizedValue = nonFinalizedDataArray[t][lat][lon];

          expect(combinedValue).toBe(nonFinalizedValue);
        }
      }
    }
    // Verify coordinates are correct
    expect(combinedVar.coords.time).toEqual(boundaryTimes);
    expect(combinedVar.coords.latitude).toEqual(finalizedVar.coords.latitude);
    expect(combinedVar.coords.longitude).toEqual(finalizedVar.coords.longitude);
  }, 300000); // 5 minutes timeout for IPFS operations

  /**
   * Test: Verify concatenated data matches independent queries
   *
   * This ensures that querying the concatenated dataset produces the same
   * results as querying each dataset independently and combining them.
   */
  test('should produce same results as independent queries', async () => {
    const ipfsElements = createIpfsElements(GATEWAY);

    // Load both datasets
    const finalizedStore = await ShardedStore.open(FINALIZED_CID, ipfsElements);
    const finalizedDs = await Dataset.open_zarr(finalizedStore);

    const nonFinalizedStore = await ShardedStore.open(NON_FINALIZED_CID, ipfsElements);
    const nonFinalizedDs = await Dataset.open_zarr(nonFinalizedStore);

    // Get time coordinates
    const finalizedTime = finalizedDs.coords.time as string[];
    const nonFinalizedTime = nonFinalizedDs.coords.time as string[];

    // Select specific time points - 2 from finalized, 2 from non-finalized
    const finalizedTestTimes = [
      finalizedTime[finalizedTime.length - 2],
      finalizedTime[finalizedTime.length - 1]
    ];
    const nonFinalizedTestTimes = [
      nonFinalizedTime[0],
      nonFinalizedTime[1]
    ];
    const allTestTimes = [...finalizedTestTimes, ...nonFinalizedTestTimes];

    // NY area
    const testLat = 40.5;
    const testLon = -74.0;

    // Query finalized
    const finalizedResult = await finalizedDs.sel({
      latitude: testLat,
      longitude: testLon,
      time: finalizedTestTimes
    });
    const finalizedComputed = await finalizedResult.compute();
    const finalizedValues = finalizedComputed.getVariable(finalizedDs.dataVars[0]).data;

    // Query non-finalized
    const nonFinalizedResult = await nonFinalizedDs.sel({
      latitude: testLat,
      longitude: testLon,
      time: nonFinalizedTestTimes
    });
    const nonFinalizedComputed = await nonFinalizedResult.compute();
    const nonFinalizedValues = nonFinalizedComputed.getVariable(nonFinalizedDs.dataVars[0]).data;

    // Query concatenated
    const combined = finalizedDs.concat(nonFinalizedDs, { dim: 'time' });
    const combinedResult = await combined.sel({
      latitude: testLat,
      longitude: testLon,
      time: allTestTimes
    });
    const combinedComputed = await combinedResult.compute();
    const combinedValues = combinedComputed.getVariable(combined.dataVars[0]).data;

    // Verify concatenated results match independent queries
    if (Array.isArray(combinedValues) && Array.isArray(finalizedValues) && Array.isArray(nonFinalizedValues)) {
      // First 2 values should match finalized
      expect(combinedValues[0]).toBe(finalizedValues[0]);
      expect(combinedValues[1]).toBe(finalizedValues[1]);

      // Last 2 values should match non-finalized
      expect(combinedValues[2]).toBe(nonFinalizedValues[0]);
      expect(combinedValues[3]).toBe(nonFinalizedValues[1]);

    }
  }, 300000);
});
