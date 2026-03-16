/**
 * Test dataset concatenation with real IPFS data
 *
 * This test demonstrates concatenating finalized and non-finalized weather datasets
 * from IPFS, querying across the boundary where they meet.
 */

import { describe, test, expect, beforeAll } from 'vitest';
import { Dataset } from '../src/Dataset';
import { ShardedStore } from '../src/backends/ipfs/sharded-store';
import { createIpfsElements } from '../src/backends/ipfs/ipfs-elements';
import { getTestCids } from './helpers/stac-cids';

describe('Dataset Concatenation with IPFS Data', () => {
  let FINALIZED_CID: string;
  let NON_FINALIZED_CID: string;
  const GATEWAY = 'https://ipfs-gateway.dclimate.net';

  beforeAll(async () => {
    const cids = await getTestCids();
    FINALIZED_CID = cids.finalized;
    NON_FINALIZED_CID = cids.nonFinalized;
  });

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

    // Use a contiguous 3-time-step range from each dataset's exclusive region.
    // sel() with an array creates a contiguous slice (min→max), so we must
    // query each side separately to avoid pulling in the entire overlap gap.
    const nonFinalizedStart = nonFinalizedTime[0];
    const finalizedEnd = finalizedTime[finalizedTime.length - 1];

    const finalizedOnlyTimes = finalizedTime.filter(t => t < nonFinalizedStart);
    const nonFinalizedOnlyTimes = nonFinalizedTime.filter(t => t > finalizedEnd);

    expect(finalizedOnlyTimes.length).toBeGreaterThanOrEqual(3);
    expect(nonFinalizedOnlyTimes.length).toBeGreaterThanOrEqual(3);

    // Pick 3 contiguous times from each exclusive region
    const finalizedTestTimes = finalizedOnlyTimes.slice(-3);
    const nonFinalizedTestTimes = nonFinalizedOnlyTimes.slice(0, 3);

    // Define New York area coordinates (roughly 1 degree resolution)
    const nyLatRange = [40, 41];
    const nyLonRange = [-75, -73];

    // Concatenate datasets
    const combined = finalizedDs.concat(nonFinalizedDs, { dim: 'time' });

    // Query each side of the boundary on the COMBINED dataset independently
    // to verify concat correctly routes to the right source dataset.
    const combinedFinalizedSel = await combined.sel({
      latitude: nyLatRange,
      longitude: nyLonRange,
      time: { start: finalizedTestTimes[0], stop: finalizedTestTimes[2] }
    });
    const combinedFinalizedData = await combinedFinalizedSel.compute();
    const combinedFinalizedVar = combinedFinalizedData.getVariable(combined.dataVars[0]);

    const combinedNonFinalizedSel = await combined.sel({
      latitude: nyLatRange,
      longitude: nyLonRange,
      time: { start: nonFinalizedTestTimes[0], stop: nonFinalizedTestTimes[2] }
    });
    const combinedNonFinalizedData = await combinedNonFinalizedSel.compute();
    const combinedNonFinalizedVar = combinedNonFinalizedData.getVariable(combined.dataVars[0]);

    // Query each source dataset independently for comparison
    const finalizedSelection = await finalizedDs.sel({
      latitude: nyLatRange,
      longitude: nyLonRange,
      time: { start: finalizedTestTimes[0], stop: finalizedTestTimes[2] }
    });
    const finalizedData = await finalizedSelection.compute();
    const finalizedVar = finalizedData.getVariable(finalizedDs.dataVars[0]);

    const nonFinalizedSelection = await nonFinalizedDs.sel({
      latitude: nyLatRange,
      longitude: nyLonRange,
      time: { start: nonFinalizedTestTimes[0], stop: nonFinalizedTestTimes[2] }
    });
    const nonFinalizedData = await nonFinalizedSelection.compute();
    const nonFinalizedVar = nonFinalizedData.getVariable(nonFinalizedDs.dataVars[0]);

    // Shapes should match
    expect(combinedFinalizedVar.shape).toEqual(finalizedVar.shape);
    expect(combinedNonFinalizedVar.shape).toEqual(nonFinalizedVar.shape);

    // Data from combined dataset's finalized region should match direct finalized query
    const combinedFinalizedArray = combinedFinalizedVar.data as number[][][];
    const finalizedDataArray = finalizedVar.data as number[][][];

    for (let t = 0; t < finalizedVar.shape[0]; t++) {
      for (let lat = 0; lat < finalizedVar.shape[1]; lat++) {
        for (let lon = 0; lon < finalizedVar.shape[2]; lon++) {
          expect(combinedFinalizedArray[t][lat][lon]).toBe(finalizedDataArray[t][lat][lon]);
        }
      }
    }

    // Data from combined dataset's non-finalized region should match direct non-finalized query
    const combinedNonFinalizedArray = combinedNonFinalizedVar.data as number[][][];
    const nonFinalizedDataArray = nonFinalizedVar.data as number[][][];

    for (let t = 0; t < nonFinalizedVar.shape[0]; t++) {
      for (let lat = 0; lat < nonFinalizedVar.shape[1]; lat++) {
        for (let lon = 0; lon < nonFinalizedVar.shape[2]; lon++) {
          expect(combinedNonFinalizedArray[t][lat][lon]).toBe(nonFinalizedDataArray[t][lat][lon]);
        }
      }
    }
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

    // Pick non-overlapping scalar time points from each dataset
    const nonFinalizedStart = nonFinalizedTime[0];
    const finalizedEnd = finalizedTime[finalizedTime.length - 1];

    const finalizedOnlyTimes = finalizedTime.filter(t => t < nonFinalizedStart);
    const nonFinalizedOnlyTimes = nonFinalizedTime.filter(t => t > finalizedEnd);

    const testLat = 40.5;
    const testLon = -74.0;

    const combined = finalizedDs.concat(nonFinalizedDs, { dim: 'time' });

    // Query scalar time points from finalized region via the combined dataset
    const fTime = finalizedOnlyTimes[finalizedOnlyTimes.length - 1];
    const combinedFinalizedResult = await combined.sel({
      latitude: testLat, longitude: testLon, time: fTime
    });
    const combinedFinalizedComputed = await combinedFinalizedResult.compute();
    const combinedFinalizedValue = combinedFinalizedComputed.getVariable(combined.dataVars[0]).data;

    // Same query directly on finalized dataset
    const directFinalizedResult = await finalizedDs.sel({
      latitude: testLat, longitude: testLon, time: fTime
    });
    const directFinalizedComputed = await directFinalizedResult.compute();
    const directFinalizedValue = directFinalizedComputed.getVariable(finalizedDs.dataVars[0]).data;

    expect(combinedFinalizedValue).toBe(directFinalizedValue);

    // Query scalar time point from non-finalized region via the combined dataset
    const nfTime = nonFinalizedOnlyTimes[0];
    const combinedNonFinalizedResult = await combined.sel({
      latitude: testLat, longitude: testLon, time: nfTime
    });
    const combinedNonFinalizedComputed = await combinedNonFinalizedResult.compute();
    const combinedNonFinalizedValue = combinedNonFinalizedComputed.getVariable(combined.dataVars[0]).data;

    // Same query directly on non-finalized dataset
    const directNonFinalizedResult = await nonFinalizedDs.sel({
      latitude: testLat, longitude: testLon, time: nfTime
    });
    const directNonFinalizedComputed = await directNonFinalizedResult.compute();
    const directNonFinalizedValue = directNonFinalizedComputed.getVariable(nonFinalizedDs.dataVars[0]).data;

    expect(combinedNonFinalizedValue).toBe(directNonFinalizedValue);
  }, 300000);
});
