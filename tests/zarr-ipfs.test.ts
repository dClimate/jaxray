/**
 * Tests for Zarr with IPFS backend
 */

import { describe, test, expect } from 'vitest';
import { Dataset } from '../src/Dataset';
import { ShardedStore } from '../src/backends/ipfs/sharded-store';
import { createIpfsElements } from '../src/backends/ipfs/ipfs-elements';
import { getFinalizedCid } from './helpers/stac-cids';

describe('Dataset.open_zarr with IPFS', () => {
  test('should open sharded zarr from IPFS gateway', async () => {
    // Fetch a current CID from the dClimate STAC API so tests don't break when datasets are republished
    const cid = await getFinalizedCid();

    // Create IPFS elements and sharded store
    const ipfsElements = createIpfsElements('https://ipfs-gateway.dclimate.net');
    const store = await ShardedStore.open(cid, ipfsElements);

    // Open as dataset
    const ds = await Dataset.open_zarr(store);

    // Basic checks
    expect(ds).toBeInstanceOf(Dataset);
    expect(ds.dataVars.length).toBeGreaterThan(0);
    expect(ds.dims.length).toBeGreaterThan(0);

    const selected = await ds.sel({
      latitude: 45,
      longitude: 34,
      time: '1987-05-03T23:00:00'
    });

    // Check if we got a result
    expect(selected).toBeDefined();
  }, 120000); // 2 minute timeout for STAC API + IPFS network requests

  test('should accept custom ipfsElements', async () => {
    const cid = 'bafyr4ifo5pm7dfbjyrnch7hqblmtmmtdywnjkdk52kuxhdpzwvlt6pkxay';
    const mockIpfsElements = {
      dagCbor: {
        components: {
          blockstore: {
            get: async () => new Uint8Array(),
          },
        },
      },
      unixfs: {
        cat: async function* () {
          yield new Uint8Array();
        },
      },
    };

    // Create store with custom IPFS elements
    // This will fail during store initialization because mock elements are incomplete
    await expect(
      ShardedStore.open(cid, mockIpfsElements)
    ).rejects.toThrow();
  });

});
