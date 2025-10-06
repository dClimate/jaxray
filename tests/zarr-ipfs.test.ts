/**
 * Tests for Zarr with IPFS backend
 */

import { describe, test, expect } from 'vitest';
import { Dataset } from '../src/Dataset';
import { ShardedStore } from '../src/backends/ipfs/sharded-store';
import { createIpfsElements } from '../src/backends/ipfs/ipfs-elements';

describe('Dataset.open_zarr with IPFS', () => {
  test.only('should open sharded zarr from IPFS gateway', async () => {
    // This is a real sharded zarr store on dclimate's IPFS gateway
    const cid = 'bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u';

    // Create IPFS elements and sharded store
    const ipfsElements = createIpfsElements('https://ipfs-gateway.dclimate.net');
    const store = await ShardedStore.open(cid, ipfsElements);

    // Open as dataset
    const ds = await Dataset.open_zarr(store);

    // Basic checks
    expect(ds).toBeInstanceOf(Dataset);
    expect(ds.dataVars.length).toBeGreaterThan(0);
    expect(ds.dims.length).toBeGreaterThan(0);



    // Print each variable's info
    for (const varName of ds.dataVars) {
      const variable = ds.getVariable(varName);
      console.log(`\n${varName}:`, {
        dims: variable.dims,
        shape: variable.shape,
        attrs: variable.attrs,
      });
    }

    // Select a specific location and time
    console.log('\n--- Selection Test ---');
    console.log('Selecting: lat=45, lon=34, time=1987-01-03T23:00:00');

    const selected = await ds.sel({
      latitude: 45,
      longitude: 34,
      time: '1987-05-03T23:00:00'
    });
    console.log('Selected dataset:');
    console.log(selected);

    console.log('Selected data:', selected.getVariable('2m_temperature').values);

    const rangeSelected = await ds.sel({
      latitude: [44, 45],
      longitude: [33, 34],
      time: ['1987-05-03T23:00:00', '1987-05-04T23:00:00']
    });
    console.log('Range selected dataset:');
    console.log(rangeSelected);

    // Check if we got a result
    expect(selected).toBeDefined();
  }, 30000); // 30 second timeout for network request

  test('should accept custom ipfsElements', async () => {
    const cid = 'bafyr4ibyb6sk2cxpoab2rvbwvmyjjsup42icy5sj6zyh5jhuqc6ntlkuaa';

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
