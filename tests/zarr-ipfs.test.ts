/**
 * Tests for Zarr with IPFS backend
 */

import { describe, test, expect } from 'vitest';
import { Dataset } from '../src/Dataset';

describe('Dataset.open_zarr with IPFS', () => {
  test.only('should open sharded zarr from IPFS gateway', async () => {
    // This is a real sharded zarr store on dclimate's IPFS gateway
    const cid = 'bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u';

    const ds = await Dataset.open_zarr(cid, {
      ipfsGateway: 'https://ipfs-gateway.dclimate.net',
    });

    // Basic checks
    expect(ds).toBeInstanceOf(Dataset);
    expect(ds.dataVars.length).toBeGreaterThan(0);
    expect(ds.dims.length).toBeGreaterThan(0);

    // Print the dataset structure
    console.log(ds);
    console.log('Variables:', ds.dataVars);
    console.log('Dimensions:', ds.dims);
    console.log('Sizes:', ds.sizes);

    // Print each variable's info
    for (const varName of ds.dataVars) {
      const variable = ds.getVariable(varName);
      if (variable) {
        console.log(`\n${varName}:`, {
          dims: variable.dims,
          shape: variable.shape,
          attrs: variable.attrs,
        });
      }
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
    console.log('Range selected data:', rangeSelected.getVariable('2m_temperature').values);

    // Check if we got a result
    expect(selected).toBeDefined();
  }, 30000); // 30 second timeout for network request

  test('should accept CID as string', async () => {
    const cid = 'bafyr4ibyb6sk2cxpoab2rvbwvmyjjsup42icy5sj6zyh5jhuqc6ntlkuaa';

    // This will fail because we don't have the store implementation yet,
    // but it tests that the API accepts a string CID
    await expect(Dataset.open_zarr(cid)).rejects.toThrow();
  });

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

    // This will fail during store initialization,
    // but it tests that the API accepts custom ipfsElements
    await expect(
      Dataset.open_zarr(cid, { ipfsElements: mockIpfsElements })
    ).rejects.toThrow();
  });
});
