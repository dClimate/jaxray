/**
 * Tests for Zarr with IPFS backend
 */

import { describe, test, expect } from 'vitest';
import { CID } from 'multiformats/cid';
import { Dataset } from '../src/Dataset';
import { ShardedStore } from '../src/backends/ipfs/sharded-store';
import { IPFSStore } from '../src/backends/ipfs/hamt-store';
import { createIpfsElements } from '../src/backends/ipfs/ipfs-elements';

describe('Dataset.open_zarr with IPFS', () => {
  test('should open sharded zarr from IPFS gateway', async () => {
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

  test('should open hamt-backed zarr from IPFS gateway', async () => {
    const cid = 'bafyr4ifo5pm7dfbjyrnch7hqblmtmmtdywnjkdk52kuxhdpzwvlt6pkxay';

    const ipfsElements = createIpfsElements('https://ipfs-gateway.dclimate.net');
    const rootCid = CID.parse(cid);
    const store = new IPFSStore(rootCid, ipfsElements);

    const ds = await Dataset.open_zarr(store);
    // Query some data
    console.log('forecast_reference_time coordinates:', ds.coords['forecast_reference_time']);

    const dataArray = ds.getVariable('AIFS Ensemble 2m Temperature');
    console.log('Data variable:', dataArray);
    console.log('forecast_reference_time attributes:', dataArray.attrs._coordAttrs?.forecast_reference_time);
    console.log('forecast_reference_time attributes:', dataArray.attrs._coordAttrs?.step);


    const dataSelected = await dataArray.isel({
      latitude: 45,
      longitude: 34,
      step: 0,
      forecast_reference_time: 0,
    });

    expect(dataSelected.values).toBe(251.53819274902344);

    // Test sel() with coordinate values
    const dataSelectedBySel = await dataArray.sel({
      latitude: -78.75,  // The coordinate value at index 45
      longitude: -171.5, // The coordinate value at index 34
      step: 0,           // The coordinate value at index 0
      forecast_reference_time: 0, // The coordinate value at index 0
    });

    expect(dataSelectedBySel.values).toBe(251.53819274902344);

    expect(ds).toBeInstanceOf(Dataset);
    expect(ds.dataVars.length).toBeGreaterThan(0);
    expect(ds.dims.length).toBeGreaterThan(0);

    const firstVar = ds.dataVars[0];
    if (firstVar) {
      const variable = ds.getVariable(firstVar);
      expect(variable.shape.length).toBeGreaterThan(0);
    }
  }, 30000);
});
