// ipfs-elements.ts
import { KuboCAS } from './ipfs-gateway.js';
import { CID } from "multiformats/cid";

export type DagCborBlockstore = {
  get: (cid: string | CID) => Promise<Uint8Array>;
};

export type UnixfsCat = {
   cat: (cid: string | CID, options?: { offset?: number; length?: number }) => AsyncIterable<Uint8Array>;
};

export interface IPFSELEMENTS_INTERFACE {
    dagCbor: {
        components: {
            blockstore: {
                get: DagCborBlockstore['get'];
            };
        };
    };
    unixfs: {
        cat: UnixfsCat['cat'];
    };
}


export function createIpfsElements(gatewayUrl = 'http://127.0.0.1:8080') {
  const gateway = new KuboCAS({
    gatewayBaseUrl: gatewayUrl,
    rpcBaseUrl: null, // Disable RPC (save operations won't work, but that's fine for read-only)
  });

  const dagCborBlockstore: DagCborBlockstore = {
    async get(cid: string | CID) {
      console.log('Fetching DAG-CBOR block for CID:', cid);
      return await gateway.load(cid);
    },
  };

  const unixfs: UnixfsCat = {
    cat: async function* (cid: string | CID): AsyncIterable<Uint8Array> {
      console.log('Fetching UnixFS data for CID:', cid);
      const data = await gateway.load(cid);
      yield data;
    },
  };

  return {
    dagCbor: { components: { blockstore: dagCborBlockstore } },
    unixfs,
  };
}
