// ipfs-elements.ts
import { KuboCAS } from './ipfs-gateway';
import { CID } from "multiformats/cid";

export type DagCborBlockstore = {
  get: (cid: string) => Promise<Uint8Array>;
};

export type UnixfsCat = {
  cat: (cid: string) => AsyncIterable<Uint8Array>;
};

export function createIpfsElements(gatewayUrl = 'http://127.0.0.1:8080') {
  const gateway = new KuboCAS({
    gatewayBaseUrl: gatewayUrl,
    rpcBaseUrl: null, // Disable RPC (save operations won't work, but that's fine for read-only)
  });

  const dagCborBlockstore: DagCborBlockstore = {
    async get(cid: string | CID) {
      return await gateway.load(cid);
    },
  };

  const unixfs: UnixfsCat = {
    cat: async function* (cid: string | CID): AsyncIterable<Uint8Array> {
      const data = await gateway.load(cid);
      yield data;
    },
  };

  return {
    dagCbor: { components: { blockstore: dagCborBlockstore } },
    unixfs,
  };
}
