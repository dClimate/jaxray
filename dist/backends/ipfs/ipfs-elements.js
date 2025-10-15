// ipfs-elements.ts
import { KuboCAS } from './ipfs-gateway';
export function createIpfsElements(gatewayUrl = 'http://127.0.0.1:8080') {
    const gateway = new KuboCAS({
        gatewayBaseUrl: gatewayUrl,
        rpcBaseUrl: null, // Disable RPC (save operations won't work, but that's fine for read-only)
    });
    const dagCborBlockstore = {
        async get(cid) {
            console.log('Fetching DAG-CBOR block for CID:', cid);
            return await gateway.load(cid);
        },
    };
    const unixfs = {
        cat: async function* (cid) {
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
//# sourceMappingURL=ipfs-elements.js.map