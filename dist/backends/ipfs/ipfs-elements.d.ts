import { CID } from "multiformats/cid";
export type DagCborBlockstore = {
    get: (cid: string | CID) => Promise<Uint8Array>;
};
export type UnixfsCat = {
    cat: (cid: string | CID) => AsyncIterable<Uint8Array>;
};
export declare function createIpfsElements(gatewayUrl?: string): {
    dagCbor: {
        components: {
            blockstore: DagCborBlockstore;
        };
    };
    unixfs: UnixfsCat;
};
//# sourceMappingURL=ipfs-elements.d.ts.map