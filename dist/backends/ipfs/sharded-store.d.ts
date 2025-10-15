import type { AbsolutePath, RangeQuery } from "zarrita";
import { AsyncReadable } from "zarrita";
import { CID } from "multiformats/cid";
export interface IPFSELEMENTS_INTERFACE {
    dagCbor: {
        components: {
            blockstore: {
                get: (cid: string | CID) => Promise<Uint8Array>;
            };
        };
    };
    unixfs: {
        cat: (cid: string | CID) => AsyncIterable<Uint8Array>;
    };
}
/**
 * A read-only Zarr Store implementation that uses a sharded layout for chunk indices.
 *
 * This store reads a Zarr array where the chunk index is split into multiple "shards".
 * Each shard is a DAG-CBOR encoded list containing CIDs for a subset of chunks, or null
 * for empty chunks. This aligns with modern IPLD data structures.
 */
export declare class ShardedStore implements AsyncReadable {
    readonly readOnly = true;
    ipfsElements: IPFSELEMENTS_INTERFACE;
    private rootCid;
    private rootObj?;
    private shardDataCache;
    private pendingShardLoads;
    private metadataCache;
    private arrayShape?;
    private chunkShape?;
    private chunksPerDim?;
    private chunksPerShard?;
    private numShards?;
    private totalChunks?;
    /**
     * Private constructor. Use the static `open` method to create an instance.
     */
    private constructor();
    /**
     * Asynchronously opens an existing read-only ShardedStore.
     */
    static open(rootCid: string, ipfsElements: IPFSELEMENTS_INTERFACE): Promise<ShardedStore>;
    private loadRootFromCid;
    /** Parses a Zarr key to determine if it's a chunk key and returns its coordinates. */
    private parseChunkKey;
    /** Converts N-D chunk coordinates to a 1-D linear index. */
    private getLinearChunkIndex;
    /** Calculates the shard index and the index within the shard for a chunk. */
    private getShardInfo;
    /**
     * Retrieves a value from the store. Handles both metadata and chunk data.
     */
    get(key: string): Promise<Uint8Array | undefined>;
    /** Checks if a key exists in the store. */
    has(key: AbsolutePath): Promise<boolean>;
    /**
     * Encapsulates the logic to get a decoded shard from cache or load it.
     */
    private getOrLoadDecodedShard;
    /**
     * Loads, decodes, and caches a single shard.
     */
    private loadAndCacheShard;
    /**
     * List all metadata keys available in this store
     * Required by ZarrBackend for discovery
     */
    listMetadataKeys(): string[];
    set(_key: AbsolutePath, _value: Uint8Array): Promise<void>;
    delete(_key: AbsolutePath): Promise<void>;
    getRange?(_key: AbsolutePath, _range: RangeQuery): Promise<Uint8Array | undefined>;
}
//# sourceMappingURL=sharded-store.d.ts.map