/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable class-methods-use-this */
import type { AbsolutePath, RangeQuery } from "zarrita";
import { AsyncReadable } from "zarrita";
// eslint-disable-next-line import/no-extraneous-dependencies
import * as dagCbor from "@ipld/dag-cbor";
import { concat as uint8ArrayConcat } from "uint8arrays/concat";
import all from "it-all";
import { CID } from "multiformats/cid";
import { IPFSELEMENTS_INTERFACE } from "./ipfs-elements";

// #region Utility Types and Interfaces
/**
 * Type alias for N-dimensional chunk coordinates.
 */
type ChunkCoords = readonly number[];

/**
 * Type definition for the root object (manifest) of the sharded store.
 */
type ShardedRoot = {
    manifest_version: "sharded_zarr_v1";
    /** A map of metadata keys (e.g., '.zattrs', 'zarr.json') to their CIDs. */
    metadata: Record<string, string>;
    /** Information about the sharded chunk index. */
    chunks: {
        array_shape: number[];
        chunk_shape: number[];
        sharding_config: {
            chunks_per_shard: number;
        };
        /** A list of CIDs, where each CID points to a DAG-CBOR shard of the chunk index. */
        shard_cids: (string | null)[];
    };
};

// #endregion

/**
 * A read-only Zarr Store implementation that uses a sharded layout for chunk indices.
 *
 * This store reads a Zarr array where the chunk index is split into multiple "shards".
 * Each shard is a DAG-CBOR encoded list containing CIDs for a subset of chunks, or null
 * for empty chunks. This aligns with modern IPLD data structures.
 */
export class ShardedStore implements AsyncReadable {
    public readonly readOnly = true;

    public ipfsElements: IPFSELEMENTS_INTERFACE;

    private rootCid: string;

    private rootObj?: ShardedRoot;

    private shardDataCache = new Map<number, (string | null)[]>();

    private pendingShardLoads = new Map<number, Promise<void>>();

    private metadataCache = new Map<string, Uint8Array>();

    // Properties derived from the root object
    private arrayShape?: readonly number[];

    private chunkShape?: readonly number[];

    private chunksPerDim?: readonly number[];

    private chunksPerShard?: number;

    private numShards?: number;

    private totalChunks?: number;

    /**
     * Private constructor. Use the static `open` method to create an instance.
     */
    private constructor(rootCid: string, ipfsElements: IPFSELEMENTS_INTERFACE) {
        this.ipfsElements = ipfsElements;
        this.rootCid = rootCid;
    }

    /**
     * Asynchronously opens an existing read-only ShardedStore.
     */
    public static async open(rootCid: string, ipfsElements: IPFSELEMENTS_INTERFACE): Promise<ShardedStore> {
        if (!rootCid) {
            throw new Error("A rootCid must be provided to open a read-only store.");
        }
        const store = new ShardedStore(rootCid, ipfsElements);
        await store.loadRootFromCid();
        return store;
    }

    private async loadRootFromCid() {
        // The root object itself is a DAG-CBOR block.
        // if type string, parse to CID
        let rootCid: CID;
        if (typeof this.rootCid === "string") {
            rootCid = CID.parse(this.rootCid);
        } else {
            rootCid = this.rootCid;
        }
        const rootBytes = await this.ipfsElements.dagCbor.components.blockstore.get(rootCid);
        const rootObj = dagCbor.decode<ShardedRoot>(rootBytes);

        if (rootObj?.manifest_version !== "sharded_zarr_v1") {
            throw new Error(`Incompatible manifest version: ${rootObj?.manifest_version}`);
        }
        this.rootObj = rootObj;

        const chunkInfo = this.rootObj.chunks;
        this.arrayShape = chunkInfo.array_shape;
        this.chunkShape = chunkInfo.chunk_shape;
        this.chunksPerShard = chunkInfo.sharding_config.chunks_per_shard;

        this.chunksPerDim = this.arrayShape.map((dim, i) => Math.ceil(dim / this.chunkShape![i]));
        this.totalChunks = this.chunksPerDim.reduce((prod, dim) => prod * dim, 1);
        this.numShards = this.totalChunks > 0 ? Math.ceil(this.totalChunks / this.chunksPerShard) : 0;

        if (chunkInfo.shard_cids.length !== this.numShards) {
            throw new Error("Inconsistent number of shards in root object.");
        }
    }

    /** Parses a Zarr key to determine if it's a chunk key and returns its coordinates. */
    private parseChunkKey(key: string): ChunkCoords | null {
        if (key.endsWith(".json") || key.endsWith(".zattrs") || key.endsWith(".zgroup")) {
            return null;
        }

        const chunkMarker = "/c/";
        const markerIdx = key.lastIndexOf(chunkMarker);
        if (markerIdx === -1) return null;

        const coordPart = key.substring(markerIdx + chunkMarker.length);
        const parts = coordPart.split("/");

        if (parts.length !== this.chunksPerDim?.length) {
            return null; // Dimensionality mismatch
        }

        try {
            const coords = parts.map(Number);
            for (let i = 0; i < coords.length; i++) {
                if (Number.isNaN(coords[i]) || coords[i] < 0 || coords[i] >= this.chunksPerDim![i]) {
                    return null;
                }
            }
            return coords;
        } catch {
            return null;
        }
    }

    /** Converts N-D chunk coordinates to a 1-D linear index. */
    private getLinearChunkIndex(chunkCoords: ChunkCoords): number {
        let linearIndex = 0;
        let multiplier = 1;
        for (let i = this.chunksPerDim!.length - 1; i >= 0; i--) {
            linearIndex += chunkCoords[i] * multiplier;
            multiplier *= this.chunksPerDim![i];
        }
        return linearIndex;
    }

    /** Calculates the shard index and the index within the shard for a chunk. */
    private getShardInfo(linearChunkIndex: number): [number, number] {
        if (!this.chunksPerShard || this.chunksPerShard <= 0) {
            throw new Error("Sharding not configured properly.");
        }
        const shardIdx = Math.floor(linearChunkIndex / this.chunksPerShard);
        const indexInShard = linearChunkIndex % this.chunksPerShard;
        return [shardIdx, indexInShard];
    }

    /**
     * Retrieves a value from the store. Handles both metadata and chunk data.
     */
    async get(key: string): Promise<Uint8Array | undefined> {
        if (!this.rootObj) throw new Error("Root object not loaded.");
        // eslint-disable-next-line no-param-reassign
        key = key.startsWith("/") ? key.substring(1) : key;
        const chunkCoords = this.parseChunkKey(key);

        // Handle metadata request
        if (chunkCoords === null) {
            // This is actually already parsed CID
            const metadataCid = this.rootObj.metadata[key];
            if (!metadataCid) {
                return undefined; // Metadata key not found
            }
            if (this.metadataCache.has(metadataCid)) {
                return this.metadataCache.get(metadataCid);
            }
            const stream = this.ipfsElements.unixfs.cat(metadataCid);
            const cidBytes = uint8ArrayConcat(await all(stream));
            this.metadataCache.set(metadataCid, cidBytes);
            return cidBytes;
        }

        // Handle chunk data request
        const linearIdx = this.getLinearChunkIndex(chunkCoords);
        const [shardIdx, indexInShard] = this.getShardInfo(linearIdx);

        if (shardIdx >= (this.numShards ?? 0)) return undefined;

        const decodedShard = await this.getOrLoadDecodedShard(shardIdx);
        if (!decodedShard) {
            // Shard couldn't be loaded or doesn't exist.
            return undefined;
        }

        const chunkCid = decodedShard[indexInShard];
        if (!chunkCid) {
            // Chunk is explicitly empty/non-existent.
            return undefined;
        }

        // Fetch the actual chunk data using the retrieved CID.
        const stream = this.ipfsElements.unixfs.cat(chunkCid);
        const contentBlocks = await all(stream as AsyncIterable<Uint8Array>);
        return uint8ArrayConcat(contentBlocks);
    }

    /** Checks if a key exists in the store. */
    async has(key: AbsolutePath): Promise<boolean> {
        if (!this.rootObj) throw new Error("Root object not loaded.");
        const chunkCoords = this.parseChunkKey(key);

        // Handle metadata
        if (chunkCoords === null) {
            return key in this.rootObj.metadata;
        }

        // Handle chunks
        try {
            const linearIdx = this.getLinearChunkIndex(chunkCoords);
            const [shardIdx, indexInShard] = this.getShardInfo(linearIdx);

            if (shardIdx >= (this.numShards ?? 0)) return false;

            const decodedShard = await this.getOrLoadDecodedShard(shardIdx);
            if (!decodedShard) return false; // Shard not loaded or doesn't exist

            // An entry exists if the pointer at its index is not null.
            return decodedShard[indexInShard] !== null;
        } catch {
            return false;
        }
    }

    /**
     * Encapsulates the logic to get a decoded shard from cache or load it.
     */
    private async getOrLoadDecodedShard(shardIdx: number): Promise<(string | null)[] | undefined> {
        if (this.shardDataCache.has(shardIdx)) {
            return this.shardDataCache.get(shardIdx)!;
        }

        if (this.pendingShardLoads.has(shardIdx)) {
            await this.pendingShardLoads.get(shardIdx)!;
            return this.shardDataCache.get(shardIdx); // May be undefined if load failed
        }
        if (!this.rootObj) throw new Error("Root object not loaded.");
        const shardCid = this.rootObj.chunks.shard_cids[shardIdx];

        if (!shardCid) {
            // If the root manifest lists a null shard, treat it as a list of nulls.
            if (!this.chunksPerShard) throw new Error("Sharding not configured.");
            const emptyShard = new Array(this.chunksPerShard).fill(null);
            this.shardDataCache.set(shardIdx, emptyShard);
            return emptyShard;
        }
        // convert shardCid to string
        const shardCidStr = String(shardCid);
        try {
            await this.loadAndCacheShard(shardIdx, shardCidStr);
            return this.shardDataCache.get(shardIdx);
        } catch (err) {
            console.error(`Failed to load shard ${shardIdx} (CID: ${shardCidStr}).`, err);
            return undefined; // Indicate failure
        }
    }

    /**
     * Loads, decodes, and caches a single shard.
     */
    private loadAndCacheShard(shardIdx: number, shardCid: string): Promise<void> {
        if (this.pendingShardLoads.has(shardIdx)) {
            return this.pendingShardLoads.get(shardIdx)!;
        }
        const loadPromise = (async () => {
            try {
                const shardCidObj = CID.parse(shardCid);
                const shardBlockBytes = await this.ipfsElements.dagCbor.components.blockstore.get(shardCidObj);
                // Decode it into a list of CIDs/nulls.
                const decodedShard = dagCbor.decode<(string | null)[]>(shardBlockBytes);

                if (!Array.isArray(decodedShard)) {
                    throw new TypeError(`Shard ${shardIdx} (CID: ${shardCid}) did not decode to an array.`);
                }
                this.shardDataCache.set(shardIdx, decodedShard);
            } catch (err) {
                console.error(`Failed to load and decode shard ${shardIdx} (CID: ${shardCid}):`, err);
                throw err; // Re-throw to propagate failure to the caller.
            } finally {
                this.pendingShardLoads.delete(shardIdx);
            }
        })();

        this.pendingShardLoads.set(shardIdx, loadPromise);
        return loadPromise;
    }

    /**
     * List all metadata keys available in this store
     * Required by ZarrBackend for discovery
     */
    listMetadataKeys(): string[] {
        if (!this.rootObj) {
            throw new Error("Root object not loaded.");
        }
        // Return all metadata keys from the root manifest
        return Object.keys(this.rootObj.metadata);
    }

    // #region Unsupported Write Methods
    set(_key: AbsolutePath, _value: Uint8Array): Promise<void> {
        throw new Error("Store is read-only.");
    }

    delete(_key: AbsolutePath): Promise<void> {
        throw new Error("Store is read-only.");
    }
    // #endregion

    // Let zarrita handle the fallback for range requests.
    getRange?(_key: AbsolutePath, _range: RangeQuery): Promise<Uint8Array | undefined> {
        throw new Error("Range requests are not supported in this read-only store.");
    }
}