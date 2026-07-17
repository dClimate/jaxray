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

type ChunkCoords = readonly number[];
export type CidLike = string | CID | { toString(): string };

export type ShardReadMode = "full" | "sparse";

type ShardingConfig = {
    chunks_per_shard: number;
    order?: string;
};

export type ShardedRootV1 = {
    manifest_version: "sharded_zarr_v1";
    metadata: Record<string, CidLike>;
    chunks: {
        array_shape: number[];
        chunk_shape: number[];
        sharding_config: ShardingConfig;
        shard_cids: (CidLike | null)[];
    };
};

export type ShardedArrayManifest = {
    array_shape: number[];
    chunk_shape: number[];
    sharding_config: ShardingConfig;
    shard_cids: (CidLike | null)[];
};

export type ShardedRootV2 = {
    manifest_version: "sharded_zarr_v2";
    sharding_config?: Partial<ShardingConfig>;
    metadata: Record<string, CidLike>;
    arrays: Record<string, ShardedArrayManifest>;
};

export type ShardedRoot = ShardedRootV1 | ShardedRootV2;

type ArrayIndex = {
    arrayPath: string;
    arrayShape: readonly number[];
    chunkShape: readonly number[];
    chunksPerDim: readonly number[];
    chunksPerShard: number;
    numShards: number;
    totalChunks: number;
    shardCids: readonly (CidLike | null)[];
    order: string;
};

type ParsedChunkKey = {
    arrayPath: string;
    coords: ChunkCoords;
    index: ArrayIndex;
};

const ZARR_METADATA_SUFFIXES = ["zarr.json", ".zarray", ".zattrs", ".zgroup", ".zmetadata"];

class CborWalker {
    private offset = 0;

    public constructor(private readonly bytes: Uint8Array) {}

    public get position(): number {
        return this.offset;
    }

    public readByte(): number {
        if (this.offset >= this.bytes.length) {
            throw new Error("Malformed CBOR: unexpected end of input.");
        }
        return this.bytes[this.offset++];
    }

    private readArgument(additionalInfo: number): number | bigint {
        if (additionalInfo < 24) return additionalInfo;
        const length = additionalInfo === 24 ? 1 : additionalInfo === 25 ? 2 : additionalInfo === 26 ? 4 : additionalInfo === 27 ? 8 : 0;
        if (length === 0) {
            if (additionalInfo === 31) {
                throw new Error("Unsupported indefinite-length CBOR item.");
            }
            throw new Error(`Unsupported CBOR additional information: ${additionalInfo}.`);
        }
        if (this.offset + length > this.bytes.length) {
            throw new Error("Malformed CBOR: truncated argument.");
        }
        let value = 0n;
        for (let i = 0; i < length; i++) {
            value = (value << 8n) | BigInt(this.bytes[this.offset++]);
        }
        return value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : value;
    }

    public readLength(additionalInfo: number): number {
        const value = this.readArgument(additionalInfo);
        if (typeof value !== "number" || !Number.isSafeInteger(value)) {
            throw new Error("CBOR item length exceeds the supported range.");
        }
        return value;
    }

    public skipItem(): void {
        const initial = this.readByte();
        const majorType = initial >> 5;
        const additionalInfo = initial & 0x1f;
        switch (majorType) {
            case 0:
            case 1:
                this.readArgument(additionalInfo);
                return;
            case 2:
            case 3:
                this.skipBytes(this.readLength(additionalInfo));
                return;
            case 4: {
                const length = this.readLength(additionalInfo);
                for (let i = 0; i < length; i++) this.skipItem();
                return;
            }
            case 5: {
                const length = this.readLength(additionalInfo);
                for (let i = 0; i < length * 2; i++) this.skipItem();
                return;
            }
            case 6:
                this.readArgument(additionalInfo);
                this.skipItem();
                return;
            case 7:
                this.skipSimpleOrFloat(additionalInfo);
                return;
            default:
                throw new Error(`Unsupported CBOR major type: ${majorType}.`);
        }
    }

    private skipBytes(length: number): void {
        if (this.offset + length > this.bytes.length) {
            throw new Error("Malformed CBOR: truncated byte or text string.");
        }
        this.offset += length;
    }

    private skipSimpleOrFloat(additionalInfo: number): void {
        if (additionalInfo === 24) this.skipBytes(1);
        else if (additionalInfo === 25) this.skipBytes(2);
        else if (additionalInfo === 26) this.skipBytes(4);
        else if (additionalInfo === 27) this.skipBytes(8);
        else if (additionalInfo === 31) throw new Error("Unsupported indefinite-length CBOR item.");
        else if (additionalInfo >= 24) throw new Error(`Unsupported CBOR simple value: ${additionalInfo}.`);
    }
}

/** Decode one entry from a definite-length DAG-CBOR shard list without decoding the other entries. */
export const decodeShardEntry = (shardBytes: Uint8Array, index: number): CidLike | null => {
    if (!Number.isInteger(index) || index < 0) {
        throw new RangeError(`Shard entry index must be non-negative: ${index}.`);
    }

    const walker = new CborWalker(shardBytes);
    const header = walker.readByte();
    if ((header >> 5) !== 4) {
        throw new TypeError("Shard did not decode to a CBOR list.");
    }
    const length = walker.readLength(header & 0x1f);
    if (index >= length) {
        throw new RangeError(`Shard entry index ${index} is out of range for list length ${length}.`);
    }

    for (let i = 0; i < index; i++) walker.skipItem();
    const itemStart = walker.position;
    walker.skipItem();
    const itemBytes = shardBytes.subarray(itemStart, walker.position);
    let decoded: unknown;
    try {
        decoded = dagCbor.decode(itemBytes);
    } catch {
        throw new TypeError(`Malformed shard entry at index ${index}.`);
    }
    if (decoded === null) return null;
    if (decoded instanceof CID) return decoded;
    if (typeof decoded !== "string") {
        throw new TypeError(`Shard entry at index ${index} is not a CID or null.`);
    }
    try {
        CID.parse(decoded);
    } catch {
        throw new TypeError(`Shard entry at index ${index} is not a valid CID.`);
    }
    return decoded as CidLike;
};

/**
 * A read-only Zarr Store implementation that uses a sharded layout for chunk indices.
 *
 * This store supports both the original sharded_zarr_v1 manifest and the
 * path-aware sharded_zarr_v2 manifest introduced by py-hamt 3.4.0.
 */
export class ShardedStore implements AsyncReadable {
    public readonly readOnly = true;

    /**
     * Lookups on a single shard past which a full decode + cache beats repeated
     * sparse per-slot walks. Benchmarked crossover is ~32 across 6k–32k-entry
     * shards; the promotion "tax" (walks wasted before promoting) grows with the
     * threshold squared, and typical region reads touch far fewer chunks, so 32
     * caps the hot-shard cost while leaving light reads on the fast sparse path.
     */
    private static readonly SPARSE_PROMOTE_THRESHOLD = 32;

    public ipfsElements: IPFSELEMENTS_INTERFACE;

    private rootCid: string;

    private shardReadMode: ShardReadMode;

    private rootObj?: ShardedRoot;

    private manifestVersion?: ShardedRoot["manifest_version"];

    private shardDataCache = new Map<string, (CidLike | null)[]>();

    private pendingShardLoads = new Map<string, Promise<void>>();

    // Per-shard sparse-lookup counter driving adaptive promotion to a full decode.
    private shardAccessCounts = new Map<string, number>();

    private metadataCache = new Map<string, Uint8Array>();

    private arrayIndices = new Map<string, ArrayIndex>();

    private primaryArrayPath = "";

    // Legacy geometry fields retained for older tests/consumers that introspect them.
    private arrayShape?: readonly number[];

    private chunkShape?: readonly number[];

    private chunksPerDim?: readonly number[];

    private chunksPerShard?: number;

    private numShards?: number;

    private totalChunks?: number;

    private constructor(
        rootCid: string,
        ipfsElements: IPFSELEMENTS_INTERFACE,
        shardReadMode: ShardReadMode = "sparse",
    ) {
        this.ipfsElements = ipfsElements;
        this.rootCid = rootCid;
        this.shardReadMode = shardReadMode;
    }

    public static async open(
        rootCid: string,
        ipfsElements: IPFSELEMENTS_INTERFACE,
        shardReadMode: ShardReadMode = "sparse",
    ): Promise<ShardedStore> {
        if (!rootCid) {
            throw new Error("A rootCid must be provided to open a read-only store.");
        }
        const store = new ShardedStore(rootCid, ipfsElements, shardReadMode);
        await store.loadRootFromCid();
        return store;
    }

    public static fromRootObject(
        rootCid: string,
        ipfsElements: IPFSELEMENTS_INTERFACE,
        rootObj: ShardedRoot,
        shardReadMode: ShardReadMode = "sparse",
    ): ShardedStore {
        if (!rootCid) {
            throw new Error("A rootCid must be provided to open a read-only store.");
        }
        const store = new ShardedStore(rootCid, ipfsElements, shardReadMode);
        store.initializeRootObject(rootObj);
        return store;
    }

    private static normalizeStoreKey(key: string): string {
        return key.replace(/^\/+/, "").replace(/\/+$/, "");
    }

    private static normalizeArrayPath(path: string | undefined): string {
        return (path ?? "").replace(/^\/+/, "").replace(/\/+$/, "");
    }

    private static isMetadataKey(key: string): boolean {
        return ZARR_METADATA_SUFFIXES.some((suffix) => key === suffix || key.endsWith(`/${suffix}`));
    }

    private static createArrayIndex(
        arrayPath: string,
        arrayShape: readonly number[],
        chunkShape: readonly number[],
        shardingConfig: ShardingConfig,
        shardCids: readonly (CidLike | null)[],
    ): ArrayIndex {
        if (!Number.isInteger(shardingConfig.chunks_per_shard) || shardingConfig.chunks_per_shard <= 0) {
            throw new Error("chunks_per_shard must be a positive integer.");
        }
        if (shardingConfig.order && shardingConfig.order !== "C") {
            throw new Error("Only row-major ('C') shard ordering is supported.");
        }
        if (arrayShape.length !== chunkShape.length) {
            throw new Error("array_shape and chunk_shape must have the same rank.");
        }
        if (chunkShape.some((dim) => dim <= 0)) {
            throw new Error("All chunk_shape dimensions must be positive.");
        }
        if (arrayShape.some((dim) => dim < 0)) {
            throw new Error("All array_shape dimensions must be non-negative.");
        }

        const chunksPerDim = arrayShape.map((dim, i) => Math.ceil(dim / chunkShape[i]));
        const totalChunks = chunksPerDim.reduce((prod, dim) => prod * dim, 1);
        const numShards = totalChunks > 0
            ? Math.ceil(totalChunks / shardingConfig.chunks_per_shard)
            : 0;

        if (shardCids.length !== numShards) {
            throw new Error(`Inconsistent number of shards. Expected ${numShards}, found ${shardCids.length}.`);
        }

        return {
            arrayPath: ShardedStore.normalizeArrayPath(arrayPath),
            arrayShape,
            chunkShape,
            chunksPerDim,
            chunksPerShard: shardingConfig.chunks_per_shard,
            numShards,
            totalChunks,
            shardCids,
            order: shardingConfig.order ?? "C",
        };
    }

    private async loadRootFromCid() {
        const rootCid = CID.parse(this.rootCid);
        const rootBytes = await this.ipfsElements.dagCbor.components.blockstore.get(rootCid);
        const rootObj = dagCbor.decode<ShardedRoot>(rootBytes);
        this.initializeRootObject(rootObj);
    }

    private initializeRootObject(rootObj: ShardedRoot) {
        this.rootObj = rootObj;
        this.manifestVersion = rootObj?.manifest_version;
        this.arrayIndices.clear();

        if (rootObj?.manifest_version === "sharded_zarr_v1") {
            this.initializeV1Root(rootObj);
            return;
        }

        if (rootObj?.manifest_version === "sharded_zarr_v2") {
            this.initializeV2Root(rootObj);
            return;
        }

        const manifestVersion = (rootObj as { manifest_version?: unknown } | undefined)?.manifest_version;
        throw new Error(`Incompatible manifest version: ${manifestVersion}`);
    }

    private initializeV1Root(rootObj: ShardedRootV1) {
        const chunkInfo = rootObj.chunks;
        const index = ShardedStore.createArrayIndex(
            "",
            chunkInfo.array_shape,
            chunkInfo.chunk_shape,
            chunkInfo.sharding_config,
            chunkInfo.shard_cids,
        );
        this.arrayIndices.set("", index);
        this.primaryArrayPath = "";
        this.setLegacyGeometry(index);
    }

    private initializeV2Root(rootObj: ShardedRootV2) {
        if (!rootObj.metadata || typeof rootObj.metadata !== "object" || !rootObj.arrays || typeof rootObj.arrays !== "object") {
            throw new Error("Root object is not a valid v2 dictionary with 'metadata' and 'arrays' keys.");
        }

        for (const [arrayPath, arrayManifest] of Object.entries(rootObj.arrays)) {
            const index = ShardedStore.createArrayIndex(
                arrayPath,
                arrayManifest.array_shape,
                arrayManifest.chunk_shape,
                arrayManifest.sharding_config,
                arrayManifest.shard_cids,
            );
            this.arrayIndices.set(index.arrayPath, index);
        }

        this.primaryArrayPath = this.arrayIndices.keys().next().value ?? "";
        const primaryIndex = this.arrayIndices.get(this.primaryArrayPath);
        if (primaryIndex) {
            this.setLegacyGeometry(primaryIndex);
        } else {
            this.arrayShape = undefined;
            this.chunkShape = undefined;
            this.chunksPerDim = undefined;
            this.chunksPerShard = undefined;
            this.numShards = undefined;
            this.totalChunks = undefined;
        }
    }

    private setLegacyGeometry(index: ArrayIndex) {
        this.arrayShape = index.arrayShape;
        this.chunkShape = index.chunkShape;
        this.chunksPerDim = index.chunksPerDim;
        this.chunksPerShard = index.chunksPerShard;
        this.numShards = index.numShards;
        this.totalChunks = index.totalChunks;
    }

    private parseChunkKey(key: string): ParsedChunkKey | null {
        if (ShardedStore.isMetadataKey(key)) {
            return null;
        }

        const chunkMarker = "/c/";
        const markerIdx = key.lastIndexOf(chunkMarker);
        if (markerIdx !== -1) {
            const arrayPath = ShardedStore.normalizeArrayPath(key.substring(0, markerIdx));
            const coordPart = key.substring(markerIdx + chunkMarker.length);
            return this.parseChunkCoords(arrayPath, coordPart);
        }

        if (key.startsWith("c/")) {
            return this.parseChunkCoords("", key.substring(2));
        }

        if (this.manifestVersion === "sharded_zarr_v2") {
            return this.parseClassicV2ChunkKey(key);
        }

        return null;
    }

    private parseClassicV2ChunkKey(key: string): ParsedChunkKey | null {
        const dottedChunk = this.parseClassicDottedV2ChunkKey(key);
        if (dottedChunk) {
            return dottedChunk;
        }

        const entries = [...this.arrayIndices.entries()]
            .sort(([left], [right]) => right.length - left.length);
        for (const [arrayPath, index] of entries) {
            const prefix = arrayPath ? `${arrayPath}/` : "";
            if (prefix && !key.startsWith(prefix)) {
                continue;
            }
            const coordPart = prefix ? key.substring(prefix.length) : key;
            const parts = coordPart.split("/");
            if (parts.length !== index.chunksPerDim.length || !parts.every((part) => /^\d+$/.test(part))) {
                continue;
            }
            const coords = parts.map(Number);
            if (!this.areChunkCoordsValid(coords, index)) {
                continue;
            }
            return { arrayPath, coords, index };
        }
        return null;
    }

    private parseClassicDottedV2ChunkKey(key: string): ParsedChunkKey | null {
        const slashIdx = key.lastIndexOf("/");
        const arrayPath = slashIdx === -1 ? "" : ShardedStore.normalizeArrayPath(key.substring(0, slashIdx));
        const coordPart = slashIdx === -1 ? key : key.substring(slashIdx + 1);
        if (!coordPart.includes(".")) {
            return null;
        }
        const index = this.arrayIndices.get(arrayPath);
        if (!index) {
            return null;
        }
        const parts = coordPart.split(".");
        if (parts.length !== index.chunksPerDim.length || !parts.every((part) => /^\d+$/.test(part))) {
            return null;
        }
        const coords = parts.map(Number);
        if (!this.areChunkCoordsValid(coords, index)) {
            return null;
        }
        return { arrayPath, coords, index };
    }

    private parseChunkCoords(arrayPath: string, coordPart: string): ParsedChunkKey | null {
        const index = this.manifestVersion === "sharded_zarr_v1"
            ? this.arrayIndices.get("")
            : this.arrayIndices.get(arrayPath);
        if (!index) {
            return null;
        }

        const parts = coordPart.split("/");
        if (parts.length !== index.chunksPerDim.length || !parts.every((part) => /^\d+$/.test(part))) {
            return null;
        }

        const coords = parts.map(Number);
        if (!this.areChunkCoordsValid(coords, index)) {
            return null;
        }

        return {
            arrayPath: index.arrayPath,
            coords,
            index,
        };
    }

    private areChunkCoordsValid(coords: ChunkCoords, index: ArrayIndex): boolean {
        if (coords.length !== index.chunksPerDim.length) {
            return false;
        }
        return coords.every((coord, i) => Number.isInteger(coord) && coord >= 0 && coord < index.chunksPerDim[i]);
    }

    private getLinearChunkIndex(chunkCoords: ChunkCoords, index: ArrayIndex): number {
        let linearIndex = 0;
        let multiplier = 1;
        for (let i = index.chunksPerDim.length - 1; i >= 0; i--) {
            linearIndex += chunkCoords[i] * multiplier;
            multiplier *= index.chunksPerDim[i];
        }
        return linearIndex;
    }

    private getShardInfo(linearChunkIndex: number, index: ArrayIndex): [number, number] {
        const shardIdx = Math.floor(linearChunkIndex / index.chunksPerShard);
        const indexInShard = linearChunkIndex % index.chunksPerShard;
        return [shardIdx, indexInShard];
    }

    async get(key: string): Promise<Uint8Array | undefined> {
        if (!this.rootObj) throw new Error("Root object not loaded.");
        const normalizedKey = ShardedStore.normalizeStoreKey(key);
        const parsedChunk = this.parseChunkKey(normalizedKey);

        if (parsedChunk === null) {
            const metadataCid = this.rootObj.metadata[normalizedKey];
            if (!metadataCid) {
                return undefined;
            }
            const metadataCacheKey = String(metadataCid);
            if (this.metadataCache.has(metadataCacheKey)) {
                return this.metadataCache.get(metadataCacheKey);
            }
            const stream = this.ipfsElements.unixfs.cat(metadataCacheKey);
            const cidBytes = uint8ArrayConcat(await all(stream));
            this.metadataCache.set(metadataCacheKey, cidBytes);
            return cidBytes;
        }

        const linearIdx = this.getLinearChunkIndex(parsedChunk.coords, parsedChunk.index);
        const [shardIdx, indexInShard] = this.getShardInfo(linearIdx, parsedChunk.index);

        if (shardIdx >= parsedChunk.index.numShards) return undefined;

        const chunkCid = await this.resolveShardEntry(parsedChunk.index, shardIdx, indexInShard);
        if (!chunkCid) {
            return undefined;
        }

        const stream = this.ipfsElements.unixfs.cat(String(chunkCid));
        const contentBlocks = await all(stream as AsyncIterable<Uint8Array>);
        return uint8ArrayConcat(contentBlocks);
    }

    async has(key: AbsolutePath): Promise<boolean> {
        if (!this.rootObj) throw new Error("Root object not loaded.");
        const normalizedKey = ShardedStore.normalizeStoreKey(key);
        const parsedChunk = this.parseChunkKey(normalizedKey);

        if (parsedChunk === null) {
            return normalizedKey in this.rootObj.metadata;
        }

        try {
            const linearIdx = this.getLinearChunkIndex(parsedChunk.coords, parsedChunk.index);
            const [shardIdx, indexInShard] = this.getShardInfo(linearIdx, parsedChunk.index);

            if (shardIdx >= parsedChunk.index.numShards) return false;

            const entry = await this.resolveShardEntry(parsedChunk.index, shardIdx, indexInShard);
            return entry !== null && entry !== undefined;
        } catch {
            return false;
        }
    }

    private shardCacheKey(index: ArrayIndex, shardIdx: number): string {
        return `${index.arrayPath}\0${shardIdx}`;
    }

    /**
     * Resolve a single chunk entry, choosing between a sparse per-slot walk and a
     * full decode + cache. In sparse mode a shard is promoted to a full decode once
     * it has been read more than SPARSE_PROMOTE_THRESHOLD times, after which the
     * populated shardDataCache serves all further lookups in O(1).
     */
    private async resolveShardEntry(
        index: ArrayIndex,
        shardIdx: number,
        indexInShard: number,
    ): Promise<CidLike | null | undefined> {
        const cacheKey = this.shardCacheKey(index, shardIdx);

        // Already decoded (full mode, or a previously-promoted sparse shard) → O(1).
        const cached = this.shardDataCache.get(cacheKey);
        if (cached) {
            return cached[indexInShard];
        }

        if (this.shardReadMode === "full") {
            return (await this.getOrLoadDecodedShard(index, shardIdx))?.[indexInShard];
        }

        // Sparse: promote a hot shard to a one-shot full decode once it crosses the crossover.
        const hits = (this.shardAccessCounts.get(cacheKey) ?? 0) + 1;
        if (hits >= ShardedStore.SPARSE_PROMOTE_THRESHOLD) {
            this.shardAccessCounts.delete(cacheKey);
            return (await this.getOrLoadDecodedShard(index, shardIdx))?.[indexInShard];
        }
        this.shardAccessCounts.set(cacheKey, hits);
        return this.getSparseShardEntry(index, shardIdx, indexInShard);
    }

    private async getSparseShardEntry(
        index: ArrayIndex,
        shardIdx: number,
        indexInShard: number,
    ): Promise<CidLike | null | undefined> {
        const cacheKey = this.shardCacheKey(index, shardIdx);
        const cachedShard = this.shardDataCache.get(cacheKey);
        if (cachedShard) {
            return cachedShard[indexInShard];
        }

        const shardCid = index.shardCids[shardIdx];
        if (!shardCid) {
            return null;
        }

        const shardCidStr = String(shardCid);
        try {
            const shardBlockBytes = await this.ipfsElements.dagCbor.components.blockstore.get(CID.parse(shardCidStr));
            return decodeShardEntry(shardBlockBytes, indexInShard);
        } catch (err) {
            console.error(`Failed to sparsely decode shard ${shardIdx} (CID: ${shardCidStr}).`, err);
            return undefined;
        }
    }

    private async getOrLoadDecodedShard(index: ArrayIndex, shardIdx: number): Promise<(CidLike | null)[] | undefined> {
        const cacheKey = this.shardCacheKey(index, shardIdx);
        if (this.shardDataCache.has(cacheKey)) {
            return this.shardDataCache.get(cacheKey)!;
        }

        if (this.pendingShardLoads.has(cacheKey)) {
            await this.pendingShardLoads.get(cacheKey)!;
            return this.shardDataCache.get(cacheKey);
        }

        const shardCid = index.shardCids[shardIdx];
        if (!shardCid) {
            const emptyShard = new Array(index.chunksPerShard).fill(null);
            this.shardDataCache.set(cacheKey, emptyShard);
            return emptyShard;
        }

        const shardCidStr = String(shardCid);
        try {
            await this.loadAndCacheShard(cacheKey, shardIdx, shardCidStr);
            return this.shardDataCache.get(cacheKey);
        } catch (err) {
            console.error(`Failed to load shard ${shardIdx} (CID: ${shardCidStr}).`, err);
            return undefined;
        }
    }

    private loadAndCacheShard(cacheKey: string, shardIdx: number, shardCid: string): Promise<void> {
        if (this.pendingShardLoads.has(cacheKey)) {
            return this.pendingShardLoads.get(cacheKey)!;
        }
        const loadPromise = (async () => {
            try {
                const shardCidObj = CID.parse(shardCid);
                const shardBlockBytes = await this.ipfsElements.dagCbor.components.blockstore.get(shardCidObj);
                const decodedShard = dagCbor.decode<(CidLike | null)[]>(shardBlockBytes);

                if (!Array.isArray(decodedShard)) {
                    throw new TypeError(`Shard ${shardIdx} (CID: ${shardCid}) did not decode to an array.`);
                }
                this.shardDataCache.set(cacheKey, decodedShard);
            } catch (err) {
                console.error(`Failed to load and decode shard ${shardIdx} (CID: ${shardCid}):`, err);
                throw err;
            } finally {
                this.pendingShardLoads.delete(cacheKey);
            }
        })();

        this.pendingShardLoads.set(cacheKey, loadPromise);
        return loadPromise;
    }

    listMetadataKeys(): string[] {
        if (!this.rootObj) {
            throw new Error("Root object not loaded.");
        }
        return Object.keys(this.rootObj.metadata);
    }

    set(_key: AbsolutePath, _value: Uint8Array): Promise<void> {
        throw new Error("Store is read-only.");
    }

    delete(_key: AbsolutePath): Promise<void> {
        throw new Error("Store is read-only.");
    }

    getRange?(_key: AbsolutePath, _range: RangeQuery): Promise<Uint8Array | undefined> {
        throw new Error("Range requests are not supported in this read-only store.");
    }
}
