/* eslint-disable no-await-in-loop */
// Disable max-classes-per-file rule for this file
// This file contains the IPFSStore class and related functions for handling Zarr data
// in IPFS using the Zarrita library. The py-hamt implementation comes from https://github.com/dClimate/py-hamt
/* eslint-disable no-bitwise */
// eslint-disable-next-line max-classes-per-file
import { AsyncReadable, AbsolutePath, RangeQuery } from "zarrita"; // Adjust import based on Zarrita package
// eslint-disable-next-line import/no-extraneous-dependencies
import * as blockCodec from "@ipld/dag-cbor";
import { concat as uint8ArrayConcat } from "uint8arrays/concat";
import all from "it-all";
import { blake3 as b3 } from "@noble/hashes/blake3";
import { CID, hasher } from "multiformats";
// eslint-disable-next-line import/no-extraneous-dependencies
import { UnixFS } from "ipfs-unixfs";

import * as zarr from "zarrita";
import { IPFSELEMENTS_INTERFACE } from "./ipfs-elements";

type ExperimentalV3RootMetadata = {
    zarr_format: 3;
    node_type: "group";
    attributes?: Record<string, unknown>;
    consolidated_metadata: Record<string, zarr.ArrayMetadata | zarr.GroupMetadata>;
};

export async function openExperimentalV3Consolidated<Store extends zarr.Readable>(
    store: Store,
): Promise<Array<zarr.Array<zarr.DataType, Store> | zarr.Group<Store>>> {
    const location = zarr.root(store);
    const rootMetadata: ExperimentalV3RootMetadata = JSON.parse(
        new TextDecoder().decode(await store.get(location.resolve("zarr.json").path)),
    );
    return Object.entries(rootMetadata.consolidated_metadata).map(([nodeName, nodeMeta]) => {
        const nodePath = location.resolve(nodeName).path;
        if (nodeMeta.node_type === "array") {
            return new zarr.Array(store, nodePath, nodeMeta);
        }
        return new zarr.Group(store, nodePath, nodeMeta);
    });
}

class KeyError extends Error {
    constructor(key: string) {
        super(`Key not found: ${key}`);
        this.name = "KeyError";
    }
}

type BucketItem = Record<string, any> | [CID];

class Node {
    data: BucketItem[];

    /**
     * Initializes a new Node.
     * The `data` array holds 256 "buckets".
     * Each bucket is either:
     * - An empty object `{}` (representing an empty bucket).
     * - An object `{[key: string]: IPLDKind}` (a bucket with key-value pairs).
     * - A list `[CID]` (representing a link to another node, where CID is the link).
     */
    constructor() {
        // Initialize data with 256 empty objects (buckets)
        this.data = Array(256)
            .fill(null)
            .map(() => ({}));
    }

    /**
     * Iterates over the buckets (elements in `this.data` that are objects/maps).
     * @returns {Iterator<Record<string, any>>} An iterator for buckets.
     */
    *iterBuckets() {
        // eslint-disable-next-line no-restricted-syntax
        for (const item of this.data) {
            if (typeof item === "object" && item !== null && !Array.isArray(item)) {
                yield item;
            }
        }
    }

    /**
     * Iterates over the indices in `this.data` that point to links.
     * @returns {Iterator<number>} An iterator for indices of links.
     */
    *iterLinkIndices() {
        for (let i = 0; i < this.data.length; i++) {
            if (Array.isArray(this.data[i])) {
                // Links are stored as [CID]
                yield i;
            }
        }
    }

    /**
     * Iterates over the links (CID objects) themselves.
     * @returns {Iterator<CID>} An iterator for CIDs.
     */
    *iterLinks() {
        // eslint-disable-next-line no-restricted-syntax
        for (const item of this.data) {
            if (Array.isArray(item) && item.length > 0 && item[0] instanceof CID) {
                yield item[0];
            }
        }
    }

    /**
     * Gets the link (CID) at a specific index.
     * @param {number} index - The index in `this.data`.
     * @returns {CID | undefined} The CID if a link exists at the index, otherwise undefined.
     */
    getLink(index: number): CID | undefined {
        const linkWrapper = this.data[index];
        if (Array.isArray(linkWrapper) && linkWrapper.length > 0 && linkWrapper[0] instanceof CID) {
            return linkWrapper[0];
        }
        return undefined; // Or throw error if index should always be a link
    }

    /**
     * Sets a link (CID) at a specific index.
     * @param {number} index - The index in `this.data`.
     * @param {CID} link - The CID to set.
     */
    // eslint-disable-next-line class-methods-use-this
    setLink() {
        throw new Error("setLink method is not implemented yet.");
    }

    /**
     * Replaces an old link with a new link.
     * Assumes only one unique link matches the old link.
     * @param {CID} oldLink - The CID of the link to replace.
     * @param {CID} newLink - The new CID.
     */
    // eslint-disable-next-line class-methods-use-this
    replaceLink() {
        throw new Error("replaceLink method is not implemented");
    }

    serialize(): Uint8Array {
        return blockCodec.encode(this.data);
    }

    static deserialize(data: Uint8Array): Node {
        try {
            const decoded = blockCodec.decode(data);
            if (Array.isArray(decoded)) {
                const node = new Node();
                node.data = decoded as BucketItem[];
                return node;
            }
            throw new Error("Invalid node data structure");
        } catch (e) {
            console.error("Error decoding node data: ", e);
            throw new Error("Invalid dag-cbor encoded data");
        }
    }
}

// Helper function to extract bits
export function extractBits(hashBytes: Uint8Array, depth: number, nbits: number): number {
    const hashBitLength = hashBytes.length * 8;
    const startBitIndex = depth * nbits;

    if (hashBitLength - startBitIndex < nbits) {
        throw new Error("Arguments extract more bits than remain in the hash bits");
    }

    // Ensure bit shift is within safe range
    if (hashBitLength - startBitIndex <= 0) {
        throw new Error("Invalid bit extraction range");
    }

    // Use BigInt for safe shifting
    const mask = (BigInt(1) << BigInt(hashBitLength - startBitIndex)) - BigInt(1);

    if (mask === BigInt(0)) {
        throw new Error("Invalid mask value: 0");
    }

    // Equivalent of Python's int.bit_length()
    const nChopOffAtEnd = mask.toString(2).length - nbits;

    // Convert bytes to BigInt
    let hashAsInt = BigInt(0);
    for (let i = 0; i < hashBytes.length; i++) {
        hashAsInt = (hashAsInt << BigInt(8)) | BigInt(hashBytes[i]);
    }

    // Extract bits
    const result = Number((mask & hashAsInt) >> BigInt(nChopOffAtEnd));
    return result;
}

export const blake3 = hasher.from({
    name: "blake3",
    code: 0x1e,
    encode: (input) => b3(input),
});

export class IPFSStore implements AsyncReadable {
    public cid: CID;

    public ipfsElements: IPFSELEMENTS_INTERFACE;

    private cache: Map<string, Node> = new Map();

    private readonly maxCacheSize: number = 30_000_000; // 30MB

    public metadata: any;

    constructor(cid: any, ipfsElements: IPFSELEMENTS_INTERFACE) {
        this.cid = cid;
        this.ipfsElements = ipfsElements;
    }

    // eslint-disable-next-line class-methods-use-this
    private async hashFn(input: string): Promise<Uint8Array> {
        const encoder = new TextEncoder();
        const hashBytes = encoder.encode(input);
        return blake3.encode(hashBytes);
    }

    private async writeNode(node: Node): Promise<any> {
        // do not support writing links yet
        throw new Error("writeNode method is not implemented yet.");
        // const serialized = node.serialize();
        // const cid = await this.ipfsElements.dagCbor.components.blockstore.put(serialized);
        // this.cache.set(cid.toString(), node);
        // this.maintainCacheSize();
        // return cid;
    }

    private async readNode(nodeId: any): Promise<Node> {
        const cidStr = nodeId.toString();
        if (this.cache.has(cidStr)) {
            return this.cache.get(cidStr)!;
        }
        const bytes = await this.ipfsElements.dagCbor.components.blockstore.get(nodeId);
        const node = Node.deserialize(bytes);
        this.cache.set(cidStr, node);
        this.maintainCacheSize();
        return node;
    }

    private maintainCacheSize(): void {
        if (this.cache.size > this.maxCacheSize) {
            const firstKey = this.cache.keys().next().value;
            if (firstKey) {
                this.cache.delete(firstKey);
            }
        }
    }

    /**
     * A reusable private method to traverse the HAMT and find the bucket for a given key.
     * This encapsulates the common logic of both `findCIDForKey` and `findItemInNode`.
     * @param {string} inputKey - The key to search for.
     * @returns {Promise<{ bucket: Record<string, any>; normalizedKey: string }>} - An object containing the found bucket and the normalized key.
     * @private
     */
    private async findBucket(inputKey: string): Promise<{ bucket: Record<string, any>; normalizedKey: string }> {
        const normalizedKey = inputKey.startsWith("/") ? inputKey.slice(1) : inputKey;
        const hash = await this.hashFn(normalizedKey);
        let currentNodeId = this.cid;
        let depth = 0;

        while (currentNodeId) {
            // It's acceptable to await in a loop when the next iteration depends on the result of the previous one.
            const node = await this.readNode(currentNodeId);
            const mapKey = extractBits(hash, depth, 8);
            const nodeItem = node.data[mapKey];

            if (Array.isArray(nodeItem)) {
                [currentNodeId] = nodeItem; // More concise destructuring
                depth += 1;
            } else if (nodeItem && typeof nodeItem === "object") {
                return { bucket: nodeItem, normalizedKey };
            } else {
                // If nodeItem is not a link or a bucket, the key is not found.
                break;
            }
        }

        throw new KeyError(normalizedKey);
    }

    /**
     * Find the CID for a given key in the HAMT structure.
     * @param {string} inputKey - The key for which to find the CID.
     * @returns {Promise<string>} - The CID associated with the key.
     */
    public async findCIDForKey(inputKey: string): Promise<string> {
        const { bucket, normalizedKey } = await this.findBucket(inputKey);

        if (normalizedKey in bucket) {
            return bucket[normalizedKey];
        }

        throw new KeyError(normalizedKey);
    }

    /**
     * Finds the raw value (as a Uint8Array) for a given item in the HAMT.
     * @param {string} itemKey - The key of the item to find.
     * @returns {Promise<Uint8Array>} - The value as a Uint8Array.
     */
    public async findItemInNode(itemKey: string): Promise<Uint8Array> {
        const { bucket, normalizedKey } = await this.findBucket(itemKey);

        if (normalizedKey in bucket) {
            const cid = bucket[normalizedKey];
            const contentBlocks = await all(this.ipfsElements.unixfs.cat(cid));
            return uint8ArrayConcat(contentBlocks as Uint8Array[]);
        }

        throw new KeyError(normalizedKey);
    }

    /** Fetch item data, with optional range support */
    private async fetchItem(cid: any, range?: { offset: number; length: number }): Promise<Uint8Array> {
        if (cid.codec === "raw") {
            // Raw block: fetch entire block and slice if range is specified
            const block = await this.ipfsElements.dagCbor.components.blockstore.get(cid);
            if (range) {
                const { offset, length } = range;
                return block.slice(offset, offset + length);
            }
            return block;
        }
        if (cid.codec === "dag-pb") {
            // UnixFS file: use range-capable unixfs.cat
            const catOptions = range ? { offset: range.offset, length: range.length } : {};
            const chunks = await all(this.ipfsElements.unixfs.cat(cid, catOptions));
            return uint8ArrayConcat(chunks as Uint8Array[]);
        }
        throw new Error(`Unsupported CID codec: ${cid.codec}`);
    }

    /** Get the size of an item based on its CID */
    private async getItemSize(cid: any): Promise<number> {
        if (cid.codec === "raw") {
            const block = await this.ipfsElements.dagCbor.components.blockstore.get(cid);
            return block.length;
        }
        if (cid.codec === "dag-pb") {
            const bytes = await this.ipfsElements.dagCbor.components.blockstore.get(cid);
            const unixfsFile = UnixFS.unmarshal(bytes);
            if (unixfsFile.type === "file") {
                return Number(unixfsFile.fileSize());
            }
            throw new Error("Not a file");
        } else {
            throw new Error(`Unsupported CID codec: ${cid.codec}`);
        }
    }

    /** Fetch entire item for a key */
    async get(key: AbsolutePath): Promise<Uint8Array | undefined> {
        try {
            const data = await this.findItemInNode(key);
            return data;
        } catch (e) {
            if (e instanceof KeyError) {
                return undefined; // Key not found
            }
            throw e;
        }
    }

    /** Fetch a byte range for a key */
    async getRange(key: AbsolutePath, range: RangeQuery): Promise<Uint8Array | undefined> {
        try {
            const cid = await this.findCIDForKey(key);
            if ("suffixLength" in range) {
                const size = await this.getItemSize(cid);
                const offset = size - range.suffixLength;
                return await this.fetchItem(cid, { offset, length: range.suffixLength });
            }
            return await this.fetchItem(cid, { offset: range.offset, length: range.length });
        } catch (e) {
            if (e instanceof KeyError) {
                return undefined; // Key not found
            }
            throw e;
        }
    }
}
