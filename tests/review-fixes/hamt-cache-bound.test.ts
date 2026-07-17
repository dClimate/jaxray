import { afterEach, describe, expect, test, vi } from "vitest";
import * as dagCbor from "@ipld/dag-cbor";

import { HamtStore } from "../../src/backends/ipfs/hamt-store";
import type { IPFSELEMENTS_INTERFACE } from "../../src/backends/ipfs/ipfs-elements";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("HamtStore node cache byte bound", () => {
  test("evicts cached nodes to keep their serialized byte footprint within 30 MB", async () => {
    const maxCacheBytes = 30_000_000;
    const nodeCount = 8;
    const serializedNode = dagCbor.encode([new Uint8Array(4_000_000)]);
    const blockstoreGet = vi.fn(async () => serializedNode);
    const ipfsElements: IPFSELEMENTS_INTERFACE = {
      dagCbor: {
        components: {
          blockstore: {
            get: blockstoreGet,
          },
        },
      },
      unixfs: {
        cat: vi.fn(async function* () {
          yield new Uint8Array();
        }),
      },
    };
    const store = new HamtStore({ toString: () => "root" }, ipfsElements);
    const nodeIds = Array.from({ length: nodeCount }, (_, index) => ({
      toString: () => `node-${index}`,
    }));

    for (const nodeId of nodeIds) {
      await (store as any).readNode(nodeId);
    }

    const cache = (store as any).cache as Map<string, { serialize(): Uint8Array }>;
    const cachedBytes = Array.from(cache.values()).reduce(
      (total: number, node: any) => total + node.serialize().length,
      0,
    );

    expect(
      cachedBytes,
      "the serialized node cache should remain within its documented 30 MB bound",
    ).toBeLessThanOrEqual(maxCacheBytes);
    expect(cache.size).toBe(7);
    expect(blockstoreGet).toHaveBeenCalledTimes(nodeCount);

    await (store as any).readNode(nodeIds[nodeCount - 1]);
    expect(blockstoreGet).toHaveBeenCalledTimes(nodeCount);

    await (store as any).readNode(nodeIds[0]);
    expect(blockstoreGet).toHaveBeenCalledTimes(nodeCount + 1);
  });
});
