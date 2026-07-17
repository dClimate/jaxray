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
    const ipfsElements: IPFSELEMENTS_INTERFACE = {
      dagCbor: {
        components: {
          blockstore: {
            get: vi.fn(async () => serializedNode),
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

    for (let index = 0; index < nodeCount; index += 1) {
      await (store as any).readNode({ toString: () => `node-${index}` });
    }

    const cachedBytes = Array.from((store as any).cache.values()).reduce(
      (total: number, node: any) => total + node.serialize().length,
      0,
    );

    expect(
      cachedBytes,
      "the serialized node cache should remain within its documented 30 MB bound",
    ).toBeLessThanOrEqual(maxCacheBytes);
  });
});
