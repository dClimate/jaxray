/**
 * RED-phase reproductions for ShardedStore error propagation.
 *
 * Each test asserts the correct behavior: infrastructure failures must reject
 * rather than being interpreted as missing chunks.
 */

import { describe, test, expect, vi } from "vitest";

import { ShardedStore } from "../../src/backends/ipfs/sharded-store";
import type { IPFSELEMENTS_INTERFACE } from "../../src/backends/ipfs/ipfs-elements";

const CHUNK_CID = "bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const SHARD_CID = "bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u";

void CHUNK_CID;

describe("BUG 1: ShardedStore turns network errors into 'missing chunk' (fill_value corruption)", () => {
  const makeManifest = () => ({
    manifest_version: "sharded_zarr_v2" as const,
    metadata: {},
    arrays: {
      "": {
        array_shape: [1],
        chunk_shape: [1],
        sharding_config: { chunks_per_shard: 1 },
        shard_cids: [SHARD_CID], // shard EXISTS; fetching it fails transiently
      },
    },
  });

  const failingElements = (): IPFSELEMENTS_INTERFACE => ({
    dagCbor: {
      components: {
        blockstore: {
          get: vi.fn(async () => {
            throw new Error("gateway unavailable (HTTP 500)");
          }),
        },
      },
    },
    unixfs: {
      cat: vi.fn(async function* () {
        yield new TextEncoder().encode("chunk");
      }),
    },
  });

  test("sparse mode: get() must propagate the shard fetch error, not resolve undefined", async () => {
    const store = ShardedStore.fromRootObject("root", failingElements(), makeManifest(), "sparse");
    // Correct behavior: a failed shard fetch is an ERROR, not "chunk absent".
    // Current code resolves `undefined`, which zarrita silently maps to fill_value.
    await expect(store.get("c/0")).rejects.toThrow(/gateway unavailable/);
  });

  test("full mode: get() must propagate the shard fetch error, not resolve undefined", async () => {
    const store = ShardedStore.fromRootObject("root", failingElements(), makeManifest(), "full");
    await expect(store.get("c/0")).rejects.toThrow(/gateway unavailable/);
  });
});
