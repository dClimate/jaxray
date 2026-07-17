import { describe, test, expect, vi } from "vitest";
import * as dagCbor from "@ipld/dag-cbor";
import { CID } from "multiformats/cid";

import { ShardedStore } from "../../src/backends/ipfs/sharded-store";
import type { IPFSELEMENTS_INTERFACE } from "../../src/backends/ipfs/ipfs-elements";

const CHUNK_CID = "bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const SHARD_CID = "bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u";

describe("BUG 6: ShardedStore v1 manifest silently drops root-level 'c/...' chunk keys", () => {
  test("v1 root array chunk 'c/0' resolves to the chunk bytes (as it does for v2)", async () => {
    const chunkBytes = new TextEncoder().encode("v1-chunk");
    const ipfsElements: IPFSELEMENTS_INTERFACE = {
      dagCbor: {
        components: {
          blockstore: { get: vi.fn(async () => dagCbor.encode([CID.parse(CHUNK_CID), null])) },
        },
      },
      unixfs: {
        cat: vi.fn(async function* () {
          yield chunkBytes;
        }),
      },
    };

    const store = ShardedStore.fromRootObject("root", ipfsElements, {
      manifest_version: "sharded_zarr_v1",
      metadata: { "zarr.json": CHUNK_CID },
      chunks: {
        array_shape: [2],
        chunk_shape: [1],
        sharding_config: { chunks_per_shard: 2 },
        shard_cids: [SHARD_CID],
      },
    });

    // Correct behavior: a v1 manifest describes a single array; when that array
    // lives at the store root, zarrita asks for "/c/0". v2 handles the bare
    // "c/..." prefix but v1 does not, so the key falls through to the metadata
    // map and resolves `undefined` -> fill_value instead of real data.
    await expect(store.get("/c/0")).resolves.toEqual(chunkBytes);
  });
});
