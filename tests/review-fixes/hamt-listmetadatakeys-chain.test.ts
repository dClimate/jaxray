import { afterEach, describe, expect, test, vi } from "vitest";
import * as dagCbor from "@ipld/dag-cbor";
import { CID } from "multiformats/cid";
import { sha256 } from "multiformats/hashes/sha2";

import {
  HamtStore,
  blake3,
  extractBits,
} from "../../src/backends/ipfs/hamt-store";
import type { IPFSELEMENTS_INTERFACE } from "../../src/backends/ipfs/ipfs-elements";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("BUG 3: HamtStore.listMetadataKeys throws TypeError when zarr.json lacks consolidated_metadata", () => {
  test("returns ['zarr.json'] for a plain (non-consolidated) v3 group", async () => {
    const rootMetaBytes = new TextEncoder().encode(
      JSON.stringify({ zarr_format: 3, node_type: "group", attributes: {} }),
    );
    const metaCid = CID.create(1, 0x55, await sha256.digest(rootMetaBytes));

    const key = "zarr.json";
    const hash = await blake3.encode(new TextEncoder().encode(key));
    const bucketIdx = extractBits(hash as Uint8Array, 0, 8);
    const nodeData: unknown[] = Array(256).fill(null).map(() => ({}));
    nodeData[bucketIdx] = { [key]: metaCid };
    const rootCid = CID.create(1, 0x71, await sha256.digest(dagCbor.encode(nodeData)));

    const ipfsElements: IPFSELEMENTS_INTERFACE = {
      dagCbor: {
        components: { blockstore: { get: vi.fn(async () => dagCbor.encode(nodeData)) } },
      },
      unixfs: {
        cat: vi.fn(async function* () {
          yield rootMetaBytes;
        }),
      },
    };

    const store = new HamtStore(rootCid, ipfsElements);
    // Correct behavior: the store still knows about the root zarr.json.
    // Current code dereferences `rootMeta?.consolidated_metadata.metadata`
    // without optional chaining and rejects with a TypeError.
    await expect(store.listMetadataKeys()).resolves.toEqual(["zarr.json"]);
  });
});
