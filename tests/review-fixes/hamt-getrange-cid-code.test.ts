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

describe("BUG 2: HamtStore.getRange is broken because multiformats CID has no `.codec` property", () => {
  test("getRange on a raw-codec CID returns the requested byte range", async () => {
    const block = new TextEncoder().encode("0123456789");
    const rawCid = CID.create(1, 0x55, await sha256.digest(block));

    const key = "data/c/0";
    const hash = await blake3.encode(new TextEncoder().encode(key));
    const bucketIdx = extractBits(hash as Uint8Array, 0, 8);
    const nodeData: unknown[] = Array(256)
      .fill(null)
      .map(() => ({}));
    nodeData[bucketIdx] = { [key]: rawCid };
    const rootCid = CID.create(1, 0x71, await sha256.digest(dagCbor.encode(nodeData)));

    const ipfsElements: IPFSELEMENTS_INTERFACE = {
      dagCbor: {
        components: {
          blockstore: {
            get: vi.fn(async (cid: any) =>
              String(cid) === String(rawCid) ? block : dagCbor.encode(nodeData),
            ),
          },
        },
      },
      unixfs: {
        cat: vi.fn(async function* () {
          yield block;
        }),
      },
    };

    const store = new HamtStore(rootCid, ipfsElements);
    await expect(store.getRange("/data/c/0", { offset: 2, length: 3 })).resolves.toEqual(
      new TextEncoder().encode("234"),
    );
  });
});
