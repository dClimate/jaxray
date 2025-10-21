import { describe, expect, test, vi, afterEach } from "vitest";
import * as dagCbor from "@ipld/dag-cbor";
import { detectIpfsStoreType, openIpfsStore } from "../src/backends/ipfs/open-store";
import { ShardedStore } from "../src/backends/ipfs/sharded-store";
import { HamtStore } from "../src/backends/ipfs/hamt-store";
import type { IPFSELEMENTS_INTERFACE } from "../src/backends/ipfs/ipfs-elements";

afterEach(() => {
  vi.restoreAllMocks();
});

function createMockIpfsElements(rootBytes: Uint8Array): IPFSELEMENTS_INTERFACE {
  return {
    dagCbor: {
      components: {
        blockstore: {
          get: vi.fn(async () => rootBytes),
        },
      },
    },
    unixfs: {
      cat: vi.fn(async function* () {
        yield new Uint8Array();
      }),
    },
  };
}

describe("IPFS store detection", () => {
  test("detects sharded manifests and opens ShardedStore", async () => {
    const manifest = {
      manifest_version: "sharded_zarr_v1",
      metadata: {},
      chunks: { shard_cids: [], sharding_config: { chunks_per_shard: 1 } },
    };
    const bytes = dagCbor.encode(manifest);
    const ipfsElements = createMockIpfsElements(bytes);

    const type = await detectIpfsStoreType(
      "bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u",
      ipfsElements,
    );
    expect(type).toBe("sharded");

    const fakeStore = { readOnly: true } as unknown as ShardedStore;
    const openSpy = vi.spyOn(ShardedStore, "open").mockResolvedValue(fakeStore);

    const { type: resolvedType, store } = await openIpfsStore(
      "bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u",
      ipfsElements,
    );
    expect(resolvedType).toBe("sharded");
    expect(openSpy).toHaveBeenCalled();
    expect(store).toBe(fakeStore);
  });

  test("falls back to HAMT store when manifest is absent", async () => {
    const hamtRootNode = Array(256).fill({});
    const bytes = dagCbor.encode(hamtRootNode);
    const ipfsElements = createMockIpfsElements(bytes);

    const type = await detectIpfsStoreType(
      "bafyr4ihpa7gtcpdmcuoqvdde6x2dll6maskygcbruplqe525ptfpybdh7i",
      ipfsElements,
    );
    expect(type).toBe("hamt");

    const { type: resolvedType, store } = await openIpfsStore(
      "bafyr4ihpa7gtcpdmcuoqvdde6x2dll6maskygcbruplqe525ptfpybdh7i",
      ipfsElements,
    );
    expect(resolvedType).toBe("hamt");
    expect(store).toBeInstanceOf(HamtStore);
  });
});
