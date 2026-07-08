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
      chunks: {
        array_shape: [0],
        chunk_shape: [1],
        shard_cids: [],
        sharding_config: { chunks_per_shard: 1 },
      },
    };
    const bytes = dagCbor.encode(manifest);
    const ipfsElements = createMockIpfsElements(bytes);

    const type = await detectIpfsStoreType(
      "bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u",
      ipfsElements,
    );
    expect(type).toBe("sharded");

    vi.mocked(ipfsElements.dagCbor.components.blockstore.get).mockClear();

    const { type: resolvedType, store } = await openIpfsStore(
      "bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u",
      ipfsElements,
    );
    expect(resolvedType).toBe("sharded");
    expect(store).toBeInstanceOf(ShardedStore);
    expect(ipfsElements.dagCbor.components.blockstore.get).toHaveBeenCalledTimes(1);
  });

  test("detects sharded zarr v2 manifests and opens ShardedStore", async () => {
    const manifest = {
      manifest_version: "sharded_zarr_v2",
      sharding_config: { chunks_per_shard: 2, order: "C" },
      metadata: {},
      arrays: {
        "0/FPAR": {
          array_shape: [2, 2],
          chunk_shape: [1, 1],
          sharding_config: { chunks_per_shard: 2, order: "C" },
          shard_cids: [null, null],
        },
      },
    };
    const bytes = dagCbor.encode(manifest);
    const ipfsElements = createMockIpfsElements(bytes);

    const type = await detectIpfsStoreType(
      "bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u",
      ipfsElements,
    );
    expect(type).toBe("sharded");

    const { type: resolvedType, store } = await openIpfsStore(
      "bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u",
      ipfsElements,
    );
    expect(resolvedType).toBe("sharded");
    expect(store).toBeInstanceOf(ShardedStore);
  });

  test("reads sharded zarr v2 metadata and chunks by array path", async () => {
    const metadataCid = "bafybeigqno5lcjnsruv4qma5ma2wefeevuzbnz5u7p5wxzfxdrdvuxnqna";
    const shardCid = "bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u";
    const chunkCid = "bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    const metadataBytes = new TextEncoder().encode(JSON.stringify({ node_type: "array" }));
    const chunkBytes = new TextEncoder().encode("chunk-0");
    const shardBytes = dagCbor.encode([chunkCid, null]);
    const ipfsElements: IPFSELEMENTS_INTERFACE = {
      dagCbor: {
        components: {
          blockstore: {
            get: vi.fn(async () => shardBytes),
          },
        },
      },
      unixfs: {
        cat: vi.fn(async function* (cid: string) {
          yield cid === metadataCid ? metadataBytes : chunkBytes;
        }),
      },
    };
    const store = ShardedStore.fromRootObject("root", ipfsElements, {
      manifest_version: "sharded_zarr_v2",
      sharding_config: { chunks_per_shard: 2, order: "C" },
      metadata: {
        "0/FPAR/zarr.json": metadataCid,
      },
      arrays: {
        "0/FPAR": {
          array_shape: [2, 2],
          chunk_shape: [1, 1],
          sharding_config: { chunks_per_shard: 2, order: "C" },
          shard_cids: [shardCid, null],
        },
      },
    });

    expect(store.listMetadataKeys()).toEqual(["0/FPAR/zarr.json"]);
    await expect(store.get("0/FPAR/zarr.json")).resolves.toEqual(metadataBytes);
    await expect(store.has("0/FPAR/c/0/0" as any)).resolves.toBe(true);
    await expect(store.get("0/FPAR/c/0/0")).resolves.toEqual(chunkBytes);
    await expect(store.has("1/FPAR/c/0/0" as any)).resolves.toBe(false);
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
