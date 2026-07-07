import * as dagCbor from "@ipld/dag-cbor";
import { CID } from "multiformats/cid";
import { ShardedStore, type ShardedRoot } from "./sharded-store.js";
import { HamtStore } from "./hamt-store.js";
import { IPFSELEMENTS_INTERFACE, createIpfsElements } from "./ipfs-elements.js";

type StoreType = "sharded" | "hamt";

const DEFAULT_GATEWAY = "https://ipfs-gateway.dclimate.net";
const textDecoder = new TextDecoder();

type ResolveOptionsInput = IPFSELEMENTS_INTERFACE | OpenStoreOptions | undefined;

export type OpenStoreOptions = {
  /**
   * Supply fully customised IPFS primitives. Takes precedence over gatewayUrl.
   */
  ipfsElements?: IPFSELEMENTS_INTERFACE;
  /**
   * Gateway URL to use when ipfsElements is not provided. Defaults to the
   * dClimate public gateway for convenience.
   */
  gatewayUrl?: string;
};

const isIpfsElements = (value: unknown): value is IPFSELEMENTS_INTERFACE => {
  return Boolean(
    value &&
      typeof value === "object" &&
      "dagCbor" in value &&
      (value as any).dagCbor?.components?.blockstore?.get,
  );
};

const toOptions = (input: ResolveOptionsInput): OpenStoreOptions => {
  if (!input) {
    return {};
  }
  if (isIpfsElements(input)) {
    return { ipfsElements: input };
  }
  return input;
};

const resolveIpfsElements = (input: ResolveOptionsInput): IPFSELEMENTS_INTERFACE => {
  const options = toOptions(input);
  if (options.ipfsElements) {
    return options.ipfsElements;
  }
  const gateway = options.gatewayUrl ?? DEFAULT_GATEWAY;
  return createIpfsElements(gateway);
};

const fetchRootBlock = async (
  cid: string | CID,
  ipfsElements: IPFSELEMENTS_INTERFACE,
): Promise<{ rootCid: CID; bytes: Uint8Array }> => {
  const rootCid = typeof cid === "string" ? CID.parse(cid) : cid;
  const bytes = await ipfsElements.dagCbor.components.blockstore.get(rootCid);
  return { rootCid, bytes };
};

const isShardedManifest = (decoded: unknown): decoded is { manifest_version: string } => {
  if (!decoded || typeof decoded !== "object" || Array.isArray(decoded)) {
    return false;
  }
  const manifest = decoded as Record<string, unknown>;
  if (manifest.manifest_version === "sharded_zarr_v1" || manifest.manifest_version === "sharded_zarr_v2") {
    return true;
  }
  if (manifest.chunks && typeof manifest.chunks === "object") {
    return true;
  }
  if (manifest.arrays && typeof manifest.arrays === "object") {
    return true;
  }
  return false;
};

const tryDecodeManifest = (bytes: Uint8Array): ShardedRoot | null => {
  try {
    const decoded = dagCbor.decode(bytes);
    if (isShardedManifest(decoded)) {
      return decoded as ShardedRoot;
    }
  } catch {
    // ignore cbor decode error and try JSON
  }

  try {
    const json = JSON.parse(textDecoder.decode(bytes));
    if (isShardedManifest(json)) {
      return json as ShardedRoot;
    }
  } catch {
    // ignore JSON parse failure
  }

  return null;
};

const resolveStoreType = async (
  cid: string | CID,
  ipfsElements: IPFSELEMENTS_INTERFACE,
): Promise<{ rootCid: CID; type: StoreType; manifest: ShardedRoot | null }> => {
  const { rootCid, bytes } = await fetchRootBlock(cid, ipfsElements);
  const manifest = tryDecodeManifest(bytes);
  return {
    rootCid,
    type: manifest ? "sharded" : "hamt",
    manifest,
  };
};

export const detectIpfsStoreType = async (
  cid: string | CID,
  options?: ResolveOptionsInput,
): Promise<StoreType> => {
  const ipfsElements = resolveIpfsElements(options);
  const { type } = await resolveStoreType(cid, ipfsElements);
  return type;
};

export const openIpfsStore = async (
  cid: string | CID,
  options?: ResolveOptionsInput,
): Promise<{ type: StoreType; store: ShardedStore | HamtStore }> => {
  const ipfsElements = resolveIpfsElements(options);
  const { rootCid, type, manifest } = await resolveStoreType(cid, ipfsElements);
  if (type === "sharded") {
    const root = typeof cid === "string" ? cid : cid.toString();
    const store = ShardedStore.fromRootObject(root, ipfsElements, manifest!);
    return { type: "sharded", store };
  }
  const hamtStore = new HamtStore(rootCid, ipfsElements);
  return { type: "hamt", store: hamtStore };
};
