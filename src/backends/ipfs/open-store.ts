import * as dagCbor from "@ipld/dag-cbor";
import { CID } from "multiformats/cid";
import { ShardedStore } from "./sharded-store.js";
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
  if (manifest.manifest_version === "sharded_zarr_v1") {
    return true;
  }
  if (manifest.chunks && typeof manifest.chunks === "object") {
    return true;
  }
  return false;
};

const tryDecodeManifest = (bytes: Uint8Array): Record<string, unknown> | null => {
  try {
    const decoded = dagCbor.decode(bytes);
    if (isShardedManifest(decoded)) {
      return decoded;
    }
  } catch {
    // ignore cbor decode error and try JSON
  }

  try {
    const json = JSON.parse(textDecoder.decode(bytes));
    if (isShardedManifest(json)) {
      return json;
    }
  } catch {
    // ignore JSON parse failure
  }

  return null;
};

export const detectIpfsStoreType = async (
  cid: string | CID,
  options?: ResolveOptionsInput,
): Promise<StoreType> => {
  const ipfsElements = resolveIpfsElements(options);
  const { bytes } = await fetchRootBlock(cid, ipfsElements);
  const manifest = tryDecodeManifest(bytes);
  if (manifest) {
    return "sharded";
  }
  return "hamt";
};

export const openIpfsStore = async (
  cid: string | CID,
  options?: ResolveOptionsInput,
): Promise<{ type: StoreType; store: ShardedStore | HamtStore }> => {
  const ipfsElements = resolveIpfsElements(options);
  const storeType = await detectIpfsStoreType(cid, ipfsElements);
  if (storeType === "sharded") {
    const root = typeof cid === "string" ? cid : cid.toString();
    const store = await ShardedStore.open(root, ipfsElements);
    return { type: "sharded", store };
  }
  const rootCid = typeof cid === "string" ? CID.parse(cid) : cid;
  const hamtStore = new HamtStore(rootCid, ipfsElements);
  return { type: "hamt", store: hamtStore };
};
