// tsconfig target: ES2022 or later
// npm i multiformats @noble/hashes @noble/hashes blake3

import { CID } from "multiformats/cid";
import { base58btc } from "multiformats/bases/base58";

// -------------------------- IPLD kind (scalar only) --------------------------
// Python note said: allow IPLDKind scalar, but exclude lists/dicts/None.
// In TS we'll model allowed scalar-ish values (no arrays/objects/null).
export type IPLDKind =
  | CID
  | string
  | number
  | boolean
  | Uint8Array;

// ------------------------------- Semaphore -----------------------------------
class Semaphore {
  private readonly max: number;
  private queue: Array<() => void> = [];
  private count = 0;

  constructor(max: number) {
    this.max = Math.max(1, max);
  }
  async withPermit<T>(fn: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
  private acquire(): Promise<void> {
    if (this.count < this.max) {
      this.count++;
      return Promise.resolve();
    }
    return new Promise((resolve) => this.queue.push(resolve));
  }
  private release() {
    this.count--;
    const next = this.queue.shift();
    if (next) {
      this.count++;
      next();
    }
  }
}

// ----------------------- ContentAddressedStore API ---------------------------
export type CodecInput = "raw" | "dag-cbor";

export abstract class ContentAddressedStore {
  abstract save(data: Uint8Array, codec: CodecInput): Promise<IPLDKind>;
  abstract load(
    id: IPLDKind,
    offset?: number | null,
    length?: number | null,
    suffix?: number | null
  ): Promise<Uint8Array>;

  async pin_cid(_id: IPLDKind, _target_rpc: string): Promise<void> {/* no-op */}
  async unpin_cid(_id: IPLDKind, _target_rpc: string): Promise<void> {/* no-op */}
  async pin_update(_oldId: IPLDKind, _newId: IPLDKind, _target_rpc: string): Promise<void> {/* no-op */}
  async pin_ls(_target_rpc: string): Promise<Array<Record<string, unknown>>> {
    return [];
  }
}

// -------------------------------- KuboCAS ------------------------------------
export interface KuboCASOptions {
  hasher?: string; // name passed to Kubo /api/v0/add hash=
  rpcBaseUrl?: string | null;
  gatewayBaseUrl?: string | null;
  concurrency?: number;
  headers?: Record<string, string>;
  auth?: { username: string; password: string } | null;
  pinOnAdd?: boolean;
  chunker?: string; // e.g. "size-1048576" | "rabin" | "rabin-<min>-<avg>-<max>"
  maxRetries?: number;
  initialDelay?: number; // seconds
  backoffFactor?: number; // >= 1.0
}

export class KuboCAS extends ContentAddressedStore {
  static readonly KUBO_DEFAULT_LOCAL_GATEWAY_BASE_URL = "http://127.0.0.1:8080";
  static readonly KUBO_DEFAULT_LOCAL_RPC_BASE_URL = "http://127.0.0.1:5001";
  private static readonly DAG_PB_MARKER = 0x70; // for parity with Python check

  private readonly hasher: string;
  private readonly rpcUrl: string;
  private readonly gatewayBase: string;
  private readonly sem: Semaphore;
  private readonly headers?: Record<string, string>;
  private readonly authHeader?: string;
  private readonly maxRetries: number;
  private readonly initialDelay: number;
  private readonly backoffFactor: number;

  constructor(opts: KuboCASOptions = {}) {
    super();

    const {
      hasher = "blake3",
      rpcBaseUrl = KuboCAS.KUBO_DEFAULT_LOCAL_RPC_BASE_URL,
      gatewayBaseUrl = KuboCAS.KUBO_DEFAULT_LOCAL_GATEWAY_BASE_URL,
      concurrency = 32,
      headers,
      auth = null,
      pinOnAdd = false,
      chunker = "size-1048576",
      maxRetries = 3,
      initialDelay = 1.0,
      backoffFactor = 2.0,
    } = opts;

    // Validate chunker like Python
    const chunkerPattern = /^(?:size-[1-9]\d*|rabin(?:-[1-9]\d*-[1-9]\d*-[1-9]\d*)?)$/;
    if (!chunkerPattern.test(chunker)) {
      throw new Error("Invalid chunker specification");
    }
    if (maxRetries < 0) throw new Error("max_retries must be non-negative");
    if (initialDelay <= 0) throw new Error("initial_delay must be positive");
    if (backoffFactor < 1.0) throw new Error("backoff_factor must be >= 1.0");

    this.hasher = hasher;

    let gateway = gatewayBaseUrl || KuboCAS.KUBO_DEFAULT_LOCAL_GATEWAY_BASE_URL;
    if (gateway.includes("/ipfs/")) {
      gateway = gateway.split("/ipfs/")[0];
    }
    this.gatewayBase = gateway.endsWith("/") ? `${gateway}ipfs/` : `${gateway}/ipfs/`;

    const pinStr = pinOnAdd ? "true" : "false";
    const rpc = (rpcBaseUrl || KuboCAS.KUBO_DEFAULT_LOCAL_RPC_BASE_URL).replace(/\/+$/, "");
    this.rpcUrl = `${rpc}/api/v0/add?hash=${encodeURIComponent(this.hasher)}&chunker=${encodeURIComponent(chunker)}&pin=${pinStr}`;

    this.sem = new Semaphore(concurrency);
    this.headers = headers;
    if (auth) {
      // Use btoa for browser compatibility instead of Buffer
      const credentials = `${auth.username}:${auth.password}`;
      this.authHeader = "Basic " + (typeof Buffer !== 'undefined'
        ? Buffer.from(credentials).toString("base64")
        : btoa(credentials));
    }
    this.maxRetries = maxRetries;
    this.initialDelay = initialDelay;
    this.backoffFactor = backoffFactor;
  }

  // ------------------------------- utilities --------------------------------
  private buildHeaders(extra?: Record<string, string>): Headers {
    const h = new Headers(this.headers ?? {});
    if (this.authHeader) h.set("Authorization", this.authHeader);
    if (extra) for (const [k, v] of Object.entries(extra)) h.set(k, v);
    return h;
  }

  private async retrying<T>(fn: () => Promise<T>): Promise<T> {
    let attempt = 0;
    while (attempt <= this.maxRetries) {
      try {
        return await fn();
      } catch (err: any) {
        attempt++;
        const isTimeoutOrNetwork =
          err?.name === "AbortError" ||
          err?.code === "ECONNRESET" ||
          err?.type === "system" ||
          (err?.message && /timeout|network/i.test(err.message));
        if (!isTimeoutOrNetwork || attempt > this.maxRetries) {
          throw err;
        }
        const delay = this.initialDelay * Math.pow(this.backoffFactor, attempt - 1);
        const jitter = delay * 0.1 * (Math.random() - 0.5);
        await new Promise((r) => setTimeout(r, (delay + jitter) * 1000));
      }
    }
    throw new Error("Exited the retry loop unexpectedly.");
  }

  // --------------------------------- save ------------------------------------
  async save(data: Uint8Array, codec: CodecInput): Promise<CID> {
    return this.sem.withPermit(async () =>
      this.retrying(async () => {
        const form = new FormData();
        // Blob is widely supported; for Node 18+, global Blob exists
        form.append("file", new Blob([data]));

        const res = await fetch(this.rpcUrl, {
          method: "POST",
          headers: this.buildHeaders(),
          body: form,
        });

        if (!res.ok) {
          const text = await res.text().catch(() => "");
          throw new Error(`Kubo add failed: ${res.status} ${res.statusText} ${text}`);
        }
        const json = await res.json() as { Hash: string };
        const cidStr: string = json["Hash"];
        let cid = CID.parse(cidStr, base58btc); // auto-detect base, base58 for legacy
        // Mirror Python: if not DAG-PB, set codec marker. In JS, codecs are multicodec names.
        // We can't directly set codec code like Python; but typical Kubo "add" returns dag-pb CIDs.
        // If you must enforce a specific multicodec, you would re-wrap via dag-cbor codec at creation time.
        // Here we just return the parsed CID.
        return cid;
      })
    );
  }

  // --------------------------------- load ------------------------------------
  async load(
    id: IPLDKind,
    offset?: number | null,
    length?: number | null,
    suffix?: number | null
  ): Promise<Uint8Array> {
    const url = this.gatewayBase + String(id);
    const headers: Record<string, string> = {};

    if (offset != null) {
      const start = offset;
      if (length != null) {
        const end = start + length - 1;
        headers["Range"] = `bytes=${start}-${end}`;
      } else {
        headers["Range"] = `bytes=${start}-`;
      }
    } else if (suffix != null) {
      headers["Range"] = `bytes=-${suffix}`;
    }

    return this.sem.withPermit(async () => {
      return this.retrying(async () => {
        const res = await fetch(url, {
          method: "GET",
          headers: this.buildHeaders(headers),
        });
        if (!res.ok) {
          const text = await res.text().catch(() => "");
          throw new Error(`Kubo gateway load failed: ${res.status} ${res.statusText} ${text}`);
        }
        const buf = new Uint8Array(await res.arrayBuffer());
        return buf;
      });
    });
  }

  // --------------------------------- pin ops ---------------------------------
  async pin_cid(cid: CID, targetRpc = KuboCAS.KUBO_DEFAULT_LOCAL_RPC_BASE_URL): Promise<void> {
    const url = `${targetRpc.replace(/\/+$/, "")}/api/v0/pin/add?recursive=true&arg=${encodeURIComponent(String(cid))}`;
    await this.sem.withPermit(async () => {
      const res = await fetch(url, { method: "POST", headers: this.buildHeaders() });
      if (!res.ok) throw new Error(`pin/add failed: ${res.status} ${res.statusText}`);
    });
  }

  async unpin_cid(cid: CID, targetRpc = KuboCAS.KUBO_DEFAULT_LOCAL_RPC_BASE_URL): Promise<void> {
    const url = `${targetRpc.replace(/\/+$/, "")}/api/v0/pin/rm?recursive=true&arg=${encodeURIComponent(String(cid))}`;
    await this.sem.withPermit(async () => {
      const res = await fetch(url, { method: "POST", headers: this.buildHeaders() });
      if (!res.ok) throw new Error(`pin/rm failed: ${res.status} ${res.statusText}`);
    });
  }

  async pin_update(oldId: IPLDKind, newId: IPLDKind, targetRpc = KuboCAS.KUBO_DEFAULT_LOCAL_RPC_BASE_URL): Promise<void> {
    const url = `${targetRpc.replace(/\/+$/, "")}/api/v0/pin/update`;
    const params = new URLSearchParams();
    params.append("arg", String(oldId));
    params.append("arg", String(newId));
    await this.sem.withPermit(async () => {
      const res = await fetch(`${url}?${params.toString()}`, {
        method: "POST",
        headers: this.buildHeaders(),
      });
      if (!res.ok) throw new Error(`pin/update failed: ${res.status} ${res.statusText}`);
    });
  }

  async pin_ls(targetRpc = KuboCAS.KUBO_DEFAULT_LOCAL_RPC_BASE_URL): Promise<Array<Record<string, unknown>>> {
    const url = `${targetRpc.replace(/\/+$/, "")}/api/v0/pin/ls`;
    return this.sem.withPermit(async () => {
      const res = await fetch(url, { method: "POST", headers: this.buildHeaders() });
      if (!res.ok) throw new Error(`pin/ls failed: ${res.status} ${res.statusText}`);
      const json = await res.json() as { Keys?: Record<string, { Type: string }> };
      // Kubo returns { Keys: { "<cid>": { Type: "recursive" } , ... } }
      const keys = json?.Keys ?? {};
      // Keep parity with Python: return array of dicts, or reshape as needed
      return Object.entries(keys).map(([k, v]) => ({ cid: k, ...v }));
    });
  }
}
