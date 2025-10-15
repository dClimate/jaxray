import { CID } from "multiformats/cid";
export type IPLDKind = CID | string | number | boolean | Uint8Array;
export type CodecInput = "raw" | "dag-cbor";
export declare abstract class ContentAddressedStore {
    abstract save(data: Uint8Array, codec: CodecInput): Promise<IPLDKind>;
    abstract load(id: IPLDKind, offset?: number | null, length?: number | null, suffix?: number | null): Promise<Uint8Array>;
    pin_cid(_id: IPLDKind, _target_rpc: string): Promise<void>;
    unpin_cid(_id: IPLDKind, _target_rpc: string): Promise<void>;
    pin_update(_oldId: IPLDKind, _newId: IPLDKind, _target_rpc: string): Promise<void>;
    pin_ls(_target_rpc: string): Promise<Array<Record<string, unknown>>>;
}
export interface KuboCASOptions {
    hasher?: string;
    rpcBaseUrl?: string | null;
    gatewayBaseUrl?: string | null;
    concurrency?: number;
    headers?: Record<string, string>;
    auth?: {
        username: string;
        password: string;
    } | null;
    pinOnAdd?: boolean;
    chunker?: string;
    maxRetries?: number;
    initialDelay?: number;
    backoffFactor?: number;
    fetchImpl?: typeof fetch;
}
export declare class KuboCAS extends ContentAddressedStore {
    static readonly KUBO_DEFAULT_LOCAL_GATEWAY_BASE_URL = "http://127.0.0.1:8080";
    static readonly KUBO_DEFAULT_LOCAL_RPC_BASE_URL = "http://127.0.0.1:5001";
    private static readonly DAG_PB_MARKER;
    private readonly hasher;
    private readonly rpcUrl;
    private readonly gatewayBase;
    private readonly sem;
    private readonly headers?;
    private readonly authHeader?;
    private readonly maxRetries;
    private readonly initialDelay;
    private readonly backoffFactor;
    private readonly fetchImpl;
    constructor(opts?: KuboCASOptions);
    private buildHeaders;
    private retrying;
    save(data: Uint8Array, codec: CodecInput): Promise<CID>;
    load(id: IPLDKind, offset?: number | null, length?: number | null, suffix?: number | null): Promise<Uint8Array>;
    pin_cid(cid: CID, targetRpc?: string): Promise<void>;
    unpin_cid(cid: CID, targetRpc?: string): Promise<void>;
    pin_update(oldId: IPLDKind, newId: IPLDKind, targetRpc?: string): Promise<void>;
    pin_ls(targetRpc?: string): Promise<Array<Record<string, unknown>>>;
}
//# sourceMappingURL=ipfs-gateway.d.ts.map