import { afterEach, describe, expect, test, vi } from "vitest";

import { KuboCAS } from "../../src/backends/ipfs/ipfs-gateway";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

const CHUNK_CID = "bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

describe("BUG 4: KuboCAS.load returns the FULL body when a gateway ignores the Range header", () => {
  test("load(cid, offset, length) yields exactly `length` bytes even on a 200 response", async () => {
    const fullBody = Uint8Array.from({ length: 100 }, (_, i) => i);
    // A gateway that ignores Range and replies 200 with the whole object —
    // permitted by HTTP (a server MAY ignore Range; 200 means full representation).
    vi.stubGlobal("fetch", vi.fn(async () => new Response(fullBody.slice(), { status: 200 })));

    const cas = new KuboCAS({ gatewayBaseUrl: "http://gw.test", rpcBaseUrl: null });
    const out = await cas.load(CHUNK_CID, 5, 3);

    // Correct behavior: the client must detect status 200 (not 206) and slice
    // (or error). Current code returns all 100 bytes as if they were the range,
    // which corrupts every downstream ranged read (HamtStore.getRange, unixfs.cat
    // with offset/length).
    expect(out).toEqual(fullBody.slice(5, 8));
  });
});
