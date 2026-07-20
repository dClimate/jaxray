import { afterEach, describe, expect, test, vi } from "vitest";

import { KuboCAS } from "../../src/backends/ipfs/ipfs-gateway";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

const CHUNK_CID = "bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

describe("KuboCAS retries transient gateway HTTP errors", () => {
  test("load retries HTTP 500 responses and eventually returns the payload", async () => {
    const payload = Uint8Array.from([1, 2, 3, 4]);
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("boom", { status: 500, statusText: "Internal Server Error" }))
      .mockResolvedValueOnce(new Response("boom", { status: 500, statusText: "Internal Server Error" }))
      .mockResolvedValueOnce(new Response(payload.slice(), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const cas = new KuboCAS({
      gatewayBaseUrl: "http://gw.test",
      rpcBaseUrl: null,
      maxRetries: 3,
      initialDelay: 0.001,
    });

    await expect(cas.load(CHUNK_CID)).resolves.toEqual(payload);
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });
});
