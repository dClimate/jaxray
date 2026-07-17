import { afterEach, describe, expect, test, vi } from "vitest";

import { KuboCAS, type KuboCASOptions } from "../../src/backends/ipfs/ipfs-gateway";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

const CHUNK_CID = "bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const GATEWAY_URL = `http://gw.test/ipfs/${CHUNK_CID}`;

describe("KuboCAS gateway HTTP/2 transport options", () => {
  test("load uses the configured fetchFn instead of global fetch", async () => {
    const payload = Uint8Array.from([1, 2, 3]);
    const fetchFn = vi.fn(async () => new Response(payload.slice(), { status: 200 }));
    const globalFetch = vi.fn(async () => new Response(payload.slice(), { status: 200 }));
    vi.stubGlobal("fetch", globalFetch);

    const cas = new KuboCAS({
      gatewayBaseUrl: "http://gw.test",
      rpcBaseUrl: null,
      fetchFn,
    } as unknown as KuboCASOptions);

    await cas.load(CHUNK_CID);

    expect(fetchFn).toHaveBeenCalledWith(GATEWAY_URL, expect.anything());
    expect(globalFetch).not.toHaveBeenCalled();
  });

  test("load passes the configured dispatcher through to fetch", async () => {
    const dispatcher = { __sentinel: "dispatcher" };
    const fetchMock = vi.fn(async () => new Response(Uint8Array.of(1), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const cas = new KuboCAS({
      gatewayBaseUrl: "http://gw.test",
      rpcBaseUrl: null,
      dispatcher,
    } as unknown as KuboCASOptions);

    await cas.load(CHUNK_CID);

    expect(fetchMock).toHaveBeenCalledWith(
      GATEWAY_URL,
      expect.objectContaining({ dispatcher })
    );
  });
});
