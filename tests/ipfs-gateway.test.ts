import { afterEach, describe, expect, test, vi } from "vitest";
import { KuboCAS } from "../src/backends/ipfs/ipfs-gateway";

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
  vi.restoreAllMocks();
});

describe("KuboCAS", () => {
  test("retries Node fetch failures with nested timeout causes", async () => {
    const timeout = new TypeError("fetch failed", {
      cause: new AggregateError([
        Object.assign(new Error("connect ETIMEDOUT 203.0.113.1:443"), {
          code: "ETIMEDOUT",
        }),
      ]),
    });
    const ok = new Response(new Uint8Array([1, 2, 3]));
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockRejectedValueOnce(timeout)
      .mockResolvedValueOnce(ok);
    globalThis.fetch = fetchMock;

    const gateway = new KuboCAS({
      gatewayBaseUrl: "https://gateway.example",
      rpcBaseUrl: null,
      maxRetries: 1,
      initialDelay: 0.001,
    });

    const bytes = await gateway.load("bafytest");

    expect(bytes).toEqual(new Uint8Array([1, 2, 3]));
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});
