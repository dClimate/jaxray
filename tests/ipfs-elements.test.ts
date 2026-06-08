import { afterEach, describe, expect, test, vi } from "vitest";
import { createIpfsElements } from "../src/backends/ipfs/ipfs-elements";
import { KuboCAS } from "../src/backends/ipfs/ipfs-gateway";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("createIpfsElements", () => {
  test("passes unixfs.cat range options to the gateway loader", async () => {
    const bytes = new Uint8Array([1, 2, 3]);
    const loadSpy = vi.spyOn(KuboCAS.prototype, "load").mockResolvedValue(bytes);
    const ipfsElements = createIpfsElements("https://example.com");

    const chunks: Uint8Array[] = [];
    for await (const chunk of ipfsElements.unixfs.cat("bafytest", { offset: 10, length: 3 })) {
      chunks.push(chunk);
    }

    expect(chunks).toEqual([bytes]);
    expect(loadSpy).toHaveBeenCalledWith("bafytest", 10, 3);
  });
});
