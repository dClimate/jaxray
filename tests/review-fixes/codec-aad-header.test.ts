import { describe, test, expect } from "vitest";
import { xchacha20poly1305 } from "@noble/ciphers/chacha.js";
import { registry } from "zarrita";

import {
  registerXChaCha20Poly1305Codec,
  XCHACHA20POLY1305_CODEC_ID,
} from "../../src/backends/zarrita-codecs/xchacha20poly1305";

describe("BUG 5: xchacha20poly1305 codec ignores the configured header/associated data", () => {
  const KEY_HEX = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
  const NONCE = Uint8Array.from({ length: 24 }, (_, i) => i + 1);
  const HEADER = "dclimate-zarr-header";

  const keyBytes = () => {
    const bytes = new Uint8Array(32);
    for (let i = 0; i < 32; i++) bytes[i] = parseInt(KEY_HEX.slice(i * 2, i * 2 + 2), 16);
    return bytes;
  };

  async function makeCodec() {
    registerXChaCha20Poly1305Codec({
      getKey: () => KEY_HEX,
      getHeader: () => HEADER,
      nonceGenerator: () => NONCE,
    });
    const factory = registry.get(XCHACHA20POLY1305_CODEC_ID)! as any;
    const mod = await factory();
    return mod.fromConfig({ header: HEADER }, {} as any);
  }

  test("decodes chunks encrypted with the header as AAD (py-hamt interop)", async () => {
    const plaintext = new TextEncoder().encode("plaintext-chunk");
    const aad = new TextEncoder().encode(HEADER);
    const ciphertext = xchacha20poly1305(keyBytes(), NONCE, aad).encrypt(plaintext);
    const chunk = new Uint8Array(NONCE.length + ciphertext.length);
    chunk.set(NONCE, 0);
    chunk.set(ciphertext, NONCE.length);

    const codec = await makeCodec();
    // Correct behavior: the header supplied via getHeader/config is the AEAD
    // associated data, so this must decrypt. Current code never passes the AAD
    // to xchacha20poly1305() and fails tag authentication.
    await expect(codec.decode(chunk)).resolves.toEqual(plaintext);
  });

  test("encode authenticates the header so AAD-aware readers can decrypt", async () => {
    const plaintext = new TextEncoder().encode("plaintext-chunk");
    const codec = await makeCodec();
    const encoded = await codec.encode(plaintext);

    const aad = new TextEncoder().encode(HEADER);
    const decrypted = xchacha20poly1305(
      keyBytes(),
      encoded.subarray(0, 24),
      aad,
    ).decrypt(encoded.subarray(24)); // throws if the header was not authenticated
    expect(new Uint8Array(decrypted)).toEqual(plaintext);
  });
});
