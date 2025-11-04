import { describe, expect, test } from 'vitest';
import { xchacha20poly1305 } from '@noble/ciphers/chacha.js';
import { registerXChaCha20Poly1305Codec } from '../../src';
import { MemoryZarrStore } from '../helpers/MemoryZarrStore';
import { ZarrBackend } from '../../src/backends/zarr';

const KEY_HEX = '00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff';
const NONCE = Uint8Array.from({ length: 24 }, (_, i) => i + 1);
const DATA_VALUES = [1.5, -2.5, 3.25, 4.75];

function buildEncryptedChunk(): Uint8Array {
  const keyBytes = hexToBytes(KEY_HEX);
  const plain = new Float64Array(DATA_VALUES);
  const plainBytes = new Uint8Array(plain.buffer.slice(0));
  const cipher = xchacha20poly1305(keyBytes, NONCE);
  const ciphertext = cipher.encrypt(plainBytes);
  const chunk = new Uint8Array(NONCE.length + ciphertext.length);
  chunk.set(NONCE, 0);
  chunk.set(ciphertext, NONCE.length);
  return chunk;
}

function createEncryptedStore(): MemoryZarrStore {
  const store = new MemoryZarrStore({
    'zarr.json': { node_type: 'group', attributes: {} },
    'data/zarr.json': {
      node_type: 'array',
      shape: [DATA_VALUES.length],
      data_type: 'float64',
      dimension_names: ['x'],
      codecs: [
        {
          name: 'xchacha20poly1305',
          // configuration: { header: HEADER }
        }
      ]
    }
  });

  store.set('data/c/0', buildEncryptedChunk());
  return store;
}

function hexToBytes(hex: string): Uint8Array {
  const normalized = hex.startsWith('0x') ? hex.slice(2) : hex;
  const bytes = new Uint8Array(normalized.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(normalized.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

describe('xchacha20poly1305 codec integration', () => {
  test('decodes encrypted chunks when registered with the correct key', async () => {
    registerXChaCha20Poly1305Codec({
      getKey: () => KEY_HEX,
      // getHeader: () => HEADER,
      nonceGenerator: () => NONCE
    });

    const dataset = await ZarrBackend.open(createEncryptedStore());
    expect(dataset.isEncrypted).toBe(true);
    const variable = dataset.getVariable('data');

    const eager = await variable.compute();
    expect(eager.data).toEqual(DATA_VALUES);
  });

  test('throws when decrypting with an incorrect key', async () => {
    registerXChaCha20Poly1305Codec({
      getKey: () => 'ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100',
      nonceGenerator: () => NONCE
    });

    const dataset = await ZarrBackend.open(createEncryptedStore());
    expect(dataset.isEncrypted).toBe(true);
    const variable = dataset.getVariable('data');

    await expect(variable.compute()).rejects.toThrow(/tag|auth/i);
  });
});
