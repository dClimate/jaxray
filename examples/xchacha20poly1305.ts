import { xchacha20poly1305 } from '@noble/ciphers/chacha.js';
import { Dataset, registerXChaCha20Poly1305Codec } from '../src';
import { ZarrBackend, type ZarrStore } from '../src/backends/zarr';

class InMemoryStore implements ZarrStore {
  private readonly data = new Map<string, Uint8Array>();

  constructor(initial: Record<string, any> = {}) {
    for (const [key, value] of Object.entries(initial)) {
      this.set(key, value);
    }
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    // Normalize key by removing leading slash
    const normalizedKey = key.startsWith('/') ? key.slice(1) : key;
    return this.data.get(normalizedKey);
  }

  async has(key: string): Promise<boolean> {
    // Normalize key by removing leading slash
    const normalizedKey = key.startsWith('/') ? key.slice(1) : key;
    return this.data.has(normalizedKey);
  }

  listMetadataKeys(): string[] {
    return Array.from(this.data.keys()).filter(key => key.endsWith('zarr.json'));
  }

  set(key: string, value: Uint8Array | Record<string, unknown>): void {
    if (value instanceof Uint8Array) {
      this.data.set(key, value);
      return;
    }
    const json = JSON.stringify(value);
    this.data.set(key, new TextEncoder().encode(json));
  }
}

async function main(): Promise<void> {
  const keyHex = '00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff';
  const nonce = Uint8Array.from({ length: 24 }, (_, i) => i + 1);
  const values = [10, 20, 30, 40];

  registerXChaCha20Poly1305Codec({
    getKey: () => keyHex,
    nonceGenerator: () => nonce
  });

  const chunk = buildEncryptedChunk(keyHex, nonce, values);

  console.log('Built encrypted chunk:', chunk);

  const store = new InMemoryStore({
    'zarr.json': {
      zarr_format: 3,
      node_type: 'group',
      attributes: {}
    },
    'temperature/zarr.json': {
      zarr_format: 3,
      node_type: 'array',
      shape: [values.length],
      data_type: 'float64',
      dimension_names: ['time'],
      chunk_grid: {
        name: 'regular',
        configuration: { chunk_shape: [values.length] }
      },
      chunk_key_encoding: {
        name: 'default',
        configuration: { separator: '/' }
      },
      fill_value: 0,
      codecs: [
        {
          name: 'xchacha20poly1305',
          // configuration: { header }
        },
        {
          name: 'bytes',
          configuration: { endian: 'little' }
        }
      ]
    }
  });

  store.set('temperature/c/0', chunk);

  const dataset: Dataset = await ZarrBackend.open(store);
  const variable = dataset.getVariable('temperature');
  const eager = await variable.compute();

  console.log('Decrypted data:', eager.data);
}

function buildEncryptedChunk(
  keyHex: string,
  nonce: Uint8Array,
  values: number[]
): Uint8Array {
  const keyBytes = hexToBytes(keyHex);
  const plain = new Float64Array(values);
  const cipher = xchacha20poly1305(keyBytes, nonce);
  const ciphertext = cipher.encrypt(new Uint8Array(plain.buffer.slice(0)));

  const chunk = new Uint8Array(nonce.length + ciphertext.length);
  chunk.set(nonce, 0);
  chunk.set(ciphertext, nonce.length);
  return chunk;
}

function hexToBytes(hex: string): Uint8Array {
  const normalized = hex.startsWith('0x') ? hex.slice(2) : hex;
  const bytes = new Uint8Array(normalized.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(normalized.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

main().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
