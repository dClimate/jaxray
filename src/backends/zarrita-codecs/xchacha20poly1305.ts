import { xchacha20poly1305 } from '@noble/ciphers/chacha.js';
import { registry } from 'zarrita';

export type XChaCha20CodecConfig = Record<string, unknown>;

export const XCHACHA20POLY1305_CODEC_ID = 'xchacha20poly1305';
const NONCE_LENGTH = 24; // bytes
const TAG_LENGTH = 16; // bytes

export interface CodecRegistrationOptions {
  getKey: (config: XChaCha20CodecConfig) => Promise<string | Uint8Array> | string | Uint8Array;
  getHeader?: (config: XChaCha20CodecConfig) => Promise<string | Uint8Array | undefined> | string | Uint8Array | undefined;
  nonceGenerator?: () =>
    | Promise<Uint8Array | ArrayBuffer | ArrayBufferView | number[]>
    | Uint8Array
    | ArrayBuffer
    | ArrayBufferView
    | number[];
}

interface CodecContext {
  config: XChaCha20CodecConfig;
  resolveKey: (config: XChaCha20CodecConfig) => Promise<Uint8Array>;
  resolveHeader: (config: XChaCha20CodecConfig) => Promise<Uint8Array>;
  nonceGenerator: () => Promise<Uint8Array>;
}

class XChaCha20Poly1305Codec {
  public readonly kind = 'bytes_to_bytes' as const;

  private readonly context: CodecContext;
  private cachedKey?: Promise<{ key: Uint8Array }>;

  constructor(context: CodecContext) {
    this.context = context;
  }

  async encode(data: Uint8Array): Promise<Uint8Array> {
    const { key } = await this.getKey();
    const nonce = await this.generateNonce();
    const cipher = xchacha20poly1305(key, nonce);
    const ciphertext = cipher.encrypt(data);
  
    const output = new Uint8Array(nonce.length + ciphertext.length);
    output.set(nonce, 0);
    output.set(ciphertext, nonce.length);
    return output;
  }

  async decode(data: Uint8Array, out?: Uint8Array): Promise<Uint8Array> {
    if (data.length < NONCE_LENGTH + TAG_LENGTH) {
      throw new Error(`Encrypted chunk too small: expected at least ${NONCE_LENGTH + TAG_LENGTH} bytes`);
    }

    const { key } = await this.getKey();
    const nonce = data.subarray(0, NONCE_LENGTH);
    const ciphertext = data.subarray(NONCE_LENGTH);
    const cipher = xchacha20poly1305(key, nonce);
    const plaintext = cipher.decrypt(ciphertext);

    if (out) {
      if (out.length !== plaintext.length) {
        throw new Error('Output buffer length does not match decrypted payload length');
      }
      out.set(plaintext);
      return out;
    }

    return plaintext;
  }

  private async getKey(): Promise<{ key: Uint8Array }> {
    if (!this.cachedKey) {
      this.cachedKey = this.context.resolveKey(this.context.config).then(key => ({ key }));
    }
    return this.cachedKey;
  }

  private async generateNonce(): Promise<Uint8Array> {
    const nonce = await this.context.nonceGenerator();
    if (nonce.length !== NONCE_LENGTH) {
      throw new Error(`Nonce generator must return ${NONCE_LENGTH} bytes; received ${nonce.length}`);
    }
    return nonce;
  }
}

export function registerXChaCha20Poly1305Codec(options: CodecRegistrationOptions): void {
  const { getKey, getHeader, nonceGenerator } = options;

  if (typeof getKey !== 'function') {
    throw new Error('registerXChaCha20Poly1305Codec requires a getKey callback');
  }

  const resolveKey = async (config: XChaCha20CodecConfig): Promise<Uint8Array> => {
    const key = await getKey(config);
    return normalizeKeyBytes(key);
  };

  const resolveHeader = async (config: XChaCha20CodecConfig): Promise<Uint8Array> => {
    const headerValue = await (getHeader?.(config) ?? extractHeader(config));
    if (headerValue === undefined) {
      return new Uint8Array();
    }
    if (headerValue instanceof Uint8Array) {
      return headerValue;
    }
    if (typeof headerValue === 'string') {
      return new TextEncoder().encode(headerValue);
    }
    throw new Error('Header must be a string or Uint8Array');
  };

  const resolveNonce = async (): Promise<Uint8Array> => {
    const generated = await (typeof nonceGenerator === 'function'
      ? nonceGenerator()
      : nonceGenerator ?? generateDefaultNonce());
    if (generated instanceof Uint8Array) {
      return generated;
    }
    if (generated instanceof ArrayBuffer) {
      return new Uint8Array(generated);
    }
    if (ArrayBuffer.isView(generated)) {
      const view = generated as ArrayBufferView;
      return new Uint8Array(view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength));
    }
    if (Array.isArray(generated)) {
      return Uint8Array.from(generated);
    }
    throw new Error('Nonce generator must return a Uint8Array-compatible value');
  };

  registry.set(XCHACHA20POLY1305_CODEC_ID, async () => ({
    kind: 'bytes_to_bytes' as const,
    fromConfig: (config: unknown, _meta: any) => {
      const normalizedConfig = normalizeConfig(config);
      return new XChaCha20Poly1305Codec({
        config: normalizedConfig,
        resolveKey,
        resolveHeader,
        nonceGenerator: resolveNonce
      });
    }
  }));
}

function extractHeader(config: XChaCha20CodecConfig): string | Uint8Array | undefined {
  const header = (config as { header?: unknown }).header;
  if (header === undefined || header === null) {
    return undefined;
  }
  if (typeof header === 'string' || header instanceof Uint8Array) {
    return header;
  }
  throw new Error('Codec configuration header must be a string or Uint8Array');
}

function normalizeKeyBytes(key: string | Uint8Array): Uint8Array {
  if (key instanceof Uint8Array) {
    if (key.length !== 32) {
      throw new Error(`Encryption key must be 32 bytes; received ${key.length}`);
    }
    return key;
  }

  if (typeof key === 'string') {
    const normalized = key.startsWith('0x') ? key.slice(2) : key;
    if (normalized.length !== 64) {
      throw new Error(`Encryption key must be 64 hex characters; received ${normalized.length}`);
    }
    if (!/^[0-9a-fA-F]+$/.test(normalized)) {
      throw new Error('Encryption key must be a hex-encoded string');
    }
    return hexToBytes(normalized);
  }

  throw new Error('Unsupported key type provided to registerXChaCha20Poly1305Codec');
}

function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

function normalizeConfig(config: unknown): XChaCha20CodecConfig {
  if (config && typeof config === 'object') {
    return config as XChaCha20CodecConfig;
  }
  return {};
}

interface CryptoLike {
  getRandomValues?: (buffer: Uint8Array) => void;
}

async function generateDefaultNonce(): Promise<Uint8Array> {
  const cryptoObj = (globalThis as { crypto?: CryptoLike }).crypto;
  if (cryptoObj?.getRandomValues) {
    const buffer = new Uint8Array(NONCE_LENGTH);
    cryptoObj.getRandomValues(buffer);
    return buffer;
  }

  const buffer = new Uint8Array(NONCE_LENGTH);
  for (let i = 0; i < NONCE_LENGTH; i++) {
    buffer[i] = Math.floor(Math.random() * 256);
  }
  return buffer;
}
