/**
 * In-memory Zarr store for testing
 */

import { ZarrStore } from '../../src/backends/zarr';

export class MemoryZarrStore implements ZarrStore {
  private data: Map<string, Uint8Array>;

  constructor(initialData: { [key: string]: any } = {}) {
    this.data = new Map();

    // Convert initial data to Uint8Array
    for (const [key, value] of Object.entries(initialData)) {
      if (value instanceof Uint8Array) {
        this.data.set(key, value);
      } else if (typeof value === 'object') {
        // Auto-add required zarrita metadata for arrays
        const enrichedValue = this.enrichMetadata(value, key);
        this.data.set(key, new TextEncoder().encode(JSON.stringify(enrichedValue)));
      } else if (typeof value === 'string') {
        this.data.set(key, new TextEncoder().encode(value));
      }
    }
  }

  private enrichMetadata(meta: any, key: string): any {
    if (meta.node_type === 'array') {
      return {
        zarr_format: 3,
        node_type: 'array',
        shape: meta.shape || [],
        data_type: meta.data_type || 'float64',
        chunk_grid: meta.chunk_grid || {
          name: 'regular',
          configuration: { chunk_shape: meta.shape || [1] }
        },
        chunk_key_encoding: meta.chunk_key_encoding || {
          name: 'default',
          configuration: { separator: '/' }
        },
        fill_value: meta.fill_value !== undefined ? meta.fill_value : 0,
        codecs: meta.codecs || [{
          name: 'bytes',
          configuration: { endian: 'little' }
        }],
        dimension_names: meta.dimension_names,
        attributes: meta.attributes || {}
      };
    }
    return meta;
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    // Normalize key - zarrita may request with leading slash
    const normalizedKey = key.startsWith('/') ? key.slice(1) : key;
    return this.data.get(normalizedKey) || this.data.get(key);
  }

  async has(key: string): Promise<boolean> {
    return this.data.has(key);
  }

  listMetadataKeys(): string[] {
    return Array.from(this.data.keys()).filter(k => k.endsWith('.json'));
  }

  // Helper method for tests to add data
  set(key: string, value: any): void {
    if (value instanceof Uint8Array) {
      this.data.set(key, value);
    } else if (typeof value === 'object') {
      this.data.set(key, new TextEncoder().encode(JSON.stringify(value)));
    } else if (typeof value === 'string') {
      this.data.set(key, new TextEncoder().encode(value));
    }
  }
}
