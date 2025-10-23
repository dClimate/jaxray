/**
 * jaxray - A JavaScript implementation similar to xarray
 *
 * This library provides labeled, multi-dimensional arrays for JavaScript,
 * inspired by Python's xarray library.
 */

export { DataArray } from './DataArray.js';
export { Dataset } from './Dataset.js';
export * from './types.js';
export * from './utils.js';
export * from './cf-time.js';
export type { WhereOptions, BinaryOpOptions } from './ops/where.js';
export {
  createEagerBlock,
  createLazyBlock,
  isLazyBlock
} from './core/data-block.js';
export type { DataBlock, DataBlockKind } from './core/data-block.js';

// Backends
export { ZarrBackend, type ZarrStore } from './backends/zarr.js';
export { ShardedStore } from './backends/ipfs/sharded-store.js';
export { HamtStore } from './backends/ipfs/hamt-store.js';
export { createIpfsElements } from './backends/ipfs/ipfs-elements.js';
export type { IPFSELEMENTS_INTERFACE } from './backends/ipfs/ipfs-elements.js';
export { openIpfsStore, detectIpfsStoreType } from './backends/ipfs/open-store.js';
export type { OpenStoreOptions } from './backends/ipfs/open-store.js';
export { KuboCAS } from './backends/ipfs/ipfs-gateway.js';

// Version
export const VERSION = '0.2.1';
