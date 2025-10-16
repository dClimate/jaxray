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
export { ZarrBackend, type ZarrStore } from './backends/zarr.js';
export { ShardedStore } from './backends/ipfs/sharded-store.js';
export { createIpfsElements } from './backends/ipfs/ipfs-elements.js';
export { KuboCAS } from './backends/ipfs/ipfs-gateway.js';
export declare const VERSION = "1.0.0";
//# sourceMappingURL=index.d.ts.map