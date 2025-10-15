/**
 * jaxray - A JavaScript implementation similar to xarray
 *
 * This library provides labeled, multi-dimensional arrays for JavaScript,
 * inspired by Python's xarray library.
 */
export { DataArray } from './DataArray';
export { Dataset } from './Dataset';
export * from './types';
export * from './utils';
export * from './cf-time';
export { ZarrBackend, type ZarrStore } from './backends/zarr';
export { ShardedStore } from './backends/ipfs/sharded-store';
export { createIpfsElements } from './backends/ipfs/ipfs-elements';
export { KuboCAS } from './backends/ipfs/ipfs-gateway';
export declare const VERSION = "1.0.0";
//# sourceMappingURL=index.d.ts.map