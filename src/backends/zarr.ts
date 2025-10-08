// backends/zarr.ts
import * as zarr from "zarrita";
import { Dataset } from "../Dataset";
import { DataArray } from "../DataArray";
import { reshape } from "../utils";
import { DataValue } from "../types";

// Types you already declared:
export interface ZarrStore {
  get(key: string): Promise<Uint8Array | undefined>;
  has?(key: string): Promise<boolean>;
  // Optional discovery (needed for sharded store)
  listMetadataKeys?(): string[];
}

export interface ZarrMetadata {
  [key: string]: {
    shape: number[];
    dimension_names?: string[];
    attributes?: { [key: string]: any };
    chunk_grid?: { configuration: { chunk_shape: number[] } };
    node_type?: "array" | "group";
    data_type?: string;
    // ... other v3 bits
  };
}

type OpenOptions = {
  group?: string;
  consolidated?: boolean;
};

function lastSegment(path: string): string {
  const p = path.replace(/^\/+/, "").replace(/\/+$/, "");
  const segs = p.split("/");
  return segs[segs.length - 1] || "";
}

function dirname(path: string): string {
  const p = path.replace(/^\/+/, "").replace(/\/+$/, "");
  const idx = p.lastIndexOf("/");
  return idx === -1 ? "" : p.slice(0, idx);
}


export class ZarrBackend {
  /**
   * Open a Zarr store as a Dataset
   * @param store - A ZarrStore implementation (e.g., ShardedStore, S3Store, LocalStore)
   * @param options - Options including group path
   */
  static async open(store: ZarrStore, options: OpenOptions = {}): Promise<Dataset> {
    const { group = "" } = options;

    // ---- Discover array and group nodes via metadata keys ----
    // Expect zarr v3 "zarr.json" files. Your ShardedStore exposes them via metadata map.
    const listKeys = typeof store.listMetadataKeys === "function"
      ? await Promise.resolve(store.listMetadataKeys())
      : [];

    if (!listKeys || listKeys.length === 0) {
      throw new Error(
        "ZarrBackend.open: unable to discover any metadata keys. Ensure the store implements listMetadataKeys()."
      );
    }

    // Keep only keys under the requested group (prefix match) and ending with zarr.json
    const normalizedGroup = group.replace(/^\/+/, "").replace(/\/+$/, "");
    const jsonKeys = listKeys
      .filter((k) => k.endsWith("zarr.json"))
      .filter((k) =>
        normalizedGroup ? k === `${normalizedGroup}/zarr.json` || k.startsWith(`${normalizedGroup}/`) : true
      );

    if (jsonKeys.length === 0) {
      throw new Error(
        `ZarrBackend.open: no zarr.json under group "${normalizedGroup || "/"}".`
      );
    }

    // ---- Parse node metadata & pick arrays ----
    // Map: arrayPath -> meta
    const arrayMetas: Map<string, any> = new Map();

    for (const key of jsonKeys) {
      const bytes = await store.get(key);
      if (!bytes) continue;

      let meta: any;
      try {
        meta = JSON.parse(new TextDecoder().decode(bytes));
      } catch (e) {
        // Some implementations DAG-CBOR the zarr.json; try a safe CBOR decode if needed
        // but typically zarr.json is JSON text.
        continue;
      }

      const nodeType = meta?.node_type;
      const path = dirname(key); // array/group path

      if (nodeType === "array") {
        arrayMetas.set(path, meta);
      }
      // (For groups, we don’t need to do anything special here.)
    }

    if (arrayMetas.size === 0) {
      throw new Error(
        `ZarrBackend.open: found zarr.json files, but none were arrays under "${normalizedGroup || "/"}".`
      );
    }

    // ---- Prepare array metadata (lazy loading - don't load data yet) ----
    const arrayMetadata: Array<{
      path: string;
      name: string;
      meta: any;
      dims: string[];
      attrs: Record<string, any>;
      shape: number[];
    }> = [];

    for (const [path, meta] of arrayMetas.entries()) {
      const name = lastSegment(path);

      const dims =
        Array.isArray(meta?.dimension_names) && meta.dimension_names.length === meta.shape?.length
          ? [...meta.dimension_names]
          : // fallback dimension names if not present
            meta?.shape?.map((_: number, i: number) => `dim_${i}`) ?? [];

      const attrs = meta?.attributes ?? {};
      const shape = meta?.shape ?? [];

      arrayMetadata.push({ path, name, meta, dims, attrs, shape });
    }

    // ---- Heuristic: identify coordinate variables ----
    // Infer global dims from any non-1D arrays or arrays with attributes marking them as data
    const dataLike = arrayMetadata.filter((a) => a.shape.length !== 1);
    const hasDataVars = dataLike.length > 0;
    const globalDims = new Set<string>();
    for (const arr of dataLike) arr.dims.forEach((d) => globalDims.add(d));

    // Finalize coordinates: 1D arrays whose name matches a dimension used somewhere (or name == its own dim)
    const coordNames = new Set<string>();
    for (const arr of arrayMetadata) {
      const is1D = arr.shape.length === 1;
      const nameEqualsDim = is1D && arr.dims.length === 1 && arr.name === arr.dims[0];
      if (is1D && (
          (hasDataVars && (globalDims.has(arr.name) || nameEqualsDim)) ||
          (!hasDataVars && nameEqualsDim) // pure-coords dataset
      )) {
        coordNames.add(arr.name);
      }
    }

    // ---- Open zarr group ----
    const zarrGroup = await zarr.open(store as any, { kind: "group" });

    // ---- Load coordinate arrays (eagerly - they're small and needed for selection) ----
    const coords: Record<string, any[]> = {};
    const coordAttrs: Record<string, Record<string, any>> = {}; // Store coordinate attributes

    for (const arr of arrayMetadata) {
      if (coordNames.has(arr.name)) {
        // Load coordinate data immediately using group.resolve()
        const coordArray = await zarr.open(zarrGroup.resolve(arr.name), { kind: "array" });

        const coordData = await zarr.get(coordArray);

        // Extract the actual values from zarrita result
        const values = coordData.data || coordData;

        // Convert to regular array
        const coordValues = Array.isArray(values) ? values : Array.from(values as any);
        coords[arr.name] = coordValues;

        // Store coordinate attributes (e.g., time units)
        coordAttrs[arr.name] = arr.attrs;
      }
    }

    // ---- Build DataArrays for data variables (lazy - store metadata only) ----
    // For now, just return the Dataset with variable info in attrs
    // Real DataArrays will be created on-demand when accessed
    const dataVars: Record<string, DataArray> = {};

    // Store array metadata for lazy loading later
    const lazyArrayInfo: Record<string, any> = {};

    for (const arr of arrayMetadata) {
      if (coordNames.has(arr.name)) continue; // skip coord arrays

      // Store metadata for lazy loading
      lazyArrayInfo[arr.name] = {
        path: arr.path,
        shape: arr.shape,
        dims: arr.dims,
        attrs: arr.attrs,
        store,
      };

      // Build per-dim coords view
      const perDimCoords: Record<string, any[]> = {};
      arr.dims.forEach((d, i) => {
        if (coords[d] && coords[d].length === arr.shape[i]) {
          perDimCoords[d] = coords[d];
        } else {
          // fallback to positional coords if no named coord exists yet
          perDimCoords[d] = Array.from({ length: arr.shape[i] }, (_, j) => j);
        }
      });

      // Create a lazy loader function that the DataArray can call
      const lazyLoader = async (indexRanges: { [dim: string]: { start: number; stop: number } | number }) => {
        const arrNode = await zarr.open(zarrGroup.resolve(arr.name), { kind: 'array'});
        // Build zarr selection
        const zarrSelection: (number | any | null)[] = [];
        for (const dim of arr.dims) {
          const range = indexRanges[dim];
          if (typeof range === 'number') {
            zarrSelection.push(range);
          } else if (range) {
            zarrSelection.push(zarr.slice(range.start, range.stop));
          } else {
            zarrSelection.push(null);
          }
        }

        // Get data from zarr
        const result = await zarr.get(arrNode, zarrSelection);

        // Calculate result shape
        const resultShape = arr.dims.map((dim, i) => {
          const range = indexRanges[dim];
          if (typeof range === 'number') return undefined; // Dimension dropped
          if (range) return range.stop - range.start;
          return arr.shape[i];
        }).filter(s => s !== undefined) as number[];

        // Handle scalar result (all dimensions were single indices)
        if (resultShape.length === 0) {
          return result.data !== undefined ? result.data : result;
        }

        // Handle array result
        const flatData = Array.from(result.data || result) as DataValue[];

        // Reshape to nested array using utility function
        return reshape(flatData, resultShape);
      };

      dataVars[arr.name] = new DataArray(null, {
        lazy: true,
        virtualShape: arr.shape,
        dims: arr.dims,  // Use real dims
        coords: perDimCoords,
        attrs: {
          ...arr.attrs,
          _zarr_path: arr.path,
          _zarr_shape: arr.shape,  // Store actual shape
          _zarr_coords: perDimCoords, // Store coords here
          _coordAttrs: coordAttrs, // Store coordinate attributes for time conversion
          _zarr_data_type: arr.meta.data_type, // Store data type for byte size calculation
          _lazy: true,
          _lazyLoader: lazyLoader, // Provide loader function
        },
        name: arr.name,
      });
    }

    // Edge case: dataset of only coordinate arrays
    if (Object.keys(dataVars).length === 0) {
      // promote 1D coords to data variables as a fallback
      for (const arr of arrayMetadata) {
        if (coordNames.has(arr.name)) {
          const d = arr.dims[0] ?? "dim_0";
          const placeholderData = Array.from({ length: arr.shape[0] }, (_, i) => i);
          dataVars[arr.name] = new DataArray(placeholderData, {
            dims: [d],
            coords: { [d]: coords[d] ?? placeholderData },
            attrs: { ...arr.attrs, _zarr_path: arr.path, _zarr_store: store },
            name: arr.name,
          });
        }
      }
    }

    // Dataset attrs: take group attrs if present; otherwise empty
    let datasetAttrs: Record<string, any> = {};
    const groupJsonKey = normalizedGroup ? `${normalizedGroup}/zarr.json` : "zarr.json";
    const groupJsonBytes = await store.get(groupJsonKey).catch(() => undefined);
    if (groupJsonBytes) {
      try {
        const gMeta = JSON.parse(new TextDecoder().decode(groupJsonBytes));
        if (gMeta?.attributes && typeof gMeta.attributes === "object") {
          datasetAttrs = gMeta.attributes;
        }
      } catch {}
    }

    return new Dataset(dataVars, {
      coords,
      attrs: datasetAttrs,
      coordAttrs,
    });
  }

}
