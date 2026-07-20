// backends/zarr.ts
import * as zarr from "zarrita";
import { Dataset } from "../Dataset.js";
import { DataArray } from "../DataArray.js";
import { DataValue, FlatDataStorage } from "../types.js";
import { decodeCFTime, isTimeCoordinate } from "../time/cf-time.js";

const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE_BIGINT = BigInt(Number.MIN_SAFE_INTEGER);

// int64/uint64 values arrive from zarrita as bigint (scalars) or BigInt64Array/
// BigUint64Array (arrays). DataValue cannot hold a bigint, so each value is
// narrowed to a number — but only when it is exactly representable. Coercing a
// value beyond ±2^53 would silently corrupt it (e.g. 9007199254740993n would
// read back as 9007199254740992), so those are rejected loudly instead.
function bigIntToNumber(value: bigint): number {
  if (value > MAX_SAFE_BIGINT || value < MIN_SAFE_BIGINT) {
    throw new Error(
      `Cannot read 64-bit integer ${value} as a JavaScript number without loss of precision ` +
        `(magnitude exceeds Number.MAX_SAFE_INTEGER, 2^53 - 1).`
    );
  }
  return Number(value);
}

function narrowBigInt(value: unknown): unknown {
  if (typeof value === "bigint") return bigIntToNumber(value);
  if ((value instanceof BigInt64Array || value instanceof BigUint64Array) && value.length === 1) {
    return bigIntToNumber(value[0]);
  }
  return value;
}

function normalizeCoordinateValues(values: any[], attrs: Record<string, any> | undefined): any[] {
  const normalized = values.map((value) => {
    // Narrow int64/uint64 coordinate labels through the same precision guard as
    // data values: coercing a value beyond ±2^53 would silently corrupt the
    // label (and every selection against it), so those are rejected loudly.
    if (typeof value === "bigint") {
      return bigIntToNumber(value);
    }
    return value;
  });

  if (attrs && isTimeCoordinate(attrs)) {
    const units: string | undefined = attrs.units;
    const calendar: string | undefined = attrs.calendar;
    if (units) {
      const decodedValues = normalized.map((value) => {
        if (typeof value === "number") {
          return decodeCFTime(value, units, calendar) ?? value;
        }
        return value;
      });

      return decodedValues.map((value) => value instanceof Date ? value.toISOString() : value);
    }
  }

  return normalized;
}

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

function normalizeGroup(group: string): string {
  return group.replace(/^\/+/, "").replace(/\/+$/, "");
}

function isPathUnderGroup(path: string, group: string): boolean {
  return path === group || path.startsWith(`${group}/`);
}

function relativeToGroup(path: string, group: string): string {
  if (!group) return path;
  if (path === group) return "";
  return path.slice(group.length + 1);
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

    const requestedGroup = normalizeGroup(group);
    const jsonKeys = listKeys.filter((k) => k.endsWith("zarr.json"));

    if (jsonKeys.length === 0 || (requestedGroup && !jsonKeys.some((k) => k === `${requestedGroup}/zarr.json` || k.startsWith(`${requestedGroup}/`)))) {
      throw new Error(
        `ZarrBackend.open: no zarr.json under group "${requestedGroup || "/"}".`
      );
    }

    // ---- Parse node metadata & pick arrays ----
    // Map: arrayPath -> meta
    const arrayMetas: Map<string, any> = new Map();

    const parsedMetadata = await Promise.all(jsonKeys.map(async (key) => {
      const bytes = await store.get(key);
      if (!bytes) return null;

      let meta: any;
      try {
        meta = JSON.parse(new TextDecoder().decode(bytes));
      } catch (e) {
        // Some implementations DAG-CBOR the zarr.json; try a safe CBOR decode if needed
        // but typically zarr.json is JSON text.
        return null;
      }

      const nodeType = meta?.node_type;
      const path = dirname(key); // array/group path
      return { nodeType, path, meta };
    }));

    for (const parsed of parsedMetadata) {
      if (parsed?.nodeType === "array") {
        const { path, meta } = parsed;
        arrayMetas.set(path, meta);
      }
      // (For groups, we don’t need to do anything special here.)
    }

    let normalizedGroup = requestedGroup;
    if (!normalizedGroup) {
      const arrayPaths = [...arrayMetas.keys()];
      const rootArrayPaths = arrayPaths.filter((path) => !path.includes("/"));
      const topLevelGroups = new Set(
        arrayPaths
          .filter((path) => path.includes("/"))
          .map((path) => path.split("/", 1)[0])
      );

      if (rootArrayPaths.length === 0 && topLevelGroups.size > 1) {
        throw new Error(
          "ZarrBackend.open: grouped Zarr stores with multiple top-level groups require an explicit group option."
        );
      }

      if (rootArrayPaths.length === 0 && topLevelGroups.size === 1) {
        normalizedGroup = [...topLevelGroups][0];
      }

      if (rootArrayPaths.length > 0) {
        for (const path of [...arrayMetas.keys()]) {
          if (path.includes("/")) {
            arrayMetas.delete(path);
          }
        }
      }
    }

    if (normalizedGroup) {
      for (const path of [...arrayMetas.keys()]) {
        if (!isPathUnderGroup(path, normalizedGroup)) {
          arrayMetas.delete(path);
        }
      }
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
      relativePath: string;
      meta: any;
      dims: string[];
      attrs: Record<string, any>;
      shape: number[];
    }> = [];

    for (const [path, meta] of arrayMetas.entries()) {
      const name = lastSegment(path);
      const relativePath = relativeToGroup(path, normalizedGroup);

      const dims =
        Array.isArray(meta?.dimension_names) && meta.dimension_names.length === meta.shape?.length
          ? [...meta.dimension_names]
          : // fallback dimension names if not present
            meta?.shape?.map((_: number, i: number) => `dim_${i}`) ?? [];

      const attrs = meta?.attributes ?? {};
      const shape = meta?.shape ?? [];

      arrayMetadata.push({ path, name, relativePath, meta, dims, attrs, shape });
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
    const rootLocation = zarr.root(store as any);
    const groupLocation = normalizedGroup ? rootLocation.resolve(normalizedGroup) : rootLocation;
    const zarrGroup = await zarr.open(groupLocation, { kind: "group" });

    // ---- Load coordinate arrays (eagerly - they're small and needed for selection) ----
    const coords: Record<string, any[]> = {};
    const coordAttrs: Record<string, Record<string, any>> = {}; // Store coordinate attributes

    await Promise.all(arrayMetadata.map(async (arr) => {
      if (coordNames.has(arr.name)) {
        // Load coordinate data immediately using group.resolve()
        const coordArray = await zarr.open(zarrGroup.resolve(arr.relativePath), { kind: "array" });

        const coordData = await zarr.get(coordArray);

        // Extract the actual values from zarrita result
        const values = coordData.data || coordData;

        // Convert to regular array
        const rawValues = Array.isArray(values) ? values : Array.from(values as any);
        const coordValues = normalizeCoordinateValues(rawValues, arr.attrs);
        coords[arr.name] = coordValues;

        // Store coordinate attributes (e.g., time units)
        coordAttrs[arr.name] = arr.attrs;
      }
    }));

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

      function openArray() {
        return zarr.open(zarrGroup.resolve(arr.relativePath), { kind: "array" });
      }

      let arrayNodePromise: ReturnType<typeof openArray> | undefined;
      const getArrayNode = () => {
        if (!arrayNodePromise) {
          const pendingOpen = openArray();
          arrayNodePromise = pendingOpen;
          void pendingOpen.catch(() => {
            if (arrayNodePromise === pendingOpen) {
              arrayNodePromise = undefined;
            }
          });
        }
        return arrayNodePromise;
      };

      // Create a lazy loader function that the DataArray can call
      const lazyLoader = async (indexRanges: { [dim: string]: { start: number; stop: number } | number }) => {
        const arrNode = await getArrayNode();
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

        const rawResult = result.data !== undefined ? result.data : result;

        // Handle scalar result (all dimensions were single indices)
        if (resultShape.length === 0) {
          return narrowBigInt(rawResult) as unknown as DataValue;
        }

        // int64/uint64 arrays arrive as BigInt64Array/BigUint64Array, which the
        // flat-data path (FlatDataStorage / isFlatData) does not accept — passing
        // one through makes DataArray treat the payload as scalar and throw a
        // dimension mismatch. Narrow them to a number[] (rejecting any value that
        // cannot be represented exactly) so every dtype flows through the flat path.
        const flatData: FlatDataStorage =
          rawResult instanceof BigInt64Array || rawResult instanceof BigUint64Array
            ? Array.from(rawResult, bigIntToNumber)
            : (rawResult as FlatDataStorage);
        return { data: flatData, shape: resultShape };
      };

      dataVars[arr.name] = new DataArray(null, {
        lazy: true,
        virtualShape: arr.shape,
        lazyLoader: lazyLoader,
        dims: arr.dims,  // Use real dims
        coords: perDimCoords,
        attrs: {
          ...arr.attrs,
          _zarr_path: arr.path,
          _zarr_shape: arr.shape,  // Store actual shape
          _zarr_coords: perDimCoords, // Store coords here
          _coordAttrs: coordAttrs, // Store coordinate attributes for time conversion
          _zarr_data_type: arr.meta.data_type, // Store data type for byte size calculation
          codecs: arr.meta.codecs, // Store codecs for encryption detection
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

    const dataset = new Dataset(dataVars, {
      coords,
      attrs: datasetAttrs,
      coordAttrs,
    });

    // Automatically detect encryption
    dataset.detectEncryption();

    return dataset;
  }

}
