import {
  NDArray,
  DataValue,
  DataArrayInput,
  FlatData,
  FlatDataStorage,
  NumericTypedArray,
  LazyLoader,
  LazyIndexRange
} from '../types.js';
import { getShape, getAtIndex, reshapeFlat } from '../utils.js';

export type DataBlockKind = 'eager' | 'lazy';

interface BaseDataBlock {
  readonly kind: DataBlockKind;
  readonly shape: number[];
  materialize(): NDArray;
  getValue(indices: number[]): DataValue;
  clone(): DataBlock;
}

export interface EagerDataBlock extends BaseDataBlock {
  kind: 'eager';
  readonly flatData: FlatData | null;
}

export interface LazyDataBlock extends BaseDataBlock {
  kind: 'lazy';
  fetch(ranges: Record<string, LazyIndexRange>): Promise<DataArrayInput>;
}

export type DataBlock = EagerDataBlock | LazyDataBlock;

export type FlatIndexSelection =
  | number
  | number[]
  | { start: number; stop: number }
  | undefined;

function expectedSize(shape: number[]): number {
  let size = 1;
  for (const dimension of shape) {
    if (!Number.isSafeInteger(dimension) || dimension < 0) {
      throw new Error('Flat data shape dimensions must be non-negative safe integers');
    }
    size *= dimension;
    if (!Number.isSafeInteger(size)) {
      throw new Error('Flat data shape size exceeds the safe integer range');
    }
  }
  return size;
}

function isTypedStorage(data: unknown): data is NumericTypedArray {
  return ArrayBuffer.isView(data)
    && !(data instanceof DataView)
    && !(data instanceof BigInt64Array)
    && !(data instanceof BigUint64Array);
}

export function isFlatData(value: DataArrayInput | unknown): value is FlatData {
  if (!value || typeof value !== 'object' || value.constructor !== Object) {
    return false;
  }

  const { data, shape } = value as FlatData;
  const hasFlatStorage = Array.isArray(data) || isTypedStorage(data);
  return hasFlatStorage && Array.isArray(shape);
}

function allocateLike(
  source: FlatDataStorage,
  length: number
): FlatDataStorage {
  if (!isTypedStorage(source)) {
    return new Array<DataValue>(length);
  }

  const Constructor = source.constructor as unknown as new (length: number) => NumericTypedArray;
  return new Constructor(length);
}

function createEagerStorageBlock(
  shape: number[],
  nested: NDArray | undefined,
  flat: FlatDataStorage | undefined
): EagerDataBlock {
  const normalizedShape = [...shape];
  let nestedCache = nested;

  return {
    kind: 'eager',
    shape: normalizedShape,
    get flatData(): FlatData | null {
      return flat ? { data: flat, shape: [...normalizedShape] } : null;
    },
    materialize(): NDArray {
      if (nestedCache === undefined) {
        nestedCache = reshapeFlat(flat!, normalizedShape);
      }
      return nestedCache;
    },
    getValue(indices: number[]): DataValue {
      if (!flat) {
        return getAtIndex(nestedCache!, indices);
      }
      if (indices.length !== normalizedShape.length) {
        throw new Error('Index dimensionality does not match data shape');
      }

      let offset = 0;
      let stride = 1;
      for (let dim = normalizedShape.length - 1; dim >= 0; dim--) {
        const index = indices[dim];
        if (!Number.isInteger(index) || index < 0 || index >= normalizedShape[dim]) {
          throw new Error('Index out of bounds');
        }
        offset += index * stride;
        stride *= normalizedShape[dim];
      }
      return flat[offset];
    },
    clone(): DataBlock {
      return createEagerStorageBlock(normalizedShape, nestedCache, flat);
    }
  };
}

export function createEagerBlock(data: NDArray): EagerDataBlock {
  return createEagerStorageBlock(getShape(data), data, undefined);
}

export function createTypedBlock(
  flat: FlatDataStorage,
  shape: number[]
): EagerDataBlock {
  const size = expectedSize(shape);
  if (flat.length !== size) {
    throw new Error(`Flat data length (${flat.length}) does not match shape size (${size})`);
  }
  return createEagerStorageBlock(shape, undefined, flat);
}

export function selectFlatData(
  source: FlatData,
  selections: FlatIndexSelection[]
): FlatData {
  if (selections.length !== source.shape.length) {
    throw new Error('Selection dimensionality does not match data shape');
  }

  const outputShape: number[] = [];
  for (let dim = 0; dim < source.shape.length; dim++) {
    const selection = selections[dim];
    if (typeof selection === 'number') continue;
    if (Array.isArray(selection)) outputShape.push(selection.length);
    else if (selection) outputShape.push(Math.max(0, selection.stop - selection.start));
    else outputShape.push(source.shape[dim]);
  }

  const outputSize = expectedSize(outputShape);
  const output = allocateLike(source.data, outputSize);
  const sourceStrides = new Array(source.shape.length);
  let stride = 1;
  for (let dim = source.shape.length - 1; dim >= 0; dim--) {
    sourceStrides[dim] = stride;
    stride *= source.shape[dim];
  }

  for (let outputOffset = 0; outputOffset < outputSize; outputOffset++) {
    let remainder = outputOffset;
    let sourceOffset = 0;
    let outputDim = outputShape.length - 1;

    for (let dim = source.shape.length - 1; dim >= 0; dim--) {
      const selection = selections[dim];
      let sourceIndex: number;
      if (typeof selection === 'number') {
        sourceIndex = selection;
      } else {
        const dimensionSize = outputShape[outputDim];
        const outputIndex = dimensionSize === 0 ? 0 : remainder % dimensionSize;
        remainder = dimensionSize === 0 ? 0 : Math.floor(remainder / dimensionSize);
        outputDim--;
        if (Array.isArray(selection)) sourceIndex = selection[outputIndex];
        else if (selection) sourceIndex = selection.start + outputIndex;
        else sourceIndex = outputIndex;
      }
      sourceOffset += sourceIndex * sourceStrides[dim];
    }
    output[outputOffset] = source.data[sourceOffset];
  }

  return { data: output, shape: outputShape };
}

export function stitchFlatData(
  sources: FlatData[],
  locations: Array<{ sourceIndex: number; offset: number }>,
  dimIndex: number
): FlatData {
  if (sources.length === 0) {
    throw new Error('Cannot stitch an empty flat data source list');
  }
  const outputShape = [...sources[0].shape];
  outputShape[dimIndex] = locations.length;
  const outputSize = expectedSize(outputShape);
  const output = allocateLike(sources[0].data, outputSize);

  const sourceStrides = sources.map(source => {
    const strides = new Array(source.shape.length);
    let sourceStride = 1;
    for (let dim = source.shape.length - 1; dim >= 0; dim--) {
      strides[dim] = sourceStride;
      sourceStride *= source.shape[dim];
    }
    return strides;
  });

  for (let outputOffset = 0; outputOffset < outputSize; outputOffset++) {
    let remainder = outputOffset;
    const outputIndices = new Array(outputShape.length);
    for (let dim = outputShape.length - 1; dim >= 0; dim--) {
      outputIndices[dim] = remainder % outputShape[dim];
      remainder = Math.floor(remainder / outputShape[dim]);
    }
    const location = locations[outputIndices[dimIndex]];
    let sourceOffset = 0;
    for (let dim = 0; dim < outputIndices.length; dim++) {
      const index = dim === dimIndex ? location.offset : outputIndices[dim];
      sourceOffset += index * sourceStrides[location.sourceIndex][dim];
    }
    output[outputOffset] = sources[location.sourceIndex].data[sourceOffset];
  }

  return { data: output, shape: outputShape };
}

export function createLazyBlock(
  shape: number[],
  loader: LazyLoader
): LazyDataBlock {
  const normalizedShape = [...shape];

  const fetch = (ranges: Record<string, LazyIndexRange>): Promise<DataArrayInput> => {
    const result = loader(ranges);
    return Promise.resolve(result);
  };

  return {
    kind: 'lazy',
    shape: normalizedShape,
    materialize(): NDArray {
      throw new Error('Materializing a lazy DataBlock requires an explicit execution step. Try running .compute() on it first');
    },
    getValue(): DataValue {
      throw new Error('Random access on a lazy DataBlock requires explicit execution. Try running .compute() on it first');
    },
    clone(): DataBlock {
      return createLazyBlock(normalizedShape, loader);
    },
    fetch
  };
}

export function isLazyBlock(block: DataBlock): block is LazyDataBlock {
  return block.kind === 'lazy';
}
