import {
  NDArray,
  DataValue,
  LazyLoader,
  LazyIndexRange
} from '../types.js';
import { getShape, getAtIndex } from '../utils.js';

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
}

export interface LazyDataBlock extends BaseDataBlock {
  kind: 'lazy';
  fetch(ranges: Record<string, LazyIndexRange>): Promise<NDArray>;
}

export type DataBlock = EagerDataBlock | LazyDataBlock;

export function createEagerBlock(data: NDArray): EagerDataBlock {
  const payload = data;
  const shape = getShape(payload);

  return {
    kind: 'eager',
    shape,
    materialize(): NDArray {
      return payload;
    },
    getValue(indices: number[]): DataValue {
      return getAtIndex(payload, indices);
    },
    clone(): DataBlock {
      return createEagerBlock(payload);
    }
  };
}

export function createLazyBlock(
  shape: number[],
  loader: LazyLoader
): LazyDataBlock {
  const normalizedShape = [...shape];

  const fetch = (ranges: Record<string, LazyIndexRange>): Promise<NDArray> => {
    const result = loader(ranges);
    return Promise.resolve(result);
  };

  return {
    kind: 'lazy',
    shape: normalizedShape,
    materialize(): NDArray {
      throw new Error('Materializing a lazy DataBlock requires an explicit execution step.');
    },
    getValue(): DataValue {
      throw new Error('Random access on a lazy DataBlock requires explicit execution.');
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
