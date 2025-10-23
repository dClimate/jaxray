import { NDArray, DataValue } from '../types.js';
import { deepClone, getShape, getAtIndex } from '../utils.js';

export type DataBlockKind = 'eager' | 'lazy';

export interface DataBlock {
  readonly kind: DataBlockKind;
  readonly shape: number[];
  materialize(): NDArray;
  getValue(indices: number[]): DataValue;
  clone(): DataBlock;
}

export interface LazyBlockOptions {
  materialize?: () => NDArray;
  getValue?: (indices: number[]) => DataValue;
}

export function createEagerBlock(data: NDArray): DataBlock {
  const payload = deepClone(data);
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

export function createPlaceholderLazyBlock(shape: number[], options: LazyBlockOptions = {}): DataBlock {
  const materialize = options.materialize;
  const getValueFn = options.getValue;

  return {
    kind: 'lazy',
    shape: [...shape],
    materialize(): NDArray {
      if (materialize) {
        return materialize();
      }
      throw new Error('Materializing a lazy DataBlock requires an execution engine.');
    },
    getValue(indices: number[]): DataValue {
      if (getValueFn) {
        return getValueFn(indices);
      }
      throw new Error('Random access on a lazy DataBlock requires an execution engine.');
    },
    clone(): DataBlock {
      return createPlaceholderLazyBlock(shape, options);
    }
  };
}
