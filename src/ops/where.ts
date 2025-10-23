import {
  NDArray,
  DataValue,
  DimensionName,
  Coordinates,
  Attributes
} from '../types.js';
import { reshape, deepClone } from '../utils.js';
import type { DataBlock } from '../core/data-block.js';

export interface ArrayWhereOperand {
  kind: 'array';
  block: DataBlock;
  dims: DimensionName[];
  coords: Coordinates;
  attrs?: Attributes;
  name?: string;
}

export interface ScalarWhereOperand {
  kind: 'scalar';
  value: DataValue;
  attrs?: Attributes;
  name?: string;
}

export type WhereOperand = ArrayWhereOperand | ScalarWhereOperand;

export interface WhereOptions {
  keepAttrs?: boolean | 'x' | 'y' | ((attrs: {
    cond?: Attributes;
    x?: Attributes;
    y?: Attributes;
  }) => Attributes | undefined);
  preferNameFrom?: 'x' | 'y' | 'cond';
}

export interface BinaryOpOptions {
  keepAttrs?: boolean | 'left' | 'right' | ((attrs: {
    left?: Attributes;
    right?: Attributes;
  }) => Attributes | undefined);
  preferNameFrom?: 'left' | 'right';
}

export interface WhereResult {
  data: NDArray;
  dims: DimensionName[];
  coords: Coordinates;
  attrs?: Attributes;
  name?: string;
}

export function isArrayOperand(operand: WhereOperand): operand is ArrayWhereOperand {
  return operand.kind === 'array';
}

export function computeWhere(
  cond: WhereOperand,
  x: WhereOperand,
  y: WhereOperand,
  options?: WhereOptions
): WhereResult {
  const metadata = computeBroadcastMetadata([cond, x, y]);
  const plan = createBroadcastPlan(metadata);

  const data = plan.execute(indices => {
    const condValue = resolveOperandValue(cond, plan.dims, indices);
    const useX = Boolean(condValue);
    const xValue = resolveOperandValue(x, plan.dims, indices);
    const yValue = resolveOperandValue(y, plan.dims, indices);
    return useX ? xValue : yValue;
  });

  const attrs = resolveAttributes(cond, x, y, options?.keepAttrs);
  const name = resolveName(cond, x, y, options?.preferNameFrom);

  return {
    data,
    dims: plan.dims,
    coords: plan.coords,
    attrs,
    name
  };
}

export function computeBinaryOp(
  left: WhereOperand,
  right: WhereOperand,
  operator: (leftValue: DataValue, rightValue: DataValue) => DataValue,
  options?: BinaryOpOptions
): WhereResult {
  const metadata = computeBroadcastMetadata([left, right]);
  const plan = createBroadcastPlan(metadata);

  const data = plan.execute(indices => {
    const leftValue = resolveOperandValue(left, plan.dims, indices);
    const rightValue = resolveOperandValue(right, plan.dims, indices);
    return operator(leftValue, rightValue);
  });

  const attrs = resolveBinaryAttributes(left, right, options?.keepAttrs);
  const name = resolveBinaryName(left, right, options?.preferNameFrom);

  return {
    data,
    dims: plan.dims,
    coords: plan.coords,
    attrs,
    name
  };
}

interface BroadcastMetadata {
  dims: DimensionName[];
  coords: Coordinates;
  shape: number[];
}

interface BroadcastPlan {
  dims: DimensionName[];
  coords: Coordinates;
  shape: number[];
  execute(handler: (indices: number[]) => DataValue): NDArray;
}

function computeBroadcastMetadata(operands: WhereOperand[]): BroadcastMetadata {
  const dims: DimensionName[] = [];
  const coords: Coordinates = {};

  for (const operand of operands) {
    if (!isArrayOperand(operand)) continue;

    operand.dims.forEach((dim, index) => {
      const existingIndex = dims.indexOf(dim);
      const operandCoords = operand.coords[dim];
      if (!operandCoords) return;

      if (existingIndex === -1) {
        dims.push(dim);
        coords[dim] = deepClone(operandCoords);
      } else {
        const existingCoords = coords[dim];
        if (!arraysEqual(existingCoords, operandCoords)) {
          throw new Error(`Coordinate mismatch for dimension '${dim}'`);
        }
      }
    });
  }

  const shape = dims.map(dim => {
    const dimCoords = coords[dim];
    if (!dimCoords) {
      throw new Error(`Missing coordinates for dimension '${dim}'`);
    }
    return dimCoords.length;
  });

  return { dims, coords, shape };
}

function createBroadcastPlan(metadata: BroadcastMetadata): BroadcastPlan {
  return {
    dims: metadata.dims,
    coords: metadata.coords,
    shape: metadata.shape,
    execute(handler: (indices: number[]) => DataValue): NDArray {
      const totalElements = metadata.shape.reduce((acc, value) => acc * value, 1) || 1;

      if (metadata.shape.length === 0) {
        return handler([]);
      }

      const flatResult: DataValue[] = new Array(totalElements);

      for (let flatIndex = 0; flatIndex < totalElements; flatIndex++) {
        const indices = unravelIndex(flatIndex, metadata.shape);
        flatResult[flatIndex] = handler(indices);
      }

      return reshape(flatResult, metadata.shape) as NDArray;
    }
  };
}

function resolveOperandValue(
  operand: WhereOperand,
  targetDims: DimensionName[],
  targetIndices: number[]
): DataValue {
  if (!isArrayOperand(operand)) {
    return operand.value;
  }

  const operandIndices = operand.dims.map(dim => {
    const targetPos = targetDims.indexOf(dim);
    if (targetPos === -1) {
      throw new Error(`Operand is missing dimension '${dim}' present in broadcast result`);
    }
    return targetIndices[targetPos];
  });

  return operand.block.getValue(operandIndices);
}

function unravelIndex(flatIndex: number, shape: number[]): number[] {
  if (shape.length === 0) return [];

  const indices = new Array(shape.length).fill(0);
  let remainder = flatIndex;

  for (let dim = shape.length - 1; dim >= 0; dim--) {
    const size = shape[dim];
    indices[dim] = remainder % size;
    remainder = Math.floor(remainder / size);
  }

  return indices;
}

function arraysEqual(a: unknown[], b: unknown[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (!valuesEqual(a[i], b[i])) {
      return false;
    }
  }
  return true;
}

function valuesEqual(a: unknown, b: unknown): boolean {
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }
  return a === b;
}

function resolveAttributes(
  cond: WhereOperand,
  x: WhereOperand,
  y: WhereOperand,
  keepAttrs?: WhereOptions['keepAttrs']
): Attributes | undefined {
  if (!keepAttrs) return undefined;

  const attrs = {
    cond: isArrayOperand(cond) ? cond.attrs : undefined,
    x: isArrayOperand(x) ? x.attrs : undefined,
    y: isArrayOperand(y) ? y.attrs : undefined
  };

  if (typeof keepAttrs === 'function') {
    return keepAttrs(attrs) || undefined;
  }

  if (keepAttrs === true || keepAttrs === 'x') {
    return attrs.x ? deepClone(attrs.x) : undefined;
  }

  if (keepAttrs === 'y') {
    return attrs.y ? deepClone(attrs.y) : undefined;
  }

  return undefined;
}

function resolveName(
  cond: WhereOperand,
  x: WhereOperand,
  y: WhereOperand,
  preferNameFrom?: 'x' | 'y' | 'cond'
): string | undefined {
  const names: { [key: string]: string | undefined } = {
    cond: isArrayOperand(cond) ? cond.name : undefined,
    x: isArrayOperand(x) ? x.name : undefined,
    y: isArrayOperand(y) ? y.name : undefined
  };

  if (preferNameFrom && names[preferNameFrom]) {
    return names[preferNameFrom];
  }

  return names.x || names.y || names.cond;
}

function resolveBinaryAttributes(
  left: WhereOperand,
  right: WhereOperand,
  keepAttrs?: BinaryOpOptions['keepAttrs']
): Attributes | undefined {
  if (!keepAttrs) return undefined;

  const attrs = {
    left: isArrayOperand(left) ? left.attrs : undefined,
    right: isArrayOperand(right) ? right.attrs : undefined
  };

  if (typeof keepAttrs === 'function') {
    return keepAttrs(attrs) || undefined;
  }

  if (keepAttrs === true || keepAttrs === 'left') {
    return attrs.left ? deepClone(attrs.left) : undefined;
  }

  if (keepAttrs === 'right') {
    return attrs.right ? deepClone(attrs.right) : undefined;
  }

  return undefined;
}

function resolveBinaryName(
  left: WhereOperand,
  right: WhereOperand,
  preferNameFrom?: BinaryOpOptions['preferNameFrom']
): string | undefined {
  const names: { [key: string]: string | undefined } = {
    left: isArrayOperand(left) ? left.name : undefined,
    right: isArrayOperand(right) ? right.name : undefined
  };

  if (preferNameFrom && names[preferNameFrom]) {
    return names[preferNameFrom];
  }

  return names.left || names.right;
}
