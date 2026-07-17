import { describe, expect, test } from 'vitest';
import { DataArray } from '../../src/DataArray';
import { ZarrBackend } from '../../src/backends/zarr';
import * as dataBlockModule from '../../src/core/data-block';
import { MemoryZarrStore } from '../helpers/MemoryZarrStore';

type NumericTypedArray = Float32Array | Float64Array;

function requireCreateTypedBlock(): (
  flat: NumericTypedArray | Array<number | string | boolean | null>,
  shape: number[]
) => any {
  const createTypedBlock = (dataBlockModule as any).createTypedBlock;
  expect(
    typeof createTypedBlock,
    'createTypedBlock not exported from src/core/data-block'
  ).toBe('function');
  return createTypedBlock;
}

function requireFlatData(array: DataArray): { data: ArrayLike<any>; shape: number[] } {
  const flatData = (array as any).flatData;
  expect(
    flatData,
    'DataArray.flatData getter not exposed for a typed-backed array'
  ).not.toBeUndefined();
  expect(
    flatData,
    'DataArray.flatData unexpectedly returned null for a typed-backed array'
  ).not.toBeNull();
  return flatData;
}

function isTypedArray(value: unknown): boolean {
  return ArrayBuffer.isView(value) && !(value instanceof DataView);
}

function expectTypedFlatData(
  array: DataArray,
  TypedArrayConstructor: Float32ArrayConstructor | Float64ArrayConstructor,
  expectedShape: number[],
  expectedValues: number[]
): void {
  const flatData = requireFlatData(array);
  expect(
    isTypedArray(flatData.data),
    'flatData.data is not backed by a TypedArray'
  ).toBe(true);
  expect(
    flatData.data,
    `flatData.data does not preserve ${TypedArrayConstructor.name}`
  ).toBeInstanceOf(TypedArrayConstructor);
  expect(flatData.shape).toEqual(expectedShape);
  expect(Array.from(flatData.data)).toEqual(expectedValues);
}

function makeStore(
  values: NumericTypedArray,
  shape: number[],
  dims: string[],
  dataType: 'float32' | 'float64'
): MemoryZarrStore {
  const store = new MemoryZarrStore({
    'zarr.json': { node_type: 'group', attributes: {} },
    'data/zarr.json': {
      node_type: 'array',
      shape,
      data_type: dataType,
      dimension_names: dims
    }
  });
  const chunkKey = `data/c/${shape.map(() => 0).join('/')}`;
  store.set(chunkKey, new Uint8Array(values.buffer.slice(0)));
  return store;
}

async function readFloat64(
  values: number[],
  shape: number[],
  dims: string[]
): Promise<DataArray> {
  const store = makeStore(new Float64Array(values), shape, dims, 'float64');
  const dataset = await ZarrBackend.open(store);
  return dataset.getVariable('data').compute();
}

function rejectSourceMaterialization(array: DataArray): void {
  const block = (array as any)._block;
  expect(block?.kind).toBe('eager');
  block.materialize = () => {
    throw new Error('typed-backed consumer requested fully nested source data');
  };
}

describe('TypedArray-backed flat storage contract', () => {
  test('createTypedBlock keeps row-major flat storage and materializes nesting on demand', () => {
    const source = new Float64Array([0, 1, 2, 3, 4, 5, 6, 7]);
    const createTypedBlock = requireCreateTypedBlock();
    const block = createTypedBlock(source, [2, 2, 2]);

    expect(block.kind).toBe('eager');
    expect(block.shape).toEqual([2, 2, 2]);
    expect(block.getValue([1, 0, 1])).toBe(5);
    expect(block.materialize()).toEqual([
      [[0, 1], [2, 3]],
      [[4, 5], [6, 7]]
    ]);

    const plainBlock = createTypedBlock([10, 11, 12, 13], [2, 2]);
    expect(plainBlock.shape).toEqual([2, 2]);
    expect(plainBlock.getValue([1, 0])).toBe(12);
    expect(plainBlock.materialize()).toEqual([[10, 11], [12, 13]]);
  });

  test('flatData exists on eager nested arrays while allowing null', () => {
    const nested = new DataArray([[1, 2], [3, 4]], { dims: ['y', 'x'] });
    const flatData = (nested as any).flatData;

    expect(
      flatData,
      'DataArray.flatData getter not exposed for an eager nested array'
    ).not.toBeUndefined();

    if (flatData !== null) {
      expect(flatData.shape).toEqual([2, 2]);
      expect(Array.from(flatData.data)).toEqual([1, 2, 3, 4]);
    }
  });

  test('Zarr float64 compute exposes typed flat data while values and records stay compatible', async () => {
    const computed = await readFloat64([0, 1, 2, 3, 4, 5], [2, 3], ['row', 'col']);

    expect(computed.values).toEqual([
      [0, 1, 2],
      [3, 4, 5]
    ]);
    expect(computed.toRecords()).toEqual([
      { row: 0, col: 0, value: 0 },
      { row: 0, col: 1, value: 1 },
      { row: 0, col: 2, value: 2 },
      { row: 1, col: 0, value: 3 },
      { row: 1, col: 1, value: 4 },
      { row: 1, col: 2, value: 5 }
    ]);
    expect((computed as any)._block?.kind).toBe('eager');

    // The current EagerDataBlock captures its payload in a closure and exposes
    // only nested materialize(), so there is no honest pre-contract way to
    // inspect typed backing. The new flatData surface is the required probe.
    rejectSourceMaterialization(computed);
    expectTypedFlatData(computed, Float64Array, [2, 3], [0, 1, 2, 3, 4, 5]);
  });

  test('sel preserves typed backing for contiguous coordinate slices', async () => {
    const computed = await readFloat64(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
      [3, 4],
      ['row', 'col']
    );
    const selected = await computed.sel({
      row: { start: 1, stop: 2 },
      col: { start: 1, stop: 3 }
    });

    expect(selected.values).toEqual([
      [5, 6, 7],
      [9, 10, 11]
    ]);
    expectTypedFlatData(selected, Float64Array, [2, 3], [5, 6, 7, 9, 10, 11]);
  });

  test('isel preserves typed backing and row-major order for discrete selections', async () => {
    const computed = await readFloat64(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
      [3, 4],
      ['row', 'col']
    );
    const selected = await computed.isel({ row: [2, 0], col: [3, 1] });

    expect(selected.values).toEqual([
      [11, 9],
      [3, 1]
    ]);
    expectTypedFlatData(selected, Float64Array, [2, 2], [11, 9, 3, 1]);
  });

  test('sel preserves typed backing when a point selection drops a dimension', async () => {
    const computed = await readFloat64(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
      [3, 4],
      ['row', 'col']
    );
    const selected = await computed.sel({ row: 1 });

    expect(selected.values).toEqual([4, 5, 6, 7]);
    expectTypedFlatData(selected, Float64Array, [4], [4, 5, 6, 7]);
  });

  test('3-D reductions use typed flat storage without materializing the source', async () => {
    const computed = await readFloat64(
      Array.from({ length: 24 }, (_, index) => index + 1),
      [2, 3, 4],
      ['plane', 'row', 'col']
    );

    expect(computed.values).toEqual([
      [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]],
      [[13, 14, 15, 16], [17, 18, 19, 20], [21, 22, 23, 24]]
    ]);
    expect(computed.sum()).toBe(300);
    expect(computed.mean()).toBe(12.5);

    const sumByColumn = computed.sum('col') as DataArray;
    const meanByRow = computed.mean('row') as DataArray;
    expect(sumByColumn.values).toEqual([
      [10, 26, 42],
      [58, 74, 90]
    ]);
    expect(meanByRow.values).toEqual([
      [5, 6, 7, 8],
      [17, 18, 19, 20]
    ]);

    rejectSourceMaterialization(computed);
    expectTypedFlatData(
      computed,
      Float64Array,
      [2, 3, 4],
      Array.from({ length: 24 }, (_, index) => index + 1)
    );

    expect(computed.sum()).toBe(300);
    expect(computed.mean()).toBe(12.5);
    expect((computed.sum('col') as DataArray).values).toEqual([
      [10, 26, 42],
      [58, 74, 90]
    ]);
    expect((computed.mean('row') as DataArray).values).toEqual([
      [5, 6, 7, 8],
      [17, 18, 19, 20]
    ]);
  });

  test('3-D toRecords reads typed flat storage in row-major order without materializing the source', async () => {
    const computed = await readFloat64(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
      [2, 2, 3],
      ['plane', 'row', 'col']
    );
    const expectedRecords = [
      { plane: 0, row: 0, col: 0, value: 0 },
      { plane: 0, row: 0, col: 1, value: 1 },
      { plane: 0, row: 0, col: 2, value: 2 },
      { plane: 0, row: 1, col: 0, value: 3 },
      { plane: 0, row: 1, col: 1, value: 4 },
      { plane: 0, row: 1, col: 2, value: 5 },
      { plane: 1, row: 0, col: 0, value: 6 },
      { plane: 1, row: 0, col: 1, value: 7 },
      { plane: 1, row: 0, col: 2, value: 8 },
      { plane: 1, row: 1, col: 0, value: 9 },
      { plane: 1, row: 1, col: 1, value: 10 },
      { plane: 1, row: 1, col: 2, value: 11 }
    ];

    expect(computed.values).toEqual([
      [[0, 1, 2], [3, 4, 5]],
      [[6, 7, 8], [9, 10, 11]]
    ]);
    expect(computed.toRecords()).toEqual(expectedRecords);
    rejectSourceMaterialization(computed);
    expectTypedFlatData(
      computed,
      Float64Array,
      [2, 2, 3],
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    );

    expect(computed.toRecords()).toEqual(expectedRecords);
  });

  test('Float32Array dtype survives Zarr compute and discrete selection', async () => {
    const values = new Float32Array([0.5, 1.5, 2.5, 3.5, 4.5, 5.5]);
    const store = makeStore(values, [2, 3], ['row', 'col'], 'float32');
    const dataset = await ZarrBackend.open(store);
    const computed = await dataset.getVariable('data').compute();
    const selected = await computed.isel({ col: [2, 0] });

    expect(computed.values).toEqual([
      [0.5, 1.5, 2.5],
      [3.5, 4.5, 5.5]
    ]);
    expect(selected.values).toEqual([
      [2.5, 0.5],
      [5.5, 3.5]
    ]);
    expectTypedFlatData(computed, Float32Array, [2, 3], [0.5, 1.5, 2.5, 3.5, 4.5, 5.5]);
    expectTypedFlatData(selected, Float32Array, [2, 2], [2.5, 0.5, 5.5, 3.5]);
  });
});

// BigInt64Array/BigUint64Array are intentionally exempt from this contract:
// DataValue currently excludes bigint and numeric reductions operate on number.
