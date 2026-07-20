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

  test('DataArray constructor distinguishes plain flat payloads from DataArray instances', () => {
    const fromPayload = new DataArray({
      data: new Float64Array([1, 2, 3, 4]),
      shape: [2, 2]
    }, { dims: ['y', 'x'] });
    expect(fromPayload.values).toEqual([[1, 2], [3, 4]]);
    expectTypedFlatData(fromPayload, Float64Array, [2, 2], [1, 2, 3, 4]);

    const source = new DataArray([[1, 2], [3, 4]], { dims: ['y', 'x'] });
    const fromInstance = new DataArray(source as any);
    expect(fromInstance.shape).toEqual([]);
    expect(fromInstance.values).toBe(source);
    expect(fromInstance.flatData).toBeNull();
  });

  test('flat payloads reject invalid shape dimensions before size arithmetic', () => {
    for (const shape of [[-1], [1.5], [Number.NaN], [Number.POSITIVE_INFINITY]]) {
      expect(() => new DataArray({ data: [], shape })).toThrow(
        'Flat data shape dimensions must be non-negative safe integers'
      );
    }
  });

  test('flat and nested boolean reductions have identical numeric semantics', () => {
    const nested = new DataArray([true, false, true]);
    const flat = new DataArray({ data: [true, false, true], shape: [3] });

    expect(flat.sum()).toBe(nested.sum());
    expect(flat.mean()).toBe(nested.mean());
    expect(flat.sum('dim_0')).toBe(nested.sum('dim_0'));
    expect(flat.mean('dim_0')).toBe(nested.mean('dim_0'));
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

  test('one-sided label slices keep data, shape, and coordinates aligned for typed and nested arrays', async () => {
    const options = {
      dims: ['y', 'x'],
      coords: { y: ['top', 'bottom'], x: ['a', 'b', 'c', 'd'] }
    };
    const sources = [
      new DataArray({
        data: new Float64Array([0, 1, 2, 3, 4, 5, 6, 7]),
        shape: [2, 4]
      }, options),
      new DataArray([[0, 1, 2, 3], [4, 5, 6, 7]], options)
    ];

    for (const source of sources) {
      const throughStop = await source.sel({ x: { stop: 'b' } });
      expect(throughStop.values).toEqual([[0, 1], [4, 5]]);
      expect(throughStop.shape).toEqual([2, 2]);
      expect(throughStop.coords.x).toEqual(['a', 'b']);

      const fromStart = await source.sel({ x: { start: 'c' } });
      expect(fromStart.values).toEqual([[2, 3], [6, 7]]);
      expect(fromStart.shape).toEqual([2, 2]);
      expect(fromStart.coords.x).toEqual(['c', 'd']);
    }
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

  test('sum uses zero identity for empty typed dimensions while mean remains NaN', async () => {
    const source = new DataArray({
      data: new Float64Array([1, 2, 3, 4, 5, 6]),
      shape: [2, 3]
    }, { dims: ['y', 'x'] });

    const emptyLeading = await source.isel({ y: [] });
    expect(emptyLeading.shape).toEqual([0, 3]);
    expect((emptyLeading.sum('y') as DataArray).values).toEqual([0, 0, 0]);
    expect((emptyLeading.mean('y') as DataArray).values).toEqual([NaN, NaN, NaN]);

    const emptyTrailing = await source.isel({ x: [] });
    expect(emptyTrailing.shape).toEqual([2, 0]);
    expect((emptyTrailing.sum('x') as DataArray).values).toEqual([0, 0]);
  });

  test('lazy multi-run flat stitching preserves unsorted and multi-dimensional selections', async () => {
    type IndexRange = { start: number; stop: number } | number;
    const loaderCalls: Array<Record<string, IndexRange>> = [];
    const dimensionIndices = (range: IndexRange): number[] =>
      typeof range === 'number'
        ? [range]
        : Array.from({ length: range.stop - range.start }, (_, index) => range.start + index);

    const loader = async (ranges: Record<string, IndexRange>) => {
      loaderCalls.push({ ...ranges });
      const pointIndices = dimensionIndices(ranges.point);
      const rowIndices = dimensionIndices(ranges.row);
      const colIndices = dimensionIndices(ranges.col);
      const data = new Float64Array(pointIndices.length * rowIndices.length * colIndices.length);
      let offset = 0;
      for (const point of pointIndices) {
        for (const row of rowIndices) {
          for (const col of colIndices) {
            data[offset++] = point * 10_000 + row * 100 + col;
          }
        }
      }
      const shape = [
        typeof ranges.point === 'number' ? null : pointIndices.length,
        typeof ranges.row === 'number' ? null : rowIndices.length,
        typeof ranges.col === 'number' ? null : colIndices.length
      ].filter((size): size is number => size !== null);
      return { data, shape };
    };

    const lazy = new DataArray(null, {
      lazy: true,
      virtualShape: [2, 40, 3],
      lazyLoader: loader,
      dims: ['point', 'row', 'col'],
      coords: {
        point: [0, 1],
        row: Array.from({ length: 40 }, (_, index) => index),
        col: [0, 1, 2]
      }
    });
    const selected = await lazy.isel({
      point: 1,
      row: [39, 0, 20],
      col: [2, 0]
    });
    const computed = await selected.compute();

    expect(computed.values).toEqual([
      [13902, 13900],
      [10002, 10000],
      [12002, 12000]
    ]);
    expect(computed.dims).toEqual(['row', 'col']);
    expect(computed.coords).toEqual({ row: [39, 0, 20], col: [2, 0] });
    expect(Array.from(requireFlatData(computed).data)).toEqual([
      13902, 13900, 10002, 10000, 12002, 12000
    ]);
    expect(loaderCalls).toHaveLength(3);
    expect(loaderCalls.map(call => call.row)).toEqual([
      { start: 0, stop: 1 },
      { start: 20, stop: 21 },
      { start: 39, stop: 40 }
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

// BigInt64Array/BigUint64Array are exempt from the TypedArray-preservation
// contract above: DataValue excludes bigint and numeric reductions operate on
// number. They must still flow through the flat path (narrowed to numbers)
// rather than being mistaken for scalar data, and must never silently lose
// precision on values beyond ±(2^53 - 1).
function makeBigIntStore(
  values: BigInt64Array | BigUint64Array,
  shape: number[],
  dims: string[],
  dataType: 'int64' | 'uint64'
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

describe('int64/uint64 Zarr storage narrows to numbers without silent corruption', () => {
  test('safe int64 values flow through the flat path as numbers', async () => {
    const store = makeBigIntStore(
      new BigInt64Array([-5n, 0n, 42n, 9007199254740991n]),
      [4],
      ['idx'],
      'int64'
    );
    const dataset = await ZarrBackend.open(store);
    const computed = await dataset.getVariable('data').compute();

    // Reshaped as a normal numeric array, not treated as scalar (which would
    // throw a dimension-count mismatch before this fix).
    expect(computed.shape).toEqual([4]);
    expect(computed.values).toEqual([-5, 0, 42, 9007199254740991]);
  });

  test('uint64 values within the safe range narrow to numbers', async () => {
    const store = makeBigIntStore(
      new BigUint64Array([0n, 1n, 9007199254740991n]),
      [3],
      ['idx'],
      'uint64'
    );
    const dataset = await ZarrBackend.open(store);
    const computed = await dataset.getVariable('data').compute();

    expect(computed.values).toEqual([0, 1, 9007199254740991]);
  });

  test('an int64 value beyond 2^53 is rejected rather than silently truncated', async () => {
    // 9007199254740993 is not representable as a JS number; Number() would round
    // it to 9007199254740992. The read must fail loudly instead.
    const store = makeBigIntStore(
      new BigInt64Array([1n, 9007199254740993n]),
      [2],
      ['idx'],
      'int64'
    );
    const dataset = await ZarrBackend.open(store);

    await expect(dataset.getVariable('data').compute())
      .rejects.toThrow('without loss of precision');
  });

  test('int64 coordinate labels narrow through the same guard', async () => {
    // Name == dimension makes this a coordinate variable, decoded eagerly at
    // open() via normalizeCoordinateValues. Safe values narrow to numbers.
    const safe = makeBigIntStore(new BigInt64Array([1n, 2n, 3n]), [3], ['data'], 'int64');
    const dataset = await ZarrBackend.open(safe);
    expect(dataset.coords.data).toEqual([1, 2, 3]);
  });

  test('an unsafe int64 coordinate value is rejected at open, not silently rounded', async () => {
    const unsafe = makeBigIntStore(
      new BigInt64Array([1n, 9007199254740993n]),
      [2],
      ['data'],
      'int64'
    );
    await expect(ZarrBackend.open(unsafe)).rejects.toThrow('without loss of precision');
  });
});

describe('selectFlatData short-circuits whole-array no-op selections', () => {
  const selectFlatData = (dataBlockModule as any).selectFlatData as (
    source: { data: ArrayLike<any>; shape: number[] },
    selections: Array<unknown>
  ) => { data: ArrayLike<any>; shape: number[] };

  test('all-undefined selections reuse the source storage instead of copying', () => {
    expect(typeof selectFlatData, 'selectFlatData not exported').toBe('function');

    const source = { data: new Float64Array([1, 2, 3, 4, 5, 6]), shape: [2, 3] };
    const result = selectFlatData(source, [undefined, undefined]);

    // Same buffer, not a duplicate — the semantic no-op must not allocate O(n).
    expect(result.data).toBe(source.data);
    expect(result.shape).toEqual([2, 3]);
    expect(result.shape).not.toBe(source.shape); // shape is a fresh array
  });

  test('any real selection still allocates a distinct buffer', () => {
    const source = { data: new Float64Array([1, 2, 3, 4, 5, 6]), shape: [2, 3] };
    const result = selectFlatData(source, [undefined, { start: 0, stop: 2 }]);

    expect(result.data).not.toBe(source.data);
    expect(result.shape).toEqual([2, 2]);
    expect(Array.from(result.data)).toEqual([1, 2, 4, 5]);
  });
});
