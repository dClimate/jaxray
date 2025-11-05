/**
 * DataArray - A labeled, multi-dimensional array
 * Similar to xarray.DataArray in Python
 */

import {
  NDArray,
  DataValue,
  DimensionName,
  Coordinates,
  Attributes,
  DataArrayOptions,
  Selection,
  CoordinateValue,
  SelectionOptions,
  StreamOptions,
  StreamChunk,
  RollingOptions,
} from './types.js';
import { getShape, flatten, deepClone } from './utils.js';
import {
  createEagerBlock,
  createLazyBlock,
  isLazyBlock,
  type DataBlock
} from './core/data-block.js';
import {
  computeWhere,
  computeBinaryOp,
  type WhereOperand,
  type WhereOptions,
  type ArrayWhereOperand,
  type BinaryOpOptions
} from './ops/where.js';
import { isTimeCoordinate, parseCFTimeUnits, cfTimeToDate } from './time/cf-time.js';
import { findCoordinateIndex } from './utils/coordinate-indexing.js';
import {
  sumAll,
  countAll,
  divideArray,
  elementWiseOp,
  reshapeSqueezed,
  selectAtDimension,
  selectMultipleAtDimension,
  sliceAtDimension
} from './utils/data-operations.js';
import {
  performLazySelection,
  mapIndexToOriginal
} from './utils/lazy-selection.js';
import { applyRolling } from './utils/rolling-operations.js';

export class DataArray {
  private _block: DataBlock;
  private _dims: DimensionName[];
  private _coords: Coordinates;
  private _attrs: Attributes;
  private _name?: string;
  private _shape: number[];
  private _precision: number = 6;
  /**
   * Mapping from current indices to original indices in the source dataset.
   * Only used for lazy DataArrays that are results of selections.
   * Maps each dimension to an array where originalIndexMapping[dim][i] = original_index_j
   * meaning: data at current index i comes from original index j
   */
  private _originalIndexMapping?: { [dimension: string]: number[] };

  // Performance optimization caches
  private _dimIndexMap: Map<string, number> = new Map();
  // private _precisionFactor: number;

  constructor(data: NDArray, options: DataArrayOptions = {}) {
    // Initialize precision factor (default 6)
    // this._precisionFactor = Math.pow(10, this._precision);

    if (options.lazy) {
      if (!options.virtualShape) throw new Error('lazy DataArray requires virtualShape');
      if (!options.lazyLoader) throw new Error('lazy DataArray requires lazyLoader');
      const shape = [...options.virtualShape];
      this._shape = shape;
      this._attrs = options.attrs ? deepClone(options.attrs) : {};
      this._name = options.name;
      this._dims = options.dims ? [...options.dims] : shape.map((_, i) => `dim_${i}`);
      this._block = createLazyBlock(shape, options.lazyLoader);
      // Store original index mapping if provided (for chained selections)
      // Deep clone to prevent mutations from affecting the original
      if (options.originalIndexMapping) {
        this._originalIndexMapping = deepClone(options.originalIndexMapping);
      }
      // coords: do NOT enforce lengths; just store if provided, else generate index arrays by size
      this._coords = {};
      if (options.coords) {
        // Shallow copy to prevent array mutations (primitives are safe, objects share reference)
        for (const [dim, coords] of Object.entries(options.coords)) {
          this._coords[dim] = [...coords];
          // Previous approach with rounding (kept as backup):
          // this._coords[dim] = coords.map(c =>
          //   typeof c === 'number' ? this._roundPrecision(c) : c
          // );
        }
      }
      for (let i = 0; i < this._dims.length; i++) {
        const d = this._dims[i];
        if (!this._coords[d]) this._coords[d] = Array.from({ length: this._shape[i] }, (_, j) => j);
      }
      // Build dimension index map
      this._dims.forEach((dim, idx) => this._dimIndexMap.set(dim, idx));
      return;
    }

    const shape = getShape(data);
    this._block = createEagerBlock(data);
    this._shape = [...shape];
    this._attrs = options.attrs ? deepClone(options.attrs) : {};
    this._name = options.name;
    // Handle dimensions
    if (options.dims) {
      if (options.dims.length !== this._shape.length) {
        throw new Error(
          `Number of dimensions (${options.dims.length}) does not match data shape (${this._shape.length})`
        );
      }
      this._dims = [...options.dims];
    } else {
      // Auto-generate dimension names
      this._dims = this._shape.map((_, i) => `dim_${i}`);
    }

    // Build dimension index map for O(1) lookups (must be done before coordinate processing)
    this._dims.forEach((dim, idx) => this._dimIndexMap.set(dim, idx));

    // Handle coordinates
    this._coords = {};
    if (options.coords) {
      for (const [dim, coords] of Object.entries(options.coords)) {
        const dimIndex = this._getDimIndex(dim);
        if (dimIndex === -1) {
          throw new Error(`Coordinate dimension '${dim}' not found in dims`);
        }
        if (coords.length !== this._shape[dimIndex]) {
          throw new Error(
            `Coordinate '${dim}' length (${coords.length}) does not match dimension size (${this._shape[dimIndex]})`
          );
        }
        // Shallow copy to prevent array mutations (primitives are safe, objects share reference)
        this._coords[dim] = [...coords];
        // Previous approach with rounding (kept as backup):
        // this._coords[dim] = coords.map(c =>
        //   typeof c === 'number' ? this._roundPrecision(c) : c
        // );
      }
    }

    // Generate default coordinates for dimensions without coordinates
    for (let i = 0; i < this._dims.length; i++) {
      const dim = this._dims[i];
      if (!this._coords[dim]) {
        this._coords[dim] = Array.from({ length: this._shape[i] }, (_, j) => j);
      }
    }

    // Deep clone to prevent mutations from affecting the original
    if (options.originalIndexMapping) {
      this._originalIndexMapping = deepClone(options.originalIndexMapping);
    }
  }


  /**
   * Get the data as a native JavaScript array
   * NOTE: Returns direct reference for performance. Do not mutate the returned array.
   */
  get data(): NDArray {
    return this._block.materialize();
  }

  /**
   * Get the values (alias for data)
   */
  get values(): NDArray {
    return this.data;
  }

  /**
   * Get the dimensions
   */
  get dims(): DimensionName[] {
    return [...this._dims];
  }

  /**
   * Get the shape
   */
  get shape(): number[] {
    return [...this._shape];
  }

  /**
   * Get the coordinates
   * NOTE: Returns direct reference for performance. Do not mutate the returned object.
   */
  get coords(): Coordinates {
    return this._coords;
  }

  /**
   * Get the attributes
   * NOTE: Returns direct reference for performance. Do not mutate the returned object.
   */
  get attrs(): Attributes {
    return this._attrs;
  }

  /**
   * Get the name
   */
  get name(): string | undefined {
    return this._name;
  }

  /**
   * Get the number of dimensions
   */
  get ndim(): number {
    return this._dims.length;
  }

  /**
   * Get the total size (number of elements)
   */
  get size(): number {
    return this._shape.reduce((a, b) => a * b, 1);
  }

  /**
   * Check if the underlying data block is lazy
   */
  get isLazy(): boolean {
    return isLazyBlock(this._block);
  }

  /**
   * Materialize the DataArray if it is lazy. Returns the original instance for eager arrays.
   */
  async compute(): Promise<DataArray> {
    if (!isLazyBlock(this._block)) {
      return this;
    }

    const ranges: { [dimension: string]: { start: number; stop: number } } = {};
    for (let i = 0; i < this._dims.length; i++) {
      ranges[this._dims[i]] = { start: 0, stop: this._shape[i] };
    }

    const data = await this._block.fetch(ranges);

    return new DataArray(data, {
      dims: this._dims,
      coords: this._coords,
      attrs: deepClone(this._attrs),
      name: this._name
    });
  }

  /**
   * Select data by coordinate labels
   */
  async sel(selection: Selection, options?: SelectionOptions): Promise<DataArray> {
    // Check if this is a lazy-loaded array with a loader function
    if (isLazyBlock(this._block)) {
      const result = performLazySelection({
        selection,
        options,
        dims: this._dims,
        shape: this._shape,
        coords: this._coords,
        attrs: this._attrs,
        name: this._name,
        originalIndexMapping: this._originalIndexMapping,
        lazyLoader: this._block.fetch
      });

      // Check if all dimensions were dropped (scalar result)
      if (result.dims.length === 0) {
        const data = await result.lazyLoader({});
        return new DataArray(data, {
          dims: result.dims,
          coords: result.coords,
          attrs: result.attrs,
          name: result.name
        });
      }

      return new DataArray(null, {
        lazy: true,
        virtualShape: result.virtualShape,
        lazyLoader: result.lazyLoader,
        dims: result.dims,
        coords: result.coords,
        attrs: result.attrs,
        name: result.name,
        originalIndexMapping: result.originalIndexMapping
      });
    }

    const newData = this._selectData(selection, options);
    const newDims: DimensionName[] = [];
    const newCoords: Coordinates = {};
    const newShape = getShape(newData);

    let shapeIndex = 0;
    for (const dim of this._dims) {
      if (selection[dim] !== undefined) {
        const sel = selection[dim];
        if (typeof sel === 'number' || typeof sel === 'string') {
          // Single value selection - dimension is dropped
          continue;
        }
      }
      newDims.push(dim);

      if (shapeIndex < newShape.length) {
        // Generate coordinates for the new dimension
        if (selection[dim] !== undefined) {
          const sel = selection[dim];
          if (Array.isArray(sel)) {
            newCoords[dim] = sel as CoordinateValue[];
          } else if (typeof sel === 'object' && 'start' in sel) {
            const { start, stop } = sel;
            const coordSlice = this._getCoordinateSlice(dim, start, stop);
            newCoords[dim] = coordSlice;
          }
        } else {
          newCoords[dim] = this._coords[dim];
        }
        shapeIndex++;
      }
    }

    return new DataArray(newData, {
      dims: newDims,
      coords: newCoords,
      attrs: this._attrs,
      name: this._name
    });
  }

  /**
   * Stream data selection in chunks (useful for large datasets)
   * @param selection - Selection specification
   * @param options - Streaming options including chunk size and dimension
   * @returns AsyncGenerator yielding chunks with progress information
   *
   * @example
   * ```typescript
   * const stream = dataArray.selStream(
   *   { time: ['2020-01-01', '2020-12-31'] },
   *   { chunkSize: 50 } // 50MB chunks
   * );
   *
   * for await (const chunk of stream) {
   *   console.log(`Progress: ${chunk.progress}%`);
   *   processData(chunk.data);
   * }
   * ```
   */
  async *selStream(
    selection: Selection,
    options?: StreamOptions
  ): AsyncGenerator<StreamChunk<DataArray>> {
    const chunkSizeMB = options?.chunkSize || 100;
    const chunkSizeBytes = chunkSizeMB * 1024 * 1024;
    const method = options?.method;
    const tolerance = options?.tolerance;

    // Determine which dimension to chunk along
    const chunkDim = options?.dimension || this._selectChunkDimension(selection);
    const chunkDimIndex = this._getDimIndex(chunkDim);

    if (chunkDimIndex === -1) {
      throw new Error(`Dimension '${chunkDim}' not found in DataArray`);
    }

    // Calculate the range for the chunk dimension
    const dimSelection = selection[chunkDim];
    let startIdx: number;
    let endIdx: number;

    const coordAttrs = (this._attrs as any)?._coordAttrs;
    const chunkDimAttrs = coordAttrs?.[chunkDim] || this._attrs;

    if (dimSelection === undefined) {
      // No selection on chunk dimension - use full range
      startIdx = 0;
      endIdx = this._shape[chunkDimIndex] - 1;
    } else if (Array.isArray(dimSelection)) {
      // Array selection - get indices
      const indices = dimSelection.map(v => findCoordinateIndex(this._coords[chunkDim], v, { method, tolerance }, chunkDim, chunkDimAttrs));
      startIdx = Math.min(...indices);
      endIdx = Math.max(...indices);
    } else if (typeof dimSelection === 'object' && 'start' in dimSelection) {
      // Slice selection
      const { start, stop } = dimSelection;
      startIdx = start !== undefined ? findCoordinateIndex(this._coords[chunkDim], start, { method, tolerance }, chunkDim, chunkDimAttrs) : 0;
      endIdx = stop !== undefined ? findCoordinateIndex(this._coords[chunkDim], stop, { method, tolerance }, chunkDim, chunkDimAttrs) : this._shape[chunkDimIndex] - 1;
    } else {
      // Single value - no need to stream
      const result = await this.sel(selection, { method, tolerance });
      yield {
        data: result,
        progress: 100,
        bytesProcessed: this._estimateSize(result),
        totalBytes: this._estimateSize(result),
        chunkIndex: 0,
        totalChunks: 1
      };
      return;
    }

    // Calculate bytes per element along chunk dimension
    const bytesPerElement = 8; // Assume float64
    const elementsPerSlice = this._shape.reduce((acc, size, i) =>
      i === chunkDimIndex ? acc : acc * size, 1
    );
    const bytesPerSlice = elementsPerSlice * bytesPerElement;

    // Calculate chunk size in terms of dimension steps
    const stepsPerChunk = Math.max(1, Math.floor(chunkSizeBytes / bytesPerSlice));
    const totalSteps = endIdx - startIdx + 1;
    const totalChunks = Math.ceil(totalSteps / stepsPerChunk);
    const totalBytes = totalSteps * bytesPerSlice;

    let bytesProcessed = 0;

    // Iterate in chunks
    for (let chunkIdx = 0; chunkIdx < totalChunks; chunkIdx++) {
      const chunkStart = startIdx + chunkIdx * stepsPerChunk;
      const chunkEnd = Math.min(chunkStart + stepsPerChunk - 1, endIdx);

      // Build selection for this chunk
      const chunkSelection: Selection = { ...selection };
      const chunkCoords = this._coords[chunkDim].slice(chunkStart, chunkEnd + 1);

      if (chunkCoords.length === 1) {
        chunkSelection[chunkDim] = chunkCoords[0];
      } else {
        chunkSelection[chunkDim] = chunkCoords;
      }

      // Execute selection for this chunk
      const chunkData = await this.sel(chunkSelection, { method, tolerance });

      // Update progress
      const chunkBytes = (chunkEnd - chunkStart + 1) * bytesPerSlice;
      bytesProcessed += chunkBytes;
      const progress = Math.round((bytesProcessed / totalBytes) * 100);

      yield {
        data: chunkData,
        progress,
        bytesProcessed,
        totalBytes,
        chunkIndex: chunkIdx,
        totalChunks
      };
    }
  }

  /**
   * Select data by integer position
   * Supports negative indexing (Python-style): -1 = last, -2 = second-to-last, etc.
   */
  async isel(selection: { [dimension: string]: number | number[] }): Promise<DataArray> {
    const indexSelection: Selection = {};

    for (const [dim, sel] of Object.entries(selection)) {
      const coords = this._coords[dim];
      if (!coords) continue;

      if (typeof sel === 'number') {
        // Support negative indexing (Python-style: -1 = last, -2 = second-to-last, etc.)
        const index = sel < 0 ? coords.length + sel : sel;
        const coordValue = coords[index];
        indexSelection[dim] = coordValue;
      } else if (Array.isArray(sel)) {
        // Support negative indexing in arrays
        const coordValues = sel.map(i => {
          const index = i < 0 ? coords.length + i : i;
          return coords[index];
        });
        indexSelection[dim] = coordValues;
      }
    }

    return await this.sel(indexSelection);
  }

  /**
   * Reduce along a dimension
   */
  sum(dim?: DimensionName): DataArray | number {
    if (!dim) {
      // Sum all values using iterative approach (no flatten needed)
      return sumAll(this._block.materialize());
    }

    const dimIndex = this._getDimIndex(dim);
    if (dimIndex === -1) {
      throw new Error(`Dimension '${dim}' not found`);
    }

    const result = this._reduce(dimIndex, (acc, val) => acc + (val as number));
    const newDims = this._dims.filter((_, i) => i !== dimIndex);

    // If all dimensions are reduced, return a scalar
    if (newDims.length === 0) {
      return result as number;
    }

    const newCoords: Coordinates = {};

    for (const d of newDims) {
      newCoords[d] = this._coords[d];
    }

    return new DataArray(result, {
      dims: newDims,
      coords: newCoords,
      attrs: this._attrs,
      name: this._name
    });
  }

  /**
   * Mean along a dimension
   */
  mean(dim?: DimensionName): DataArray | number {
    if (!dim) {
      const data = this._block.materialize();
      const sum = sumAll(data);
      const count = countAll(data);
      return sum / count;
    }

    const dimIndex = this._getDimIndex(dim);
    if (dimIndex === -1) {
      throw new Error(`Dimension '${dim}' not found`);
    }

    const dimSize = this._shape[dimIndex];
    const sumResult = this._reduce(dimIndex, (acc, val) => acc + (val as number));
    const meanResult = divideArray(sumResult, dimSize);

    const newDims = this._dims.filter((_, i) => i !== dimIndex);

    // If all dimensions are reduced, return a scalar
    if (newDims.length === 0) {
      return meanResult as number;
    }

    const newCoords: Coordinates = {};

    for (const d of newDims) {
      newCoords[d] = this._coords[d];
    }

    return new DataArray(meanResult, {
      dims: newDims,
      coords: newCoords,
      attrs: this._attrs,
      name: this._name
    });
  }

  /**
   * Apply a condition and choose values between this array (x) and another (y)
   * Similar to xarray.DataArray.where. Broadcasts across shared dimensions.
   */
  where(
    cond: DataArray | DataValue,
    other: DataArray | DataValue | null = null,
    options?: WhereOptions
  ): DataArray {
    return DataArray.where(cond, this, other ?? null, options);
  }

  /**
   * Static version similar to xr.where(cond, x, y)
   */
  static where(
    cond: DataArray | DataValue,
    x: DataArray | DataValue,
    y: DataArray | DataValue,
    options?: WhereOptions
  ): DataArray {
    const condOperand = DataArray._normalizeOperand(cond, 'cond');
    const xOperand = DataArray._normalizeOperand(x, 'x');
    const yOperand = DataArray._normalizeOperand(y, 'y');

    if (
      (condOperand.kind === 'array' && isLazyBlock(condOperand.block)) ||
      (xOperand.kind === 'array' && isLazyBlock(xOperand.block)) ||
      (yOperand.kind === 'array' && isLazyBlock(yOperand.block))
    ) {
      throw new Error('where on lazy DataArray operands is not yet supported.');
    }

    const result = computeWhere(condOperand, xOperand, yOperand, options);

    return new DataArray(result.data, {
      dims: result.dims,
      coords: result.coords,
      attrs: result.attrs,
      name: result.name
    });
  }

  cloneWith(options?: { coords?: Coordinates; attrs?: Attributes; name?: string }): DataArray {
    const clone = Object.create(DataArray.prototype) as DataArray;
    clone._block = this._block;
    clone._dims = [...this._dims];
    clone._shape = [...this._shape];
    clone._precision = this._precision;
    clone._coords = options?.coords ? deepClone(options.coords) : deepClone(this._coords);
    clone._attrs = options?.attrs ? deepClone(options.attrs) : deepClone(this._attrs);
    clone._name = options?.name ?? this._name;
    return clone;
  }

  assignCoords(mapping: { [dimension: string]: CoordinateValue[] | DataArray }): DataArray {
    const updatedCoords = deepClone(this._coords);

    for (const [dim, value] of Object.entries(mapping)) {
      const dimIndex = this._getDimIndex(dim);
      if (dimIndex === -1) {
        throw new Error(`Cannot assign coordinates for non-existent dimension '${dim}'`);
      }

      let values: CoordinateValue[];

      if (value instanceof DataArray) {
        if (value.dims.length !== 1 || value.dims[0] !== dim) {
          throw new Error(`Coordinate DataArray for '${dim}' must be 1D and share the same dimension`);
        }

        const data = value.data;
        if (!Array.isArray(data)) {
          throw new Error(`Coordinate DataArray for '${dim}' must be 1D`);
        }

        values = (data as CoordinateValue[]).map(v =>
          v instanceof Date ? new Date(v.getTime()) : v
        );
      } else if (Array.isArray(value)) {
        values = value.map(v => (v instanceof Date ? new Date(v.getTime()) : v));
      } else {
        throw new Error(`assignCoords requires an array or DataArray for dimension '${dim}'`);
      }

      if (values.length !== this._shape[dimIndex]) {
        throw new Error(
          `Coordinate length for '${dim}' (${values.length}) does not match dimension size (${this._shape[dimIndex]})`
        );
      }

      updatedCoords[dim] = values;
    }

    return this.cloneWith({ coords: updatedCoords });
  }

  squeeze(): DataArray {
    const newDims: DimensionName[] = [];
    const newCoords: Coordinates = {};
    const squeezedIndices: number[] = [];

    for (let i = 0; i < this._dims.length; i++) {
      const dim = this._dims[i];
      const size = this._shape[i];
      if (size === 1) {
        squeezedIndices.push(i);
        continue;
      }
      newDims.push(dim);
      newCoords[dim] = this._coords[dim];
    }

    if (newDims.length === this._dims.length) {
      return this;
    }

    const newData = reshapeSqueezed(this._block.materialize(), squeezedIndices);

    return new DataArray(newData, {
      dims: newDims,
      coords: newCoords,
      attrs: deepClone(this._attrs),
      name: this._name
    });
  }

  rolling(dim: DimensionName, window: number, options?: RollingOptions): DataArrayRolling {
    return new DataArrayRolling(this, dim, window, options ?? {});
  }

  add(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this._binaryOperation(
      other,
      (left, right) => DataArray._numericBinary('add', left, right, (a, b) => a + b),
      options,
      { keepAttrs: 'left', preferNameFrom: 'left' }
    );
  }

  subtract(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this._binaryOperation(
      other,
      (left, right) => DataArray._numericBinary('subtract', left, right, (a, b) => a - b),
      options,
      { keepAttrs: 'left', preferNameFrom: 'left' }
    );
  }

  sub(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this.subtract(other, options);
  }

  multiply(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this._binaryOperation(
      other,
      (left, right) => DataArray._numericBinary('multiply', left, right, (a, b) => a * b),
      options,
      { keepAttrs: 'left', preferNameFrom: 'left' }
    );
  }

  mul(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this.multiply(other, options);
  }

  divide(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this._binaryOperation(
      other,
      (left, right) => DataArray._numericBinary('divide', left, right, (a, b) => a / b),
      options,
      { keepAttrs: 'left', preferNameFrom: 'left' }
    );
  }

  div(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this.divide(other, options);
  }

  power(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this._binaryOperation(
      other,
      (left, right) => DataArray._numericBinary('power', left, right, (a, b) => Math.pow(a, b)),
      options,
      { keepAttrs: 'left', preferNameFrom: 'left' }
    );
  }

  pow(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this.power(other, options);
  }

  greaterThan(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this._binaryOperation(
      other,
      (left, right) => DataArray._numericComparison('greaterThan', left, right, (a, b) => a > b),
      options
    );
  }

  gt(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this.greaterThan(other, options);
  }

  greaterEqual(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this._binaryOperation(
      other,
      (left, right) => DataArray._numericComparison('greaterEqual', left, right, (a, b) => a >= b),
      options
    );
  }

  ge(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this.greaterEqual(other, options);
  }

  lessThan(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this._binaryOperation(
      other,
      (left, right) => DataArray._numericComparison('lessThan', left, right, (a, b) => a < b),
      options
    );
  }

  lt(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this.lessThan(other, options);
  }

  lessEqual(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this._binaryOperation(
      other,
      (left, right) => DataArray._numericComparison('lessEqual', left, right, (a, b) => a <= b),
      options
    );
  }

  le(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this.lessEqual(other, options);
  }

  equal(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this._binaryOperation(
      other,
      (left, right) => DataArray._equalityComparison(left, right, (a, b) => a === b),
      options
    );
  }

  eq(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this.equal(other, options);
  }

  notEqual(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this._binaryOperation(
      other,
      (left, right) => DataArray._equalityComparison(left, right, (a, b) => a !== b),
      options
    );
  }

  ne(other: DataArray | DataValue, options?: BinaryOpOptions): DataArray {
    return this.notEqual(other, options);
  }

  /**
   * Convert to a plain JavaScript object
   */
  toObject(): any {
    return {
      data: this.data,
      dims: this.dims,
      coords: this.coords,
      attrs: this.attrs,
      name: this.name,
      shape: this.shape
    };
  }

  /**
   * Convert to JSON string
   */
  toJSON(): string {
    return JSON.stringify(this.toObject());
  }

  /**
   * Convert to an array of records with coordinates and values
   * Each record contains coordinate values and the data value
   * Time coordinates are automatically converted to ISO datetime strings
   * Numeric coordinates are rounded to avoid floating-point precision errors
   *
   * @param options - Optional configuration
   * @param options.precision - Number of decimal places to round coordinate values (default: 6)
   *
   * @example
   * ```typescript
   * // For a 2D array with dims ['time', 'lat']
   * dataArray.toRecords()
   * // Returns:
   * // [
   * //   { time: '2020-01-01T00:00:00', lat: 45.5, value: 23.4 },
   * //   { time: '2020-01-02T00:00:00', lat: 46.0, value: 24.1 },
   * //   ...
   * // ]
   *
   * // Custom precision
   * dataArray.toRecords({ precision: 2 })
   * // [
   * //   { time: '2020-01-01T00:00:00', lat: 45.50, lon: -73.25, value: 23.4 },
   * //   ...
   * // ]
   * ```
   */
  toRecords(options?: { precision?: number }): Array<Record<string, any>> {
    const precision = options?.precision !== undefined ? options.precision : 6;
    const records: Array<Record<string, any>> = [];
    const flatData = flatten(this._block.materialize());

    // Helper function to round numbers
    const round = (value: number): number => {
      const factor = Math.pow(10, precision);
      return Math.round(value * factor) / factor;
    };

    // Pre-check which dimensions are time coordinates
    const timeCoordInfo: { [dim: string]: string } = {};
    const coordAttrs = (this._attrs as any)?._coordAttrs;

    for (const dim of this._dims) {
      const dimAttrs = coordAttrs?.[dim] || this._attrs;
      if (isTimeCoordinate(dimAttrs)) {
        const units = dimAttrs?.units as string | undefined;
        if (units) {
          timeCoordInfo[dim] = units;
        }
      }
    }

    // Calculate indices for each element in the flattened array
    const totalElements = flatData.length;

    for (let i = 0; i < totalElements; i++) {
      const record: Record<string, any> = {};

      // Calculate multi-dimensional indices
      let remaining = i;
      const indices: number[] = [];

      for (let d = this._dims.length - 1; d >= 0; d--) {
        const dimSize = this._shape[d];
        indices[d] = remaining % dimSize;
        remaining = Math.floor(remaining / dimSize);
      }

      // Add coordinate values to record
      for (let d = 0; d < this._dims.length; d++) {
        const dim = this._dims[d];
        const coordIndex = indices[d];
        let coordValue = this._coords[dim][coordIndex];
        // check if coordValue is a Date object
        if (coordValue instanceof Date) {
          coordValue = coordValue.toISOString();
        }

        // Convert time coordinates to datetime strings
        if (timeCoordInfo[dim] && typeof coordValue === 'number') {
          const units = timeCoordInfo[dim];
          const convertedDate = cfTimeToDate(coordValue, units);
          if (convertedDate) {
            coordValue = convertedDate.toISOString();
          }
        } else if (typeof coordValue === 'number') {
          // Round numeric coordinates to avoid floating-point precision errors
          coordValue = round(coordValue);
        }

        record[dim] = coordValue;
      }

      // Add data value
      record.value = flatData[i];

      records.push(record);
    }

    return records;
  }


  /**
   * Get the bounding box for spatial coordinates
   * @param options - Optional configuration
   * @param options.latDim - Name of the latitude dimension (defaults to 'latitude' or 'lat')
   * @param options.lonDim - Name of the longitude dimension (defaults to 'longitude' or 'lon')
   * @param options.precision - Number of decimal places to round to (default: 6, set to null for no rounding)
   * @returns Bounding box with min/max lat/lon, or undefined if spatial dims not found
   *
   * @example
   * ```typescript
   * const bounds = dataArray.getBounds();
   * // Returns: { latMin: 30, latMax: 50, lonMin: -120, lonMax: -70 }
   *
   * const bounds = dataArray.getBounds({ precision: 2 });
   * // Returns: { latMin: 30.12, latMax: 50.45, lonMin: -120.34, lonMax: -70.89 }
   * ```
   */
  getBounds(options?: { latDim?: string; lonDim?: string; precision?: number | null }): { latMin: number; latMax: number; lonMin: number; lonMax: number } | undefined {
    const precision = options?.precision !== undefined ? options.precision : 6;

    // Auto-detect latitude dimension
    const latDimName = options?.latDim ||
      this._dims.find(d => d === 'latitude' || d === 'lat' || d === 'y');

    // Auto-detect longitude dimension
    const lonDimName = options?.lonDim ||
      this._dims.find(d => d === 'longitude' || d === 'lon' || d === 'x');

    if (!latDimName || !lonDimName) {
      return undefined;
    }

    const latCoords = this._coords[latDimName];
    const lonCoords = this._coords[lonDimName];

    if (!latCoords || !lonCoords || latCoords.length === 0 || lonCoords.length === 0) {
      return undefined;
    }

    const latValues = latCoords.filter(v => typeof v === 'number') as number[];
    const lonValues = lonCoords.filter(v => typeof v === 'number') as number[];

    if (latValues.length === 0 || lonValues.length === 0) {
      return undefined;
    }

    const round = (value: number): number => {
      if (precision === null) return value;
      const factor = Math.pow(10, precision);
      return Math.round(value * factor) / factor;
    };

    return {
      latMin: round(Math.min(...latValues)),
      latMax: round(Math.max(...latValues)),
      lonMin: round(Math.min(...lonValues)),
      lonMax: round(Math.max(...lonValues))
    };
  }

  /**
   * Get dimension index with O(1) lookup using cached map
   */
  private _getDimIndex(dim: DimensionName): number {
    const index = this._dimIndexMap.get(dim);
    if (index === undefined) {
      return -1;
    }
    return index;
  }


  /**
   * Round a number to the specified precision
   */
  // private _roundPrecision(value: number): number {
  //   return Math.round(value * this._precisionFactor) / this._precisionFactor;
  // }

  /**
   * Calculate and determine the time resolution from the time coordinate
   * Automatically detects if data is hourly, daily, weekly, monthly, yearly, etc.
   * Uses CF-time utilities to properly handle time units
   * Stores resolution info in attrs
   *
   * @param timeDim - Name of the time dimension (defaults to 'time')
   * @returns Object with resolution value and type, or undefined if cannot be determined
   *
   * @example
   * ```typescript
   * const result = dataArray.calculateTimeResolution();
   * // Returns: { value: 1, type: 'daily', unit: 'days' }
   * // attrs.time_resolution = 1
   * // attrs.time_resolution_type = 'daily'
   * // attrs.time_resolution_unit = 'days'
   * ```
   */
  calculateTimeResolution(timeDim: string = 'time'): { value: number; type: string; unit: string } | undefined {
    if (!this._dims.includes(timeDim)) {
      return undefined;
    }

    const timeCoords = this._coords[timeDim];
    if (!timeCoords || timeCoords.length < 2) {
      return undefined;
    }

    let first = timeCoords[0];
    let second = timeCoords[1];

    // Convert date strings to seconds for calculation
    if (typeof first === 'object') {
      first = first.getTime() / 1000;
    }

    if (typeof second === 'object') {
      second = second.getTime() / 1000;
    }

    if (typeof first !== 'number' || typeof second !== 'number') {
      return undefined;
    }

    const resolution = Math.abs(second - first);

    // Get coordinate attributes to determine the time unit
    const coordAttrs = (this._attrs as any)?._coordAttrs;
    const dimAttrs = coordAttrs?.[timeDim] || this._attrs;
    const unitsStr = dimAttrs?.units as string | undefined;

    // Parse CF time units to get base unit
    let baseUnit = 'seconds';
    if (unitsStr) {
      const parsed = parseCFTimeUnits(unitsStr);
      if (parsed) {
        baseUnit = parsed.unit;
      }
    }

    // Determine resolution type based on value and base unit
    let type: string;
    let outputUnit: string;
    let outputValue: number = resolution;

    const tolerance = 0.1;

    // Check for common patterns based on base unit
    if (baseUnit === 'second') {
      if (Math.abs(resolution - 3600) / 3600 < tolerance) {
        type = 'hourly';
        outputUnit = 'hours';
        outputValue = resolution / 3600;
      } else if (Math.abs(resolution - 10800) / 10800 < tolerance) {
        type = '3-hourly';
        outputUnit = 'hours';
        outputValue = resolution / 3600;
      } else if (Math.abs(resolution - 21600) / 21600 < tolerance) {
        type = '6-hourly';
        outputUnit = 'hours';
        outputValue = resolution / 3600;
      } else if (Math.abs(resolution - 86400) / 86400 < tolerance) {
        type = 'daily';
        outputUnit = 'days';
        outputValue = resolution / 86400;
      } else {
        type = 'custom';
        outputUnit = 'seconds';
      }
    } else if (baseUnit === 'minute') {
      if (Math.abs(resolution - 60) / 60 < tolerance) {
        type = 'hourly';
        outputUnit = 'hours';
        outputValue = resolution / 60;
      } else if (Math.abs(resolution - 1440) / 1440 < tolerance) {
        type = 'daily';
        outputUnit = 'days';
        outputValue = resolution / 1440;
      } else {
        type = 'custom';
        outputUnit = 'minutes';
      }
    } else if (baseUnit === 'hour') {
      if (Math.abs(resolution - 1) < tolerance) {
        type = 'hourly';
        outputUnit = 'hours';
      } else if (Math.abs(resolution - 3) / 3 < tolerance) {
        type = '3-hourly';
        outputUnit = 'hours';
      } else if (Math.abs(resolution - 6) / 6 < tolerance) {
        type = '6-hourly';
        outputUnit = 'hours';
      } else if (Math.abs(resolution - 24) / 24 < tolerance) {
        type = 'daily';
        outputUnit = 'days';
        outputValue = resolution / 24;
      } else {
        type = 'custom';
        outputUnit = 'hours';
      }
    } else if (baseUnit === 'day') {
      if (Math.abs(resolution - 1) < tolerance) {
        type = 'daily';
        outputUnit = 'days';
      } else if (Math.abs(resolution - 7) / 7 < tolerance) {
        type = 'weekly';
        outputUnit = 'days';
      } else if (Math.abs(resolution - 30) / 30 < tolerance || Math.abs(resolution - 31) / 31 < tolerance) {
        type = 'monthly';
        outputUnit = 'months';
        outputValue = 1;
      } else if (Math.abs(resolution - 365) / 365 < tolerance || Math.abs(resolution - 366) / 366 < tolerance) {
        type = 'yearly';
        outputUnit = 'years';
        outputValue = 1;
      } else {
        type = 'custom';
        outputUnit = 'days';
      }
    } else if (baseUnit === 'month') {
      if (Math.abs(resolution - 1) < tolerance) {
        type = 'monthly';
        outputUnit = 'months';
      } else if (Math.abs(resolution - 12) / 12 < tolerance) {
        type = 'yearly';
        outputUnit = 'years';
        outputValue = 1;
      } else {
        type = 'custom';
        outputUnit = 'months';
      }
    } else if (baseUnit === 'year') {
      if (Math.abs(resolution - 1) < tolerance) {
        type = 'yearly';
        outputUnit = 'years';
      } else {
        type = 'custom';
        outputUnit = 'years';
      }
    } else {
      type = 'custom';
      outputUnit = baseUnit;
    }

    // Store in attributes
    this._attrs.time_resolution = outputValue;
    this._attrs.time_resolution_type = type;
    this._attrs.time_resolution_unit = outputUnit;

    return {
      value: outputValue,
      type,
      unit: outputUnit
    };
  }

  // Private helper methods

  private _toOperand(): ArrayWhereOperand {
    return {
      kind: 'array',
      block: this._block,
      dims: this.dims,
      coords: this.coords,
      attrs: this.attrs,
      name: this._name
    };
  }

  private static _normalizeOperand(
    value: DataArray | DataValue | null,
    label: string
  ): WhereOperand {
    if (value instanceof DataArray) {
      return value._toOperand();
    }

    if (value === undefined) {
      throw new Error(`Missing operand '${label}' for DataArray operation`);
    }

    if (Array.isArray(value)) {
      throw new Error('Raw array operands are not yet supported. Use DataArray instances instead.');
    }

    return {
      kind: 'scalar',
      value: value as DataValue
    };
  }

  private _binaryOperation(
    other: DataArray | DataValue,
    operator: (left: DataValue, right: DataValue) => DataValue,
    options?: BinaryOpOptions,
    defaults?: BinaryOpOptions
  ): DataArray {
    const leftOperand = this._toOperand();
    const rightOperand = DataArray._normalizeOperand(other, 'other');

    if (isLazyBlock(leftOperand.block) || (rightOperand.kind === 'array' && isLazyBlock(rightOperand.block))) {
      throw new Error('Binary operations on lazy DataArray operands are not yet supported.');
    }
    const keepAttrs = options?.keepAttrs ?? defaults?.keepAttrs;
    const preferNameFrom = options?.preferNameFrom ?? defaults?.preferNameFrom;

    const result = computeBinaryOp(
      leftOperand,
      rightOperand,
      operator,
      keepAttrs === undefined && preferNameFrom === undefined
        ? undefined
        : { keepAttrs, preferNameFrom }
    );

    return new DataArray(result.data, {
      dims: result.dims,
      coords: result.coords,
      attrs: result.attrs,
      name: result.name
    });
  }

  private static _numericBinary(
    opName: string,
    left: DataValue,
    right: DataValue,
    op: (a: number, b: number) => number
  ): number {
    if (typeof left !== 'number' || typeof right !== 'number') {
      throw new Error(
        `Operation '${opName}' requires numeric operands; received ${typeof left} and ${typeof right}`
      );
    }
    return op(left, right);
  }

  private static _numericComparison(
    opName: string,
    left: DataValue,
    right: DataValue,
    op: (a: number, b: number) => boolean
  ): boolean {
    if (typeof left !== 'number' || typeof right !== 'number') {
      throw new Error(
        `Comparison '${opName}' requires numeric operands; received ${typeof left} and ${typeof right}`
      );
    }
    return op(left, right);
  }

  private static _equalityComparison(
    left: DataValue,
    right: DataValue,
    op: (a: DataValue, b: DataValue) => boolean
  ): boolean {
    return op(left, right);
  }


  private _selectData(selection: Selection, options?: SelectionOptions): NDArray {
    let result: any = this._block.materialize();
    let dimensionsDropped = 0;

    for (let i = 0; i < this._dims.length; i++) {
      const dim = this._dims[i];
      const sel = selection[dim];

      if (sel === undefined) {
        continue;
      }

      // Adjust dimension index for already-dropped dimensions
      const currentDimIndex = i - dimensionsDropped;

      if (typeof sel === 'number' || typeof sel === 'string' || typeof sel === 'bigint' || sel instanceof Date) {
        // Single value selection - this will drop a dimension
        const coordAttrs = (this._attrs as any)?._coordAttrs;
        const dimAttrs = coordAttrs?.[dim] || this._attrs;
        const index = findCoordinateIndex(this._coords[dim], sel, options, dim, dimAttrs);
        result = selectAtDimension(result, currentDimIndex, index);
        dimensionsDropped++;
      } else if (Array.isArray(sel)) {
        // Multiple values selection
        const coordAttrs = (this._attrs as any)?._coordAttrs;
        const dimAttrs = coordAttrs?.[dim] || this._attrs;
        const indices = sel.map(v => findCoordinateIndex(this._coords[dim], v, options, dim, dimAttrs));
        result = selectMultipleAtDimension(result, currentDimIndex, indices);
      } else if (typeof sel === 'object' && 'start' in sel) {
        // Slice selection
        const { start, stop } = sel;
        const coordAttrs = (this._attrs as any)?._coordAttrs;
        const dimAttrs = coordAttrs?.[dim] || this._attrs;
        const startIndex = start !== undefined ? findCoordinateIndex(this._coords[dim], start, options, dim, dimAttrs) : 0;
        const stopIndex = stop !== undefined ? findCoordinateIndex(this._coords[dim], stop, options, dim, dimAttrs) + 1 : this._shape[i];
        result = sliceAtDimension(result, currentDimIndex, startIndex, stopIndex);
      }
    }

    return result;
  }



  private _getCoordinateSlice(dim: DimensionName, start?: CoordinateValue, stop?: CoordinateValue): CoordinateValue[] {
    const coords = this._coords[dim];
    const startIndex = start !== undefined ? coords.indexOf(start) : 0;
    const stopIndex = stop !== undefined ? coords.indexOf(stop) + 1 : coords.length;
    return coords.slice(startIndex, stopIndex);
  }

  private _reduce(dimIndex: number, reducer: (acc: number, val: number) => number): any {
    if (dimIndex === 0) {
      // Reducing the first dimension
      const data = this._block.materialize() as any[];
      if (!Array.isArray(data) || data.length === 0) {
        return data;
      }

      // Check if elements are arrays (multi-dimensional)
      if (Array.isArray(data[0])) {
        // Element-wise reduction across first dimension
        return data.reduce((acc: any, row: any) => {
          if (!acc) return deepClone(row);
          if (Array.isArray(row)) {
            return elementWiseOp(acc, row, reducer);
          }
          return reducer(acc as number, row as number);
        });
      } else {
        // Simple 1D reduction
        return data.reduce((acc: number, val: any) => reducer(acc, val as number), 0);
      }
    } else {
      // Reducing a later dimension - recurse into structure
      const data = this._block.materialize() as any[];
      return data.map((item: any) => {
        const subArray = new DataArray(item, {
          dims: this._dims.slice(1),
          coords: Object.fromEntries(
            Object.entries(this._coords).filter(([k]) => this._dims.slice(1).includes(k))
          )
        });
        const result = subArray._reduce(dimIndex - 1, reducer);
        return result;
      });
    }
  }



  /**
   * Select best dimension to chunk along for streaming
   */
  private _selectChunkDimension(selection: Selection): DimensionName {
    // Prefer dimensions with range selections
    for (const dim of this._dims) {
      const sel = selection[dim];
      if (Array.isArray(sel) || (typeof sel === 'object' && 'start' in sel)) {
        return dim;
      }
    }

    // Fall back to first dimension (usually time for climate data)
    return this._dims[0];
  }

  /**
   * Estimate size in bytes for a DataArray
   */
  private _estimateSize(dataArray: DataArray): number {
    const bytesPerElement = 8; // Assume float64
    return dataArray.size * bytesPerElement;
  }
}

class DataArrayRolling {
  private readonly _dimIndex: number;
  private readonly _options: RollingOptions;
  private readonly _window: number;

  constructor(
    private readonly _source: DataArray,
    private readonly _dim: DimensionName,
    window: number,
    options: RollingOptions
  ) {
    if (window <= 0 || !Number.isFinite(window)) {
      throw new Error('rolling window must be a positive integer');
    }
    const dimIndex = _source.dims.indexOf(_dim);
    if (dimIndex === -1) {
      throw new Error(`Dimension '${_dim}' not found in DataArray`);
    }

    this._dimIndex = dimIndex;
    this._window = Math.floor(window);
    this._options = options;
  }

  mean(): DataArray {
    return this._apply('mean');
  }

  sum(): DataArray {
    return this._apply('sum');
  }

  private _apply(reducer: 'mean' | 'sum'): DataArray {
    const data = applyRolling(
      this._source.data,
      this._dimIndex,
      this._window,
      this._options,
      reducer
    );

    return new DataArray(data, {
      dims: this._source.dims,
      coords: this._source.coords,
      attrs: this._source.attrs,
      name: this._source.name
    });
  }
}
