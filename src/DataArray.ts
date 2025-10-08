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
  StreamChunk
} from './types';
import { getShape, flatten, reshape, deepClone } from './utils';
import { isTimeCoordinate, parseCFTimeUnits } from './cf-time';

export class DataArray {
  private _data: NDArray;
  private _dims: DimensionName[];
  private _coords: Coordinates;
  private _attrs: Attributes;
  private _name?: string;
  private _shape: number[];
  private _precision: number = 6;

  constructor(data: NDArray, options: DataArrayOptions = {}) {
    if (options.lazy) {
      if (!options.virtualShape) throw new Error("lazy DataArray requires virtualShape");
      this._data = [];                           // no allocation
      this._shape = [...options.virtualShape];   // real shape
      this._attrs = options.attrs || {};
      this._name = options.name;
      this._dims = options.dims ? [...options.dims] : this._shape.map((_, i) => `dim_${i}`);
      // coords: do NOT enforce lengths; just store if provided, else generate index arrays by size
      this._coords = {};
      if (options.coords) {
        // Deep clone and round numeric coordinates
        for (const [dim, coords] of Object.entries(options.coords)) {
          this._coords[dim] = coords.map(c =>
            typeof c === 'number' ? this._roundPrecision(c) : c
          );
        }
      }
      for (let i = 0; i < this._dims.length; i++) {
        const d = this._dims[i];
        if (!this._coords[d]) this._coords[d] = Array.from({ length: this._shape[i] }, (_, j) => j);
      }
      return;
    }

    this._data = deepClone(data);
    this._shape = getShape(data);
    this._attrs = options.attrs || {};
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

    // Handle coordinates
    this._coords = {};
    if (options.coords) {
      for (const [dim, coords] of Object.entries(options.coords)) {
        const dimIndex = this._dims.indexOf(dim);
        if (dimIndex === -1) {
          throw new Error(`Coordinate dimension '${dim}' not found in dims`);
        }
        if (coords.length !== this._shape[dimIndex]) {
          throw new Error(
            `Coordinate '${dim}' length (${coords.length}) does not match dimension size (${this._shape[dimIndex]})`
          );
        }
        // Round numeric coordinates to specified precision
        this._coords[dim] = coords.map(c =>
          typeof c === 'number' ? this._roundPrecision(c) : c
        );
      }
    }

    // Generate default coordinates for dimensions without coordinates
    for (let i = 0; i < this._dims.length; i++) {
      const dim = this._dims[i];
      if (!this._coords[dim]) {
        this._coords[dim] = Array.from({ length: this._shape[i] }, (_, j) => j);
      }
    }
  }

  /**
   * Get the data as a native JavaScript array
   */
  get data(): NDArray {
    return deepClone(this._data);
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
   */
  get coords(): Coordinates {
    return deepClone(this._coords);
  }

  /**
   * Get the attributes
   */
  get attrs(): Attributes {
    return deepClone(this._attrs);
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
   * Select data by coordinate labels
   */
  async sel(selection: Selection, options?: SelectionOptions): Promise<DataArray> {
    // Check if this is a lazy-loaded array with a loader function
    if (this._attrs._lazy && this._attrs._lazyLoader) {
      return this._selLazy(selection, options);
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
    const chunkDimIndex = this._dims.indexOf(chunkDim);

    if (chunkDimIndex === -1) {
      throw new Error(`Dimension '${chunkDim}' not found in DataArray`);
    }

    // Calculate the range for the chunk dimension
    const dimSelection = selection[chunkDim];
    let startIdx: number;
    let endIdx: number;

    if (dimSelection === undefined) {
      // No selection on chunk dimension - use full range
      startIdx = 0;
      endIdx = this._shape[chunkDimIndex] - 1;
    } else if (Array.isArray(dimSelection)) {
      // Array selection - get indices
      const indices = dimSelection.map(v => this._findCoordinateIndex(chunkDim, v, { method, tolerance }));
      startIdx = Math.min(...indices);
      endIdx = Math.max(...indices);
    } else if (typeof dimSelection === 'object' && 'start' in dimSelection) {
      // Slice selection
      const { start, stop } = dimSelection;
      startIdx = start !== undefined ? this._findCoordinateIndex(chunkDim, start, { method, tolerance }) : 0;
      endIdx = stop !== undefined ? this._findCoordinateIndex(chunkDim, stop, { method, tolerance }) : this._shape[chunkDimIndex] - 1;
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
   */
  async isel(selection: { [dimension: string]: number | number[] }): Promise<DataArray> {
    const indexSelection: Selection = {};

    for (const [dim, sel] of Object.entries(selection)) {
      if (typeof sel === 'number') {
        // Convert index to coordinate value
        indexSelection[dim] = this._coords[dim][sel];
      } else if (Array.isArray(sel)) {
        const coords = sel.map(i => this._coords[dim][i]);
        indexSelection[dim] = coords;
      }
    }

    return await this.sel(indexSelection);
  }

  /**
   * Reduce along a dimension
   */
  sum(dim?: DimensionName): DataArray | number {
    if (!dim) {
      // Sum all values
      const flatData = flatten(this._data);
      return flatData.reduce((a, b) => (a as number) + (b as number), 0) as number;
    }

    const dimIndex = this._dims.indexOf(dim);
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
      const flatData = flatten(this._data);
      const sum = flatData.reduce((a, b) => (a as number) + (b as number), 0) as number;
      return sum / flatData.length;
    }

    const dimIndex = this._dims.indexOf(dim);
    if (dimIndex === -1) {
      throw new Error(`Dimension '${dim}' not found`);
    }

    const dimSize = this._shape[dimIndex];
    const sumResult = this._reduce(dimIndex, (acc, val) => acc + (val as number));
    const meanResult = this._divideArray(sumResult, dimSize);
    
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
    const flatData = flatten(this._data);

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

        // Convert time coordinates to datetime strings
        if (timeCoordInfo[dim] && typeof coordValue === 'number') {
          const units = timeCoordInfo[dim];
          const date = parseCFTimeUnits(units);
          if (date) {
            const convertedDate = this._convertCFTimeToDate(coordValue, date.unit, date.referenceDate);
            if (convertedDate) {
              coordValue = convertedDate.toISOString();
            }
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
   * Convert CF time value to Date
   * Helper method for toRecords
   */
  private _convertCFTimeToDate(value: number, unit: string, referenceDate: Date): Date | null {
    const refTime = referenceDate.getTime();
    let milliseconds: number;

    switch (unit) {
      case 'second':
        milliseconds = value * 1000;
        break;
      case 'minute':
        milliseconds = value * 60 * 1000;
        break;
      case 'hour':
        milliseconds = value * 60 * 60 * 1000;
        break;
      case 'day':
        milliseconds = value * 24 * 60 * 60 * 1000;
        break;
      case 'week':
        milliseconds = value * 7 * 24 * 60 * 60 * 1000;
        break;
      case 'month':
        milliseconds = value * 30 * 24 * 60 * 60 * 1000;
        break;
      case 'year':
        milliseconds = value * 365.25 * 24 * 60 * 60 * 1000;
        break;
      default:
        return null;
    }

    return new Date(refTime + milliseconds);
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
   * Round a number to the specified precision
   */
  private _roundPrecision(value: number): number {
    const factor = Math.pow(10, this._precision);
    return Math.round(value * factor) / factor;
  }

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

    const first = timeCoords[0];
    const second = timeCoords[1];

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

  private async _selLazy(selection: Selection, options?: SelectionOptions): Promise<DataArray> {
    const loader = this._attrs._lazyLoader;

    // Build index ranges for each dimension
    const indexRanges: { [dim: string]: { start: number; stop: number } | number } = {};
    const newDims: DimensionName[] = [];
    const newCoords: Coordinates = {};

    for (let i = 0; i < this._dims.length; i++) {
      const dim = this._dims[i];
      const sel = selection[dim];

      if (sel === undefined) {
        // No selection on this dimension - select all
        indexRanges[dim] = { start: 0, stop: this._shape[i] };
        newDims.push(dim);
        newCoords[dim] = this._coords[dim];
      } else if (typeof sel === 'number' || typeof sel === 'string' || sel instanceof Date) {
        // Single value - dimension will be dropped
        const index = this._findCoordinateIndex(dim, sel, options);
        indexRanges[dim] = index;
        // Don't add to newDims or newCoords
      } else if (Array.isArray(sel)) {
        // Multiple values selection - convert to range
        const indices = sel.map(v => this._findCoordinateIndex(dim, v, options));
        const minIdx = Math.min(...indices);
        const maxIdx = Math.max(...indices);
        indexRanges[dim] = { start: minIdx, stop: maxIdx + 1 };
        newDims.push(dim);
        newCoords[dim] = this._coords[dim].slice(minIdx, maxIdx + 1);
      } else if (typeof sel === 'object' && 'start' in sel) {
        // Slice selection
        const { start, stop } = sel;
        const startIndex = start !== undefined ? this._findCoordinateIndex(dim, start, options) : 0;
        const stopIndex = stop !== undefined ? this._findCoordinateIndex(dim, stop, options) + 1 : this._shape[i];
        indexRanges[dim] = { start: startIndex, stop: stopIndex };
        newDims.push(dim);
        newCoords[dim] = this._coords[dim].slice(startIndex, stopIndex);
      }
    }

    try {
      const data = await loader(indexRanges);

      if (!data) {
        throw new Error('Lazy loader returned undefined/null data');
      }

      return new DataArray(data, {
        dims: newDims,
        coords: newCoords,
        attrs: { ...this._attrs, _lazy: false, _lazyLoader: undefined }, // Mark as no longer lazy
        name: this._name
      });
    } catch (error) {
      console.error('Error in lazy loader:', error);
      throw error;
    }
  }

  private _selectData(selection: Selection, options?: SelectionOptions): NDArray {
    let result: any = this._data;
    let dimensionsDropped = 0;

    for (let i = 0; i < this._dims.length; i++) {
      const dim = this._dims[i];
      const sel = selection[dim];

      if (sel === undefined) {
        continue;
      }

      // Adjust dimension index for already-dropped dimensions
      const currentDimIndex = i - dimensionsDropped;

      if (typeof sel === 'number' || typeof sel === 'string' || sel instanceof Date) {
        // Single value selection - this will drop a dimension
        const index = this._findCoordinateIndex(dim, sel, options);
        result = this._selectAtDimension(result, currentDimIndex, index);
        dimensionsDropped++;
      } else if (Array.isArray(sel)) {
        // Multiple values selection
        const indices = sel.map(v => this._findCoordinateIndex(dim, v, options));
        result = this._selectMultipleAtDimension(result, currentDimIndex, indices);
      } else if (typeof sel === 'object' && 'start' in sel) {
        // Slice selection
        const { start, stop } = sel;
        const startIndex = start !== undefined ? this._findCoordinateIndex(dim, start, options) : 0;
        const stopIndex = stop !== undefined ? this._findCoordinateIndex(dim, stop, options) + 1 : this._shape[i];
        result = this._sliceAtDimension(result, currentDimIndex, startIndex, stopIndex);
      }
    }

    return result;
  }

  private _findCoordinateIndex(dim: DimensionName, value: CoordinateValue, options?: SelectionOptions): number {
    const coords = this._coords[dim];
    const method = options?.method;
    const tolerance = options?.tolerance;

    // Handle time coordinate conversion for string dates
    if (typeof value === 'string') {
      // Get coordinate-specific attributes
      // Check _coordAttrs first (Dataset level), then fall back to _attrs
      const coordAttrs = (this._attrs as any)?._coordAttrs;
      const dimAttrs = coordAttrs?.[dim] || this._attrs;

      // Check if this looks like a time coordinate and we have a date string
      if (isTimeCoordinate(dimAttrs)) {
        const units = dimAttrs?.units as string | undefined;

        if (units) {
          const parsed = parseCFTimeUnits(units);
          if (parsed) {
            // Parse the input date string as UTC
            let inputDateStr = value;
            if (!inputDateStr.endsWith('Z') && !inputDateStr.includes('+')) {
              inputDateStr = inputDateStr + 'Z';
            }
            const inputDate = new Date(inputDateStr);
            if (isNaN(inputDate.getTime())) {
              throw new Error(`Invalid date string: '${value}'`);
            }

            // Calculate the CF time value from the input date
            const { unit, referenceDate } = parsed;
            const timeDiff = inputDate.getTime() - referenceDate.getTime();

            let targetValue: number;
            switch (unit) {
              case 'second':
                targetValue = timeDiff / 1000;
                break;
              case 'minute':
                targetValue = timeDiff / (60 * 1000);
                break;
              case 'hour':
                targetValue = timeDiff / (60 * 60 * 1000);
                break;
              case 'day':
                targetValue = timeDiff / (24 * 60 * 60 * 1000);
                break;
              case 'week':
                targetValue = timeDiff / (7 * 24 * 60 * 60 * 1000);
                break;
              case 'month':
                targetValue = timeDiff / (30 * 24 * 60 * 60 * 1000);
                break;
              case 'year':
                targetValue = timeDiff / (365.25 * 24 * 60 * 60 * 1000);
                break;
              default:
                targetValue = timeDiff / 1000;
            }

            // Find nearest coordinate value
            let closestIndex = 0;
            let minDiff = Math.abs((coords[0] as number) - targetValue);

            for (let i = 1; i < coords.length; i++) {
              const diff = Math.abs((coords[i] as number) - targetValue);
              if (diff < minDiff) {
                minDiff = diff;
                closestIndex = i;
              }
            }

            return closestIndex;
          }
        }
      }
    }

    // Apply selection method
    if (method === 'nearest') {
      return this._findNearestIndex(coords, value, tolerance);
    } else if (method === 'ffill' || method === 'pad') {
      return this._findFfillIndex(coords, value, tolerance);
    } else if (method === 'bfill' || method === 'backfill') {
      return this._findBfillIndex(coords, value, tolerance);
    }

    // Default exact match
    const index = coords.indexOf(value);
    if (index === -1) {
      throw new Error(`Coordinate value '${value}' not found in dimension '${dim}'`);
    }

    return index;
  }

  /**
   * Find nearest coordinate index
   */
  private _findNearestIndex(coords: CoordinateValue[], value: CoordinateValue, tolerance?: number): number {
    if (typeof value !== 'number' || !coords.every(c => typeof c === 'number')) {
      throw new Error('Nearest neighbor lookup requires numeric coordinates');
    }

    let closestIndex = 0;
    let minDiff = Math.abs((coords[0] as number) - value);

    for (let i = 1; i < coords.length; i++) {
      const diff = Math.abs((coords[i] as number) - value);
      if (diff < minDiff) {
        minDiff = diff;
        closestIndex = i;
      }
    }

    if (tolerance !== undefined && minDiff > tolerance) {
      throw new Error(`No coordinate within tolerance ${tolerance} of value ${value}`);
    }

    return closestIndex;
  }

  /**
   * Find forward fill index (last valid index <= value)
   */
  private _findFfillIndex(coords: CoordinateValue[], value: CoordinateValue, tolerance?: number): number {
    if (typeof value !== 'number' || !coords.every(c => typeof c === 'number')) {
      throw new Error('Forward fill requires numeric coordinates');
    }

    let lastValidIndex = -1;
    let minDiff = Infinity;

    for (let i = 0; i < coords.length; i++) {
      const coordValue = coords[i] as number;
      if (coordValue <= value) {
        const diff = value - coordValue;
        if (diff < minDiff) {
          minDiff = diff;
          lastValidIndex = i;
        }
      }
    }

    if (lastValidIndex === -1) {
      throw new Error(`No coordinate <= ${value} for forward fill`);
    }

    if (tolerance !== undefined && minDiff > tolerance) {
      throw new Error(`No coordinate within tolerance ${tolerance} of value ${value}`);
    }

    return lastValidIndex;
  }

  /**
   * Find backward fill index (first valid index >= value)
   */
  private _findBfillIndex(coords: CoordinateValue[], value: CoordinateValue, tolerance?: number): number {
    if (typeof value !== 'number' || !coords.every(c => typeof c === 'number')) {
      throw new Error('Backward fill requires numeric coordinates');
    }

    let firstValidIndex = -1;
    let minDiff = Infinity;

    for (let i = 0; i < coords.length; i++) {
      const coordValue = coords[i] as number;
      if (coordValue >= value) {
        const diff = coordValue - value;
        if (diff < minDiff) {
          minDiff = diff;
          firstValidIndex = i;
        }
      }
    }

    if (firstValidIndex === -1) {
      throw new Error(`No coordinate >= ${value} for backward fill`);
    }

    if (tolerance !== undefined && minDiff > tolerance) {
      throw new Error(`No coordinate within tolerance ${tolerance} of value ${value}`);
    }

    return firstValidIndex;
  }

  private _selectAtDimension(data: any, dimIndex: number, index: number): any {
    if (dimIndex === 0) {
      return data[index];
    }

    if (!Array.isArray(data)) {
      throw new Error('Invalid dimension index');
    }

    return data.map((item: any) => this._selectAtDimension(item, dimIndex - 1, index));
  }

  private _selectMultipleAtDimension(data: any, dimIndex: number, indices: number[]): any {
    if (dimIndex === 0) {
      return indices.map(i => data[i]);
    }

    if (!Array.isArray(data)) {
      throw new Error('Invalid dimension index');
    }

    return data.map((item: any) => this._selectMultipleAtDimension(item, dimIndex - 1, indices));
  }

  private _sliceAtDimension(data: any, dimIndex: number, start: number, stop: number): any {
    if (dimIndex === 0) {
      return data.slice(start, stop);
    }

    if (!Array.isArray(data)) {
      throw new Error('Invalid dimension index');
    }

    return data.map((item: any) => this._sliceAtDimension(item, dimIndex - 1, start, stop));
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
      const data = this._data as any[];
      if (!Array.isArray(data) || data.length === 0) {
        return data;
      }

      // Check if elements are arrays (multi-dimensional)
      if (Array.isArray(data[0])) {
        // Element-wise reduction across first dimension
        return data.reduce((acc: any, row: any) => {
          if (!acc) return deepClone(row);
          if (Array.isArray(row)) {
            return this._elementWiseOp(acc, row, reducer);
          }
          return reducer(acc as number, row as number);
        });
      } else {
        // Simple 1D reduction
        return data.reduce((acc: number, val: any) => reducer(acc, val as number), 0);
      }
    } else {
      // Reducing a later dimension - recurse into structure
      const data = this._data as any[];
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

  private _elementWiseOp(a: any, b: any, op: (x: number, y: number) => number): any {
    if (!Array.isArray(a) && !Array.isArray(b)) {
      return op(a as number, b as number);
    }
    if (Array.isArray(a) && Array.isArray(b)) {
      return a.map((val, i) => this._elementWiseOp(val, b[i], op));
    }
    throw new Error('Mismatched array dimensions');
  }

  private _divideArray(data: NDArray, divisor: number): NDArray {
    if (!Array.isArray(data)) {
      return (data as number) / divisor;
    }

    return data.map((item: any) => this._divideArray(item, divisor)) as NDArray;
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
