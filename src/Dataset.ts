/**
 * Dataset - A collection of labeled DataArrays
 * Similar to xarray.Dataset in Python
 */

import { DataArray } from './DataArray.js';
import {
  Coordinates,
  Attributes,
  DatasetOptions,
  Selection,
  DimensionName,
  CoordinateValue,
  DataValue,
  SelectionOptions,
  StreamOptions,
  StreamChunk,
  RollingOptions
} from './types.js';
import { deepClone, getBytesPerElement, ZARR_ENCODINGS } from './utils.js';
import { formatCoordinateValue, isTimeCoordinate } from './time/cf-time.js';
import { ZarrBackend } from './backends/zarr.js';
import type { WhereOptions } from './ops/where.js';
import { DatasetRolling } from './DatasetRolling.js';

export class Dataset {
  private _dataVars: Map<string, DataArray>;
  private _coords: Coordinates;
  private _attrs: Attributes;
  private _coordAttrs: { [coordName: string]: Attributes };
  private _precision: number = 6;
  private _isEncrypted: boolean = false;

  constructor(
    dataVars: { [name: string]: DataArray } = {},
    options: DatasetOptions = {}
  ) {
    this._dataVars = new Map();
    this._attrs = options.attrs || {};
    // Round numeric coordinates to specified precision
    this._coords = {};
    if (options.coords) {
      for (const [dim, coords] of Object.entries(options.coords)) {
        this._coords[dim] = coords.map(c =>
          typeof c === 'number' ? this._roundPrecision(c) : c
        );
      }
    }
    this._coordAttrs = options.coordAttrs || {};

    // Add data variables
    for (const [name, dataArray] of Object.entries(dataVars)) {
      this.addVariable(name, dataArray);
    }
  }

  /**
   * Get all data variable names
   */
  get dataVars(): string[] {
    return Array.from(this._dataVars.keys());
  }

  /**
   * Get all dimension names
   */
  get dims(): DimensionName[] {
    const dimsSet = new Set<DimensionName>();
    
    for (const dataArray of this._dataVars.values()) {
      for (const dim of dataArray.dims) {
        dimsSet.add(dim);
      }
    }
    
    return Array.from(dimsSet);
  }

  /**
   * Get the coordinates
   */
  get coords(): Coordinates {
    return deepClone(this._coords);
  }

  get coordAttrs(): { [coordName: string]: Attributes } {
    return deepClone(this._coordAttrs);
  }

  /**
   * Get the attributes
   */
  get attrs(): Attributes {
    return deepClone(this._attrs);
  }

  /**
   * Check if the dataset contains encrypted data
   */
  get isEncrypted(): boolean {
    return this._isEncrypted;
  }

  /**
   * Get dimension sizes
   */
  get sizes(): { [dim: string]: number } {
    const sizes: { [dim: string]: number } = {};
    
    for (const dataArray of this._dataVars.values()) {
      for (let i = 0; i < dataArray.dims.length; i++) {
        const dim = dataArray.dims[i];
        const size = dataArray.shape[i];
        
        if (sizes[dim] !== undefined && sizes[dim] !== size) {
          throw new Error(
            `Inconsistent dimension size for '${dim}': ${sizes[dim]} vs ${size}`
          );
        }
        
        sizes[dim] = size;
      }
    }
    
    return sizes;
  }

  /**
   * Add a data variable
   */
  addVariable(name: string, dataArray: DataArray): void {
    // Validate that dimensions are consistent
    const dataArrayDims = dataArray.dims;
    const dataArrayShape = dataArray.shape;

    for (let i = 0; i < dataArrayDims.length; i++) {
      const dim = dataArrayDims[i];
      const size = dataArrayShape[i];

      // Check against existing variables
      for (const existingArray of this._dataVars.values()) {
        const existingDimIndex = existingArray.dims.indexOf(dim);
        if (existingDimIndex !== -1) {
          const existingSize = existingArray.shape[existingDimIndex];
          if (existingSize !== size) {
            throw new Error(
              `Dimension '${dim}' size mismatch: ${size} vs ${existingSize}`
            );
          }
        }
      }

      // Update coordinates if not already present
      if (!this._coords[dim]) {
        const dataArrayCoords = dataArray.coords;
        if (dataArrayCoords[dim]) {
          // Round numeric coordinates to specified precision
          this._coords[dim] = dataArrayCoords[dim].map(c =>
            typeof c === 'number' ? this._roundPrecision(c) : c
          );
        }
      }
    }

    this._dataVars.set(name, dataArray);
  }

  /**
   * Round a number to the specified precision
   */
  private _roundPrecision(value: number): number {
    const factor = Math.pow(10, this._precision);
    return Math.round(value * factor) / factor;
  }

  private _datasetOperandFor(
    varName: string,
    dataset: Dataset,
    kind: 'condition' | 'other',
    required: boolean = true
  ): DataArray | null {
    if (!dataset.hasVariable(varName)) {
      if (required) {
        throw new Error(
          `Dataset provided for ${kind} does not contain variable '${varName}'`
        );
      }
      return null;
    }

    return dataset.getVariable(varName);
  }

  /**
   * Get a data variable
   */
  getVariable(name: string): DataArray {
    const variable = this._dataVars.get(name);
    if (variable === undefined) {
      throw new Error(`Variable '${name}' not found in dataset`);
    }
    return variable;
  }

  /**
   * Dictionary-style access to data variables (xarray-style)
   * Supports:
   * - ds['varname'] -> returns DataArray
   * - ds[['var1', 'var2']] -> returns new Dataset with subset
   */
  get(key: string | string[]): DataArray | Dataset {
    if (typeof key === 'string') {
      return this.getVariable(key);
    } else if (Array.isArray(key)) {
      const newDataVars: { [name: string]: DataArray } = {};
      const usedDims = new Set<DimensionName>();

      // Collect all dimensions used by selected variables
      for (const varName of key) {
        if (!this.hasVariable(varName)) {
          throw new Error(`Variable '${varName}' not found in dataset`);
        }
        const variable = this._dataVars.get(varName)!;
        newDataVars[varName] = variable;

        // Add all dimensions from this variable
        for (const dim of variable.dims) {
          usedDims.add(dim);
        }
      }

      // Only include coordinates that are used by the selected variables
      const newCoords: Coordinates = {};
      const newCoordAttrs: { [name: string]: Attributes } = {};

      for (const dim of usedDims) {
        if (this._coords[dim]) {
          newCoords[dim] = this._coords[dim];
          if (this._coordAttrs[dim]) {
            newCoordAttrs[dim] = this._coordAttrs[dim];
          }
        }
      }

      return new Dataset(newDataVars, {
        coords: newCoords,
        attrs: this._attrs,
        coordAttrs: newCoordAttrs
      });
    }
    throw new Error('Key must be a string or array of strings');
  }

  where(
    cond: DataArray | Dataset | DataValue,
    other: DataArray | Dataset | DataValue | null = null,
    options?: WhereOptions
  ): Dataset {
    const resultVars: { [name: string]: DataArray } = {};

    for (const [name, dataArray] of this._dataVars.entries()) {
      const condition = cond instanceof Dataset
        ? this._datasetOperandFor(name, cond, 'condition')
        : cond;

      const otherOperand = other instanceof Dataset
        ? this._datasetOperandFor(name, other, 'other', false)
        : other;

      resultVars[name] = dataArray.where(
        condition as DataArray | DataValue,
        otherOperand as DataArray | DataValue | null,
        options
      );
    }

    const newCoords: Coordinates = {};
    for (const dataArray of Object.values(resultVars)) {
      for (const dim of dataArray.dims) {
        if (!newCoords[dim]) {
          newCoords[dim] = dataArray.coords[dim];
        }
      }
    }

    return new Dataset(resultVars, {
      coords: newCoords,
      attrs: deepClone(this._attrs),
      coordAttrs: deepClone(this._coordAttrs)
    });
  }

  /**
   * Check if a variable exists
   */
  hasVariable(name: string): boolean {
    return this._dataVars.has(name);
  }

  /**
   * Remove a data variable
   */
  async compute(): Promise<Dataset> {
    const computedVars: { [name: string]: DataArray } = {};

    for (const [name, dataArray] of this._dataVars.entries()) {
      computedVars[name] = await dataArray.compute();
    }

    return new Dataset(computedVars, {
      coords: this._coords,
      attrs: this._attrs,
      coordAttrs: this._coordAttrs
    });
  }

  removeVariable(name: string): boolean {
    return this._dataVars.delete(name);
  }

  /**
   * Select data by coordinate labels
   */
  async sel(selection: Selection, options?: SelectionOptions): Promise<Dataset> {
    const newDataVars: { [name: string]: DataArray } = {};

    for (const [name, dataArray] of this._dataVars.entries()) {
      // Only apply selection to dimensions present in this dataArray
      const relevantSelection: Selection = {};
      for (const dim of dataArray.dims) {
        if (selection[dim] !== undefined) {
          relevantSelection[dim] = selection[dim];
        }
      }

      if (Object.keys(relevantSelection).length > 0) {
        newDataVars[name] = await dataArray.sel(relevantSelection, options);
      } else {
        newDataVars[name] = dataArray;
      }
    }

    // Update coordinates from the selected DataArrays
    const newCoords: Coordinates = {};
    for (const dataArray of Object.values(newDataVars)) {
      for (const dim of dataArray.dims) {
        if (!newCoords[dim]) {
          newCoords[dim] = dataArray.coords[dim];
        }
      }
    }

    return new Dataset(newDataVars, {
      coords: newCoords,
      attrs: this._attrs,
      coordAttrs: this._coordAttrs
    });
  }

  /**
   * Stream data selection in chunks (useful for large datasets)
   * @param selection - Selection specification
   * @param options - Streaming options including chunk size and dimension
   * @returns AsyncGenerator yielding Dataset chunks with progress information
   *
   * @example
   * ```typescript
   * const stream = dataset.selStream(
   *   { time: ['2020-01-01', '2020-12-31'], lat: 45, lon: -73 },
   *   { chunkSize: 50 } // 50MB chunks
   * );
   *
   * for await (const chunk of stream) {
   *   console.log(`Progress: ${chunk.progress}%`);
   *   const temp = chunk.data.getVariable('temperature');
   *   await writeToFile(temp);
   * }
   * ```
   */
  async *selStream(
    selection: Selection,
    options?: StreamOptions
  ): AsyncGenerator<StreamChunk<Dataset>> {
    // Get the first data variable to use as reference for streaming
    const firstVarName = this.dataVars[0];
    if (!firstVarName) {
      throw new Error('Dataset has no variables');
    }

    const firstVar = this._dataVars.get(firstVarName)!;

    // Create stream from first variable
    const varStream = firstVar.selStream(selection, options);

    // Iterate through chunks
    for await (const chunk of varStream) {
      const newDataVars: { [name: string]: DataArray } = {};

      // For each variable, get the corresponding chunk
      for (const [name, dataArray] of this._dataVars.entries()) {
        // Only apply selection to dimensions present in this dataArray
        const relevantSelection: Selection = {};
        for (const dim of dataArray.dims) {
          if (selection[dim] !== undefined) {
            relevantSelection[dim] = selection[dim];
          }
        }

        if (Object.keys(relevantSelection).length > 0) {
          // For the first variable, use the chunk data
          if (name === firstVarName) {
            newDataVars[name] = chunk.data;
          } else {
            // For other variables, perform the same selection
            // Build chunk-specific selection based on first var's chunk
            const chunkSelection = this._buildChunkSelection(
              relevantSelection,
              chunk.data,
              options?.dimension || firstVar.dims[0]
            );
            newDataVars[name] = await dataArray.sel(chunkSelection, {
              method: options?.method,
              tolerance: options?.tolerance
            });
          }
        } else {
          newDataVars[name] = dataArray;
        }
      }

      // Update coordinates from the selected DataArrays
      const newCoords: Coordinates = {};
      for (const dataArray of Object.values(newDataVars)) {
        for (const dim of dataArray.dims) {
          if (!newCoords[dim]) {
            newCoords[dim] = dataArray.coords[dim];
          }
        }
      }
 
      yield {
        data: new Dataset(newDataVars, {
          coords: newCoords,
          attrs: this._attrs,
          coordAttrs: this._coordAttrs
        }),
        progress: chunk.progress,
        bytesProcessed: chunk.bytesProcessed,
        totalBytes: chunk.totalBytes,
        chunkIndex: chunk.chunkIndex,
        totalChunks: chunk.totalChunks
      };
    }
  }

  /**
   * Select data by integer position
   */
  async isel(selection: { [dimension: string]: number | number[] }): Promise<Dataset> {
    const newDataVars: { [name: string]: DataArray } = {};

    for (const [name, dataArray] of this._dataVars.entries()) {
      const relevantSelection: { [dimension: string]: number | number[] } = {};
      for (const dim of dataArray.dims) {
        if (selection[dim] !== undefined) {
          relevantSelection[dim] = selection[dim];
        }
      }

      if (Object.keys(relevantSelection).length > 0) {
        newDataVars[name] = await dataArray.isel(relevantSelection);
      } else {
        newDataVars[name] = dataArray;
      }
    }

    return new Dataset(newDataVars, {
      attrs: this._attrs,
      coordAttrs: this._coordAttrs
    });
  }

  /**
   * Apply a function to all data variables
   */
  map(fn: (dataArray: DataArray, name: string) => DataArray): Dataset {
    const newDataVars: { [name: string]: DataArray } = {};

    for (const [name, dataArray] of this._dataVars.entries()) {
      newDataVars[name] = fn(dataArray, name);
    }

    return new Dataset(newDataVars, {
      coords: this._coords,
      attrs: this._attrs,
      coordAttrs: this._coordAttrs
    });
  }

  rename(mapping: { [oldName: string]: string }): Dataset {
    const newDataVars: { [name: string]: DataArray } = {};
    const usedNewNames = new Set<string>();

    for (const key of Object.keys(mapping)) {
      if (!this._dataVars.has(key)) {
        throw new Error(`Cannot rename non-existent variable '${key}'`);
      }
    }

    for (const [name, dataArray] of this._dataVars.entries()) {
      const newName = mapping[name] ?? name;

      if (usedNewNames.has(newName)) {
        throw new Error(`Duplicate target name '${newName}' in rename mapping`);
      }

      usedNewNames.add(newName);
      newDataVars[newName] = dataArray;
    }

    return new Dataset(newDataVars, {
      coords: this._coords,
      attrs: this._attrs,
      coordAttrs: this._coordAttrs
    });
  }

  assignCoords(coords: { [dimension: string]: CoordinateValue[] | DataArray }): Dataset {
    const updatedCoords = deepClone(this._coords);
    const sizes = this.sizes;
    const dimsToUpdate = Object.keys(coords);

    for (const dim of dimsToUpdate) {
      if (sizes[dim] === undefined) {
        throw new Error(`Dimension '${dim}' not found in dataset`);
      }

      const coordValue = coords[dim];
      let values: CoordinateValue[];

      if (coordValue instanceof DataArray) {
        if (coordValue.dims.length !== 1 || coordValue.dims[0] !== dim) {
          throw new Error(`Coordinate DataArray for '${dim}' must be 1D and share the same dimension`);
        }

        const data = coordValue.data;
        if (!Array.isArray(data)) {
          throw new Error(`Coordinate DataArray for '${dim}' must be 1D`);
        }

        values = (data as CoordinateValue[]).map(v =>
          v instanceof Date ? new Date(v.getTime()) : v
        );
      } else if (Array.isArray(coordValue)) {
        values = coordValue.map(value =>
          value instanceof Date ? new Date(value.getTime()) : value
        );
      } else {
        throw new Error(`assignCoords requires an array or DataArray for dimension '${dim}'`);
      }

      if (values.length !== sizes[dim]) {
        throw new Error(
          `Coordinate array length for '${dim}' (${values.length}) does not match dimension size (${sizes[dim]})`
        );
      }

      updatedCoords[dim] = values;
    }

    const newDataVars: { [name: string]: DataArray } = {};

    for (const [name, dataArray] of this._dataVars.entries()) {
      let needsUpdate = false;
      const coordsForArray = dataArray.coords;

      for (const dim of dimsToUpdate) {
        if (dataArray.dims.includes(dim)) {
          coordsForArray[dim] = updatedCoords[dim].map(value =>
            value instanceof Date ? new Date(value.getTime()) : value
          );
          needsUpdate = true;
        }
      }

      newDataVars[name] = needsUpdate
        ? dataArray.cloneWith({ coords: coordsForArray })
        : dataArray;
    }

    return new Dataset(newDataVars, {
      coords: deepClone(updatedCoords),
      attrs: this._attrs,
      coordAttrs: this._coordAttrs
    });
  }

  rolling(dim: DimensionName, window: number, options?: RollingOptions): DatasetRolling {
    return new DatasetRolling(this, dim, window, options ?? {});
  }

  dropVars(names: string | string[]): Dataset {
    const dropSet = new Set(Array.isArray(names) ? names : [names]);
    const newDataVars: { [name: string]: DataArray } = {};

    for (const [name, dataArray] of this._dataVars.entries()) {
      if (!dropSet.has(name)) {
        newDataVars[name] = dataArray;
      }
    }

    if (Object.keys(newDataVars).length === this._dataVars.size) {
      return this;
    }

    const newCoords: Coordinates = {};
    const newCoordAttrs: { [coordName: string]: Attributes } = {};
    const usedDims = new Set<DimensionName>();

    for (const dataArray of Object.values(newDataVars)) {
      for (const dim of dataArray.dims) {
        usedDims.add(dim);
      }
    }

    for (const dim of usedDims) {
      for (const dataArray of Object.values(newDataVars)) {
        if (dataArray.dims.includes(dim)) {
          newCoords[dim] = dataArray.coords[dim];
          if (this._coordAttrs[dim]) {
            newCoordAttrs[dim] = this._coordAttrs[dim];
          }
          break;
        }
      }
    }

    return new Dataset(newDataVars, {
      coords: deepClone(newCoords),
      attrs: this._attrs,
      coordAttrs: deepClone(newCoordAttrs)
    });
  }

  squeeze(): Dataset {
    const newDataVars: { [name: string]: DataArray } = {};
    const usedDims = new Set<DimensionName>();

    for (const [name, dataArray] of this._dataVars.entries()) {
      const squeezed = dataArray.squeeze();
      newDataVars[name] = squeezed;
      for (const dim of squeezed.dims) {
        usedDims.add(dim);
      }
    }

    const newCoords: Coordinates = {};
    const newCoordAttrs: { [coordName: string]: Attributes } = {};

    for (const dim of usedDims) {
      for (const dataArray of Object.values(newDataVars)) {
        if (dataArray.dims.includes(dim)) {
          newCoords[dim] = dataArray.coords[dim];
          if (this._coordAttrs[dim]) {
            newCoordAttrs[dim] = this._coordAttrs[dim];
          }
          break;
        }
      }
    }

    return new Dataset(newDataVars, {
      coords: deepClone(newCoords),
      attrs: this._attrs,
      coordAttrs: deepClone(newCoordAttrs)
    });
  }

  /**
   * Merge with another dataset
   */
  merge(other: Dataset): Dataset {
    const newDataVars: { [name: string]: DataArray } = {};

    // Add all variables from this dataset
    for (const [name, dataArray] of this._dataVars.entries()) {
      newDataVars[name] = dataArray;
    }

    // Add variables from other dataset
    for (const [name, dataArray] of other._dataVars.entries()) {
      if (this._dataVars.has(name)) {
        throw new Error(`Variable '${name}' already exists in dataset`);
      }
      newDataVars[name] = dataArray;
    }

    // Merge coordinates
    const newCoords: Coordinates = { ...this._coords, ...other._coords };

    // Merge attributes
    const newAttrs: Attributes = { ...this._attrs, ...other._attrs };

    // Merge coordinate attributes
    const newCoordAttrs = { ...this._coordAttrs, ...other._coordAttrs };

    return new Dataset(newDataVars, {
      coords: newCoords,
      attrs: newAttrs,
      coordAttrs: newCoordAttrs
    });
  }

  /**
   * Detect if any data variables use encryption codecs
   * Checks the codecs in the attributes of each data variable
   * @returns true if encryption is detected, false otherwise
   */
  detectEncryption(): boolean {
    for (const dataArray of this._dataVars.values()) {
      const codecs = dataArray.attrs.codecs;
      if (codecs && Array.isArray(codecs)) {
        for (const codec of codecs) {
          if (codec && codec.name && ZARR_ENCODINGS.has(codec.name)) {
            this._isEncrypted = true;
            return true;
          }
        }
      }
    }

    this._isEncrypted = false;
    return false;
  }

  /**
   * Convert to a plain JavaScript object
   */
  toObject(): any {
    const dataVars: any = {};
    
    for (const [name, dataArray] of this._dataVars.entries()) {
      dataVars[name] = dataArray.toObject();
    }

    return {
      dataVars,
      coords: this.coords,
      attrs: this.attrs,
      dims: this.dims,
      sizes: this.sizes
    };
  }

  /**
   * Convert to JSON string
   */
  toJSON(): string {
    return JSON.stringify(this.toObject());
  }

  /**
   * Convert a data variable to an array of records with coordinates and values
   * Each record contains coordinate values and the data value for that point
   *
   * @param varName - Name of the variable to convert to records
   * @param options - Optional configuration
   * @param options.precision - Number of decimal places to round coordinate values (default: 6)
   * @returns Array of records with coordinate fields and a value field
   *
   * @example
   * ```typescript
   * // For a variable 'temperature' with dims ['time', 'lat', 'lon']
   * dataset.toRecords('temperature')
   * // Returns:
   * // [
   * //   { time: '2020-01-01', lat: 45.5, lon: -73.5, value: 23.4 },
   * //   { time: '2020-01-01', lat: 45.5, lon: -74.0, value: 24.1 },
   * //   ...
   * // ]
   *
   * // With custom precision
   * dataset.toRecords('temperature', { precision: 2 })
   * // Returns:
   * // [
   * //   { time: '2020-01-01', lat: 45.50, lon: -73.50, value: 23.4 },
   * //   ...
   * // ]
   * ```
   */
  toRecords(varName: string, options?: { precision?: number }): Array<Record<string, any>> {
    const dataArray = this.getVariable(varName);
    return dataArray.toRecords(options);
  }

  /**
   * Calculate and store resolution information for all coordinates in coordAttrs
   * For spatial coordinates (lat/lon), stores numeric resolution (e.g., 0.1 degrees)
   * For time coordinates, stores both numeric and human-readable resolution (e.g., "daily")
   *
   * @returns Object mapping dimension names to their resolution info
   *
   * @example
   * ```typescript
   * const resolutions = dataset.calculateCoordinateResolutions();
   * // Returns:
   * // {
   * //   latitude: { resolution: 0.1, unit: 'degrees' },
   * //   longitude: { resolution: 0.1, unit: 'degrees' },
   * //   time: { resolution: 1, type: 'daily', unit: 'days' }
   * // }
   * // Also updates dataset.coordAttrs with resolution info
   * ```
   */
  calculateCoordinateResolutions(): { [dim: string]: any } {
    const resolutions: { [dim: string]: any } = {};

    for (const dim of this.dims) {
      const coords = this._coords[dim];

      if (!coords || coords.length < 2) {
        continue;
      }

      // Check if this is a time coordinate
      const dimAttrs = this._coordAttrs[dim] || {};
      const isTime = isTimeCoordinate(dimAttrs);

      if (isTime) {
        // Use the DataArray's calculateTimeResolution method
        // Find a variable that has this time dimension
        for (const dataArray of this._dataVars.values()) {
          if (dataArray.dims.includes(dim)) {
            const timeResolution = dataArray.calculateTimeResolution(dim);
            if (timeResolution) {
              resolutions[dim] = timeResolution;

              // Store in coordAttrs
              if (!this._coordAttrs[dim]) {
                this._coordAttrs[dim] = {};
              }
              this._coordAttrs[dim].resolution = timeResolution.value;
              this._coordAttrs[dim].resolution_type = timeResolution.type;
              this._coordAttrs[dim].resolution_unit = timeResolution.unit;

              break;
            }
          }
        }
      } else {
        // For spatial coordinates, calculate numeric resolution
        const first = coords[0];
        const second = coords[1];

        if (typeof first === 'number' && typeof second === 'number') {
          const resolution = Math.abs(second - first);

          // Round to avoid floating-point errors
          const roundedResolution = Math.round(resolution * 1000000) / 1000000;

          // Determine unit based on dimension name
          let unit = 'unknown';
          const dimLower = dim.toLowerCase();
          if (dimLower.includes('lat') || dimLower === 'y') {
            unit = 'degrees_north';
          } else if (dimLower.includes('lon') || dimLower === 'x') {
            unit = 'degrees_east';
          } else {
            // Try to get unit from coordAttrs
            unit = dimAttrs.units || 'unknown';
          }

          resolutions[dim] = {
            resolution: roundedResolution,
            unit: unit
          };

          // Store in coordAttrs
          if (!this._coordAttrs[dim]) {
            this._coordAttrs[dim] = {};
          }
          this._coordAttrs[dim].resolution = roundedResolution;
          this._coordAttrs[dim].resolution_unit = unit;
        }
      }
    }

    return resolutions;
  }

  /**
   * String representation of the Dataset
   * Works in both Node.js and browser environments
   */
  toString(): string {
    const lines: string[] = [];
    const sizes = this.sizes;

    // Header with dimensions
    lines.push('<jaxray.Dataset>');

    // Dimensions section
    const dimStrs = Object.entries(sizes).map(([dim, size]) => `${dim}: ${size}`);
    lines.push(`Dimensions:  (${dimStrs.join(', ')})`);

    // Coordinates section
    const coordKeys = Object.keys(this._coords);
    if (coordKeys.length > 0) {
      lines.push('Coordinates:');
      for (const key of coordKeys) {
        const coords = this._coords[key];
        
        const dataArray = this._dataVars.get(key);
        const attrs = this._coordAttrs[key];

        const dtype = dataArray ? this._inferDtype(dataArray) : this._inferDtypeFromCoords(coords);
        const coordDims = dataArray ? `(${dataArray.dims.join(', ')})` : `(${key})`;
        const preview = this._formatCoordPreview(coords, attrs);
        lines.push(`  * ${key.padEnd(12)} ${coordDims.padEnd(20)} ${dtype} ${preview}`);
      }
    }

    // Data variables section
    if (this._dataVars.size > 0) {
      lines.push('Data variables:');
      for (const [name, dataArray] of this._dataVars.entries()) {
        const dims = `(${dataArray.dims.join(', ')})`;
        const dtype = this._inferDtype(dataArray);
        lines.push(`    ${name.padEnd(12)} ${dims.padEnd(20)} ${dtype}`);
      }
    }

    // Attributes section
    const attrKeys = Object.keys(this._attrs);
    if (attrKeys.length > 0) {
      lines.push(`Attributes:`);
      for (const key of attrKeys) {
        const value = this._attrs[key];
        const valueStr = typeof value === 'string' ? `'${value}'` : String(value);
        lines.push(`    ${key}:  ${valueStr}`);
      }
    }

    return lines.join('\n');
  }

  /**
   * Infer dtype from DataArray
   */
  private _inferDtype(dataArray: DataArray): string {
    const values = dataArray.values;
    if (!values) return 'object';

    // Flatten the array manually to handle nested arrays
    const flatData: DataValue[] = [];
    const flatten = (arr: any): void => {
      if (Array.isArray(arr)) {
        for (const item of arr) {
          flatten(item);
        }
      } else {
        flatData.push(arr);
      }
    };
    flatten(values);

    if (flatData.length === 0) return 'object';

    const firstValue = flatData.find(v => v != null);
    if (firstValue === undefined) return 'object';

    if (typeof firstValue === 'number') {
      return Number.isInteger(firstValue) ? 'int64' : 'float64';
    } else if (typeof firstValue === 'string') {
      return 'object';
    } else if (typeof firstValue === 'boolean') {
      return 'bool';
    }
    return 'object';
  }

  /**
   * Infer dtype from coordinate values
   */
  private _inferDtypeFromCoords(coords: CoordinateValue[]): string {
    if (coords.length === 0) return 'object';

    const firstValue = coords.find(v => v != null);
    if (firstValue === undefined) return 'object';

    if (typeof firstValue === 'number') {
      return Number.isInteger(firstValue) ? 'int64' : 'float64';
    } else if (typeof firstValue === 'string') {
      return 'object';
    }
    return 'object';
  }

  /**
   * Format coordinate preview
   */
  private _formatCoordPreview(coords: CoordinateValue[], attrs?: Attributes): string {
    if (coords.length === 0) return '[]';

    if (coords.length <= 3) {
      return `[${coords.map(val => formatCoordinateValue(val, attrs)).join(', ')}]`;
    }
    return `[${formatCoordinateValue(coords[0], attrs)}, ${formatCoordinateValue(coords[1], attrs)}, ..., ${formatCoordinateValue(coords[coords.length - 1], attrs)}]`;
  }

  /**
   * Custom Node.js inspector (optional, only works in Node.js)
   */
  [Symbol.for('nodejs.util.inspect.custom')]?(): string {
    return this.toString();
  }

  /**
   * Estimate the size in bytes of a selection for a given variable
   * Useful for checking data size before downloading
   *
   * @param varName - Name of the variable
   * @param selection - Selection object with dimension names and ranges/indices
   * @returns Estimated size in bytes
   */
  getSizeEstimation(
    varName: string,
    selection?: { [dim: string]: number | { start: number; stop: number } | [number, number] }
  ): number {
    const dataArray = this.getVariable(varName);

    // Get bytes per element from data type in attrs
    const dataType = dataArray.attrs._zarr_data_type as string | undefined;
    const bytesPerElement = getBytesPerElement(dataType);

    if (!selection) {
      // Return full array size
      return dataArray.size * bytesPerElement;
    }

    // Calculate size based on selection
    let totalElements = 1;

    for (let i = 0; i < dataArray.dims.length; i++) {
      const dim = dataArray.dims[i];
      const dimSize = dataArray.shape[i];
      const sel = selection[dim];

      if (sel === undefined || sel === null) {
        // No selection on this dimension - use full size
        totalElements *= dimSize;
      } else if (typeof sel === 'number') {
        // Single index - this dimension contributes 1
        totalElements *= 1;
      } else if (Array.isArray(sel)) {
        // Array [start, stop]
        totalElements *= (sel[1] - sel[0]);
      } else if (typeof sel === 'object' && 'start' in sel && 'stop' in sel) {
        // Range object {start, stop}
        totalElements *= (sel.stop - sel.start);
      }
    }

    return totalElements * bytesPerElement;
  }

  /**
   * Open a Zarr store as a Dataset
   * Similar to xarray.open_zarr() in Python
   *
   * @param store - A ZarrStore implementation (e.g., ShardedStore for IPFS, S3Store, LocalStore)
   * @param options - Options for opening the store
   * @returns A Promise that resolves to a Dataset
   *
   * @example
   * ```typescript
   * // IPFS: Create ShardedStore and open
   * import { ShardedStore, createIpfsElements } from 'jaxray';
   * const ipfsElements = createIpfsElements('https://ipfs-gateway.dclimate.net');
   * const store = await ShardedStore.open('bafyr4i...', ipfsElements);
   * const ds = await Dataset.open_zarr(store);
   *
   * // Future: S3
   * const store = new S3Store(bucket, key);
   * const ds = await Dataset.open_zarr(store);
   *
   * // Future: Local filesystem
   * const store = new LocalStore('/path/to/zarr');
   * const ds = await Dataset.open_zarr(store);
   * ```
   */
  static async open_zarr(
    store: any,
    options?: {
      group?: string;
      consolidated?: boolean;
    }
  ): Promise<Dataset> {
    return ZarrBackend.open(store, options);
  }

  /**
   * Build chunk-specific selection based on reference chunk
   */
  private _buildChunkSelection(
    baseSelection: Selection,
    referenceChunk: DataArray,
    chunkDim: DimensionName
  ): Selection {
    const chunkSelection: Selection = { ...baseSelection };

    // Get the coordinate values from the reference chunk for the chunk dimension
    if (referenceChunk.dims.includes(chunkDim)) {
      const chunkCoords = referenceChunk.coords[chunkDim];
      if (chunkCoords && chunkCoords.length > 0) {
        // Use the actual coordinate values from the chunk
        chunkSelection[chunkDim] = chunkCoords.length === 1 ? chunkCoords[0] : chunkCoords;
      }
    }

    return chunkSelection;
  }
}

