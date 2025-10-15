/**
 * Dataset - A collection of labeled DataArrays
 * Similar to xarray.Dataset in Python
 */
import { deepClone, getBytesPerElement, ZARR_ENCODINGS } from './utils';
import { formatCoordinateValue, isTimeCoordinate } from './cf-time';
import { ZarrBackend } from './backends/zarr';
export class Dataset {
    constructor(dataVars = {}, options = {}) {
        this._precision = 6;
        this._isEncrypted = false;
        this._dataVars = new Map();
        this._attrs = options.attrs || {};
        // Round numeric coordinates to specified precision
        this._coords = {};
        if (options.coords) {
            for (const [dim, coords] of Object.entries(options.coords)) {
                this._coords[dim] = coords.map(c => typeof c === 'number' ? this._roundPrecision(c) : c);
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
    get dataVars() {
        return Array.from(this._dataVars.keys());
    }
    /**
     * Get all dimension names
     */
    get dims() {
        const dimsSet = new Set();
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
    get coords() {
        return deepClone(this._coords);
    }
    get coordAttrs() {
        return deepClone(this._coordAttrs);
    }
    /**
     * Get the attributes
     */
    get attrs() {
        return deepClone(this._attrs);
    }
    /**
     * Check if the dataset contains encrypted data
     */
    get isEncrypted() {
        return this._isEncrypted;
    }
    /**
     * Get dimension sizes
     */
    get sizes() {
        const sizes = {};
        for (const dataArray of this._dataVars.values()) {
            for (let i = 0; i < dataArray.dims.length; i++) {
                const dim = dataArray.dims[i];
                const size = dataArray.shape[i];
                if (sizes[dim] !== undefined && sizes[dim] !== size) {
                    throw new Error(`Inconsistent dimension size for '${dim}': ${sizes[dim]} vs ${size}`);
                }
                sizes[dim] = size;
            }
        }
        return sizes;
    }
    /**
     * Add a data variable
     */
    addVariable(name, dataArray) {
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
                        throw new Error(`Dimension '${dim}' size mismatch: ${size} vs ${existingSize}`);
                    }
                }
            }
            // Update coordinates if not already present
            if (!this._coords[dim]) {
                const dataArrayCoords = dataArray.coords;
                if (dataArrayCoords[dim]) {
                    // Round numeric coordinates to specified precision
                    this._coords[dim] = dataArrayCoords[dim].map(c => typeof c === 'number' ? this._roundPrecision(c) : c);
                }
            }
        }
        this._dataVars.set(name, dataArray);
    }
    /**
     * Round a number to the specified precision
     */
    _roundPrecision(value) {
        const factor = Math.pow(10, this._precision);
        return Math.round(value * factor) / factor;
    }
    /**
     * Get a data variable
     */
    getVariable(name) {
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
    get(key) {
        if (typeof key === 'string') {
            return this.getVariable(key);
        }
        else if (Array.isArray(key)) {
            const newDataVars = {};
            const usedDims = new Set();
            // Collect all dimensions used by selected variables
            for (const varName of key) {
                if (!this.hasVariable(varName)) {
                    throw new Error(`Variable '${varName}' not found in dataset`);
                }
                const variable = this._dataVars.get(varName);
                newDataVars[varName] = variable;
                // Add all dimensions from this variable
                for (const dim of variable.dims) {
                    usedDims.add(dim);
                }
            }
            // Only include coordinates that are used by the selected variables
            const newCoords = {};
            const newCoordAttrs = {};
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
    /**
     * Check if a variable exists
     */
    hasVariable(name) {
        return this._dataVars.has(name);
    }
    /**
     * Remove a data variable
     */
    removeVariable(name) {
        return this._dataVars.delete(name);
    }
    /**
     * Select data by coordinate labels
     */
    async sel(selection, options) {
        const newDataVars = {};
        for (const [name, dataArray] of this._dataVars.entries()) {
            // Only apply selection to dimensions present in this dataArray
            const relevantSelection = {};
            for (const dim of dataArray.dims) {
                if (selection[dim] !== undefined) {
                    relevantSelection[dim] = selection[dim];
                }
            }
            if (Object.keys(relevantSelection).length > 0) {
                newDataVars[name] = await dataArray.sel(relevantSelection, options);
            }
            else {
                newDataVars[name] = dataArray;
            }
        }
        // Update coordinates from the selected DataArrays
        const newCoords = {};
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
    async *selStream(selection, options) {
        // Get the first data variable to use as reference for streaming
        const firstVarName = this.dataVars[0];
        if (!firstVarName) {
            throw new Error('Dataset has no variables');
        }
        const firstVar = this._dataVars.get(firstVarName);
        // Create stream from first variable
        const varStream = firstVar.selStream(selection, options);
        // Iterate through chunks
        for await (const chunk of varStream) {
            const newDataVars = {};
            // For each variable, get the corresponding chunk
            for (const [name, dataArray] of this._dataVars.entries()) {
                // Only apply selection to dimensions present in this dataArray
                const relevantSelection = {};
                for (const dim of dataArray.dims) {
                    if (selection[dim] !== undefined) {
                        relevantSelection[dim] = selection[dim];
                    }
                }
                if (Object.keys(relevantSelection).length > 0) {
                    // For the first variable, use the chunk data
                    if (name === firstVarName) {
                        newDataVars[name] = chunk.data;
                    }
                    else {
                        // For other variables, perform the same selection
                        // Build chunk-specific selection based on first var's chunk
                        const chunkSelection = this._buildChunkSelection(relevantSelection, chunk.data, options?.dimension || firstVar.dims[0]);
                        newDataVars[name] = await dataArray.sel(chunkSelection, {
                            method: options?.method,
                            tolerance: options?.tolerance
                        });
                    }
                }
                else {
                    newDataVars[name] = dataArray;
                }
            }
            // Update coordinates from the selected DataArrays
            const newCoords = {};
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
    async isel(selection) {
        const newDataVars = {};
        for (const [name, dataArray] of this._dataVars.entries()) {
            const relevantSelection = {};
            for (const dim of dataArray.dims) {
                if (selection[dim] !== undefined) {
                    relevantSelection[dim] = selection[dim];
                }
            }
            if (Object.keys(relevantSelection).length > 0) {
                newDataVars[name] = await dataArray.isel(relevantSelection);
            }
            else {
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
    map(fn) {
        const newDataVars = {};
        for (const [name, dataArray] of this._dataVars.entries()) {
            newDataVars[name] = fn(dataArray, name);
        }
        return new Dataset(newDataVars, {
            coords: this._coords,
            attrs: this._attrs,
            coordAttrs: this._coordAttrs
        });
    }
    /**
     * Merge with another dataset
     */
    merge(other) {
        const newDataVars = {};
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
        const newCoords = { ...this._coords, ...other._coords };
        // Merge attributes
        const newAttrs = { ...this._attrs, ...other._attrs };
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
    detectEncryption() {
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
    toObject() {
        const dataVars = {};
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
    toJSON() {
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
    toRecords(varName, options) {
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
    calculateCoordinateResolutions() {
        const resolutions = {};
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
            }
            else {
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
                    }
                    else if (dimLower.includes('lon') || dimLower === 'x') {
                        unit = 'degrees_east';
                    }
                    else {
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
    toString() {
        const lines = [];
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
    _inferDtype(dataArray) {
        const values = dataArray.values;
        if (!values)
            return 'object';
        // Flatten the array manually to handle nested arrays
        const flatData = [];
        const flatten = (arr) => {
            if (Array.isArray(arr)) {
                for (const item of arr) {
                    flatten(item);
                }
            }
            else {
                flatData.push(arr);
            }
        };
        flatten(values);
        if (flatData.length === 0)
            return 'object';
        const firstValue = flatData.find(v => v != null);
        if (firstValue === undefined)
            return 'object';
        if (typeof firstValue === 'number') {
            return Number.isInteger(firstValue) ? 'int64' : 'float64';
        }
        else if (typeof firstValue === 'string') {
            return 'object';
        }
        else if (typeof firstValue === 'boolean') {
            return 'bool';
        }
        return 'object';
    }
    /**
     * Infer dtype from coordinate values
     */
    _inferDtypeFromCoords(coords) {
        if (coords.length === 0)
            return 'object';
        const firstValue = coords.find(v => v != null);
        if (firstValue === undefined)
            return 'object';
        if (typeof firstValue === 'number') {
            return Number.isInteger(firstValue) ? 'int64' : 'float64';
        }
        else if (typeof firstValue === 'string') {
            return 'object';
        }
        return 'object';
    }
    /**
     * Format coordinate preview
     */
    _formatCoordPreview(coords, attrs) {
        if (coords.length === 0)
            return '[]';
        if (coords.length <= 3) {
            return `[${coords.map(val => formatCoordinateValue(val, attrs)).join(', ')}]`;
        }
        return `[${formatCoordinateValue(coords[0], attrs)}, ${formatCoordinateValue(coords[1], attrs)}, ..., ${formatCoordinateValue(coords[coords.length - 1], attrs)}]`;
    }
    /**
     * Custom Node.js inspector (optional, only works in Node.js)
     */
    [Symbol.for('nodejs.util.inspect.custom')]() {
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
    getSizeEstimation(varName, selection) {
        const dataArray = this.getVariable(varName);
        // Get bytes per element from data type in attrs
        const dataType = dataArray.attrs._zarr_data_type;
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
            }
            else if (typeof sel === 'number') {
                // Single index - this dimension contributes 1
                totalElements *= 1;
            }
            else if (Array.isArray(sel)) {
                // Array [start, stop]
                totalElements *= (sel[1] - sel[0]);
            }
            else if (typeof sel === 'object' && 'start' in sel && 'stop' in sel) {
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
    static async open_zarr(store, options) {
        return ZarrBackend.open(store, options);
    }
    /**
     * Build chunk-specific selection based on reference chunk
     */
    _buildChunkSelection(baseSelection, referenceChunk, chunkDim) {
        const chunkSelection = { ...baseSelection };
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
//# sourceMappingURL=Dataset.js.map