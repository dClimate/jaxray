# jaxray

A JavaScript/TypeScript implementation similar to Python's xarray library for working with labeled multi-dimensional arrays.

## Features

- **DataArray**: Labeled, multi-dimensional arrays with named dimensions and coordinates
- **Dataset**: Collections of multiple DataArrays with shared dimensions
- **Selection**: Select data by labels (`sel`) or integer positions (`isel`)
- **Nearest Neighbor**: Support for `nearest`, `ffill`, and `bfill` selection methods with tolerance
- **Aggregations**: Compute statistics along dimensions (sum, mean)
- **Type-safe**: Written in TypeScript with full type definitions
- **Immutable operations**: All operations return new instances

## Installation

```bash
npm install jaxray
```

## Quick Start

### Creating a DataArray

```typescript
import { DataArray } from 'jaxray';

// Simple 1D array with labeled coordinates
const temperatures = new DataArray([20, 22, 25, 23, 21], {
  dims: ['time'],
  coords: {
    time: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
  },
  attrs: {
    units: 'celsius',
    description: 'Daily temperatures'
  }
});

console.log(temperatures.data);  // [20, 22, 25, 23, 21]
console.log(temperatures.shape); // [5]
```

### Creating a 2D DataArray

```typescript
const gridData = new DataArray(
  [
    [1, 2, 3],
    [4, 5, 6]
  ],
  {
    dims: ['y', 'x'],
    coords: {
      y: [0, 10],
      x: [0, 10, 20]
    }
  }
);
```

### Selecting Data

```typescript
// Select by label
const wednesday = await temperatures.sel({ time: 'Wed' });
console.log(wednesday.data); // 25

// Select multiple values
const selected = await temperatures.sel({ time: ['Mon', 'Wed', 'Fri'] });
console.log(selected.data); // [20, 25, 21]

// Slice selection
const midweek = await temperatures.sel({ time: { start: 'Tue', stop: 'Thu' } });
console.log(midweek.data); // [22, 25, 23]

// Select by integer position
const byIndex = await temperatures.isel({ time: 2 });
console.log(byIndex.data); // 25
```

### Nearest Neighbor Selection

jaxray supports xarray-style nearest neighbor lookups and interpolation methods:

```typescript
const data = new DataArray([10, 20, 30, 40, 50], {
  dims: ['x'],
  coords: {
    x: [0, 5, 10, 15, 20]
  }
});

// Find nearest coordinate
const nearest = await data.sel({ x: 7 }, { method: 'nearest' });
console.log(nearest.data); // 20 (nearest to x=7 is x=5)

// Forward fill (last value <= target)
const ffill = await data.sel({ x: 12 }, { method: 'ffill' });
console.log(ffill.data); // 30 (last value where x <= 12 is x=10)

// Backward fill (first value >= target)
const bfill = await data.sel({ x: 12 }, { method: 'bfill' });
console.log(bfill.data); // 40 (first value where x >= 12 is x=15)

// With tolerance
const tolerant = await data.sel({ x: 7 }, {
  method: 'nearest',
  tolerance: 3
});
// Succeeds because distance is 2

// This would throw an error (distance 7 > tolerance 2)
await data.sel({ x: 13 }, { method: 'nearest', tolerance: 2 });
```

### Aggregations

```typescript
// Sum all values
const total = temperatures.sum();
console.log(total); // 111

// Mean of all values
const average = temperatures.mean();
console.log(average); // 22.2

// Sum along a dimension
const rowSums = gridData.sum('x');
console.log(rowSums.data); // [6, 15]
```

### Working with Datasets

```typescript
import { DataArray, Dataset } from 'jaxray';

// Create multiple related DataArrays
const temp = new DataArray(
  [[15, 16], [18, 19]],
  {
    dims: ['lat', 'lon'],
    coords: {
      lat: [40.0, 40.5],
      lon: [-74.0, -73.5]
    }
  }
);

const pressure = new DataArray(
  [[1013, 1014], [1012, 1013]],
  {
    dims: ['lat', 'lon'],
    coords: {
      lat: [40.0, 40.5],
      lon: [-74.0, -73.5]
    }
  }
);

// Combine into a Dataset
const weather = new Dataset({
  temperature: temp,
  pressure: pressure
});

console.log(weather.dataVars); // ['temperature', 'pressure']
console.log(weather.dims);     // ['lat', 'lon']

// Select from Dataset
const location = await weather.sel({ lat: 40.0, lon: -73.5 });
const tempAtLocation = location.getVariable('temperature');
console.log(tempAtLocation?.data); // 16

// Works with nearest neighbor too
const nearLocation = await weather.sel(
  { lat: 40.2, lon: -73.7 },
  { method: 'nearest' }
);
```

### Merging Datasets

```typescript
const humidity = new DataArray([[65, 70], [68, 72]], {
  dims: ['lat', 'lon'],
  coords: {
    lat: [40.0, 40.5],
    lon: [-74.0, -73.5]
  }
});

const humidityData = new Dataset({ humidity });
const combined = weather.merge(humidityData);

console.log(combined.dataVars); // ['temperature', 'pressure', 'humidity']
```

## API Reference

### DataArray

#### Constructor

```typescript
new DataArray(data, options?)
```

**Parameters:**
- `data`: Multi-dimensional array of values
- `options`:
  - `dims`: Array of dimension names
  - `coords`: Coordinate values for each dimension
  - `attrs`: Metadata attributes
  - `name`: Name of the DataArray

#### Properties

- `data`: Get the underlying data
- `values`: Alias for data
- `dims`: Array of dimension names
- `shape`: Array of dimension sizes
- `coords`: Coordinate values
- `attrs`: Metadata attributes
- `name`: Name of the DataArray
- `ndim`: Number of dimensions
- `size`: Total number of elements

#### Methods

- `sel(selection, options?)`: Select by coordinate labels
  - `options.method`: Selection method ('nearest', 'ffill', 'bfill', 'pad', 'backfill')
  - `options.tolerance`: Maximum distance for method selection
- `isel(selection)`: Select by integer positions
- `sum(dim?)`: Sum along dimension (or all values)
- `mean(dim?)`: Mean along dimension (or all values)
- `toObject()`: Convert to plain JavaScript object
- `toJSON()`: Convert to JSON string

### Dataset

#### Constructor

```typescript
new Dataset(dataVars?, options?)
```

**Parameters:**
- `dataVars`: Object mapping variable names to DataArrays
- `options`:
  - `coords`: Shared coordinates
  - `attrs`: Metadata attributes

#### Properties

- `dataVars`: Array of variable names
- `dims`: Array of all dimension names
- `coords`: Shared coordinates
- `attrs`: Metadata attributes
- `sizes`: Object mapping dimension names to sizes

#### Methods

- `addVariable(name, dataArray)`: Add a new variable
- `getVariable(name)`: Get a variable by name (throws if not found)
- `get(key)`: Dictionary-style access - `get('varname')` or `get(['var1', 'var2'])`
- `hasVariable(name)`: Check if variable exists
- `removeVariable(name)`: Remove a variable
- `sel(selection, options?)`: Select by coordinate labels
  - `options.method`: Selection method ('nearest', 'ffill', 'bfill', 'pad', 'backfill')
  - `options.tolerance`: Maximum distance for method selection
- `isel(selection)`: Select by integer positions
- `map(fn)`: Apply function to all variables
- `merge(other)`: Merge with another Dataset
- `toObject()`: Convert to plain JavaScript object
- `toJSON()`: Convert to JSON string

## Examples

See the [examples](examples/) directory for more detailed usage examples.

## Development

```bash
# Install dependencies
npm install

# Build
npm run build

# Run tests
npm test
```

## License

ISC

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Inspiration

This library is inspired by Python's [xarray](https://xarray.pydata.org/) library, which provides labeled multi-dimensional arrays for scientific computing.
