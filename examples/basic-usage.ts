/**
 * Basic usage examples for jaxray
 */

import { DataArray, Dataset } from '../src';

async function main() {
  // Example 1: Creating a simple 1D DataArray
  console.log('=== Example 1: Simple 1D DataArray ===');
  const temperatures = new DataArray([20, 22, 25, 23, 21], {
    dims: ['time'],
    coords: {
      time: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
    },
    attrs: {
      units: 'celsius',
      description: 'Daily temperatures'
    },
    name: 'temperature'
  });

  console.log('Data:', temperatures.data);
  console.log('Dimensions:', temperatures.dims);
  console.log('Shape:', temperatures.shape);
  console.log('Coordinates:', temperatures.coords);
  console.log('Attributes:', temperatures.attrs);

  // Example 2: Creating a 2D DataArray
  console.log('\n=== Example 2: 2D DataArray ===');
  const gridData = new DataArray(
    [
      [1, 2, 3, 4],
      [5, 6, 7, 8],
      [9, 10, 11, 12]
    ],
    {
      dims: ['y', 'x'],
      coords: {
        y: [0, 10, 20],
        x: [0, 10, 20, 30]
      },
      name: 'grid_values'
    }
  );

  console.log('2D Data:', gridData.data);
  console.log('Shape:', gridData.shape);

  // Example 3: Selecting data
  console.log('\n=== Example 3: Selecting data ===');
  const selectedTemp = await temperatures.sel({ time: 'Wed' });
  console.log('Temperature on Wednesday:', selectedTemp.data);

  const multiSelect = await temperatures.sel({ time: ['Mon', 'Wed', 'Fri'] });
  console.log('Temperatures for Mon, Wed, Fri:', multiSelect.data);

  const sliceSelect = await temperatures.sel({ time: { start: 'Tue', stop: 'Thu' } });
  console.log('Temperatures from Tue to Thu:', sliceSelect.data);

  // Example 4: Integer-based selection
  console.log('\n=== Example 4: Integer-based selection ===');
  const byIndex = await temperatures.isel({ time: 2 });
  console.log('Temperature at index 2:', byIndex.data);

  const multiIndex = await temperatures.isel({ time: [0, 2, 4] });
  console.log('Temperatures at indices 0, 2, 4:', multiIndex.data);

  // Example 5: Selection with nearest neighbor
  console.log('\n=== Example 5: Nearest neighbor selection ===');
  const numericData = new DataArray([10, 20, 30, 40, 50], {
    dims: ['x'],
    coords: {
      x: [0, 5, 10, 15, 20]
    }
  });

  const nearest = await numericData.sel({ x: 7 }, { method: 'nearest' });
  console.log('Nearest to x=7:', nearest.data); // 20 (x=5)

  const ffill = await numericData.sel({ x: 12 }, { method: 'ffill' });
  console.log('Forward fill for x=12:', ffill.data); // 30 (x=10)

  // Example 6: Aggregations
  console.log('\n=== Example 6: Aggregations ===');
  const totalSum = temperatures.sum();
  console.log('Total sum of temperatures:', totalSum);

  const avgTemp = temperatures.mean();
  console.log('Average temperature:', avgTemp);

  // 2D aggregation
  const rowSums = gridData.sum('x');
  if (rowSums instanceof DataArray) {
    console.log('Sum along x dimension:', rowSums.data);
  }

  const colSums = gridData.sum('y');
  if (colSums instanceof DataArray) {
    console.log('Sum along y dimension:', colSums.data);
  }

  // Example 7: Creating a Dataset
  console.log('\n=== Example 7: Dataset with multiple variables ===');
  const temp2D = new DataArray(
    [
      [15, 16, 17],
      [18, 19, 20]
    ],
    {
      dims: ['lat', 'lon'],
      coords: {
        lat: [40.0, 40.5],
        lon: [-74.0, -73.5, -73.0]
      }
    }
  );

  const pressure2D = new DataArray(
    [
      [1013, 1014, 1015],
      [1012, 1013, 1014]
    ],
    {
      dims: ['lat', 'lon'],
      coords: {
        lat: [40.0, 40.5],
        lon: [-74.0, -73.5, -73.0]
      }
    }
  );

  const weatherData = new Dataset(
    {
      temperature: temp2D,
      pressure: pressure2D
    },
    {
      attrs: {
        description: 'Weather data for NYC area',
        date: '2024-01-01'
      }
    }
  );

  console.log('Dataset variables:', weatherData.dataVars);
  console.log('Dataset dimensions:', weatherData.dims);
  console.log('Dataset sizes:', weatherData.sizes);

  // Example 8: Selecting from Dataset
  console.log('\n=== Example 8: Selecting from Dataset ===');
  const locationData = await weatherData.sel({ lat: 40.0, lon: -73.5 });
  const locationTemp = locationData.getVariable('temperature');
  const locationPressure = locationData.getVariable('pressure');

  console.log('Temperature at location:', locationTemp.data);
  console.log('Pressure at location:', locationPressure.data);

  // Dictionary-style access
  const temp = weatherData.get('temperature') as DataArray;
  console.log('Temperature via .get():', temp.data);

  // Example 9: Streaming large selections
  console.log('\n=== Example 9: Streaming ===');
  const largeTimeSeries = new DataArray(
    Array.from({ length: 100 }, (_, i) => i * 10),
    {
      dims: ['time'],
      coords: {
        time: Array.from({ length: 100 }, (_, i) => i)
      }
    }
  );

  const stream = largeTimeSeries.selStream(
    { time: [0, 99] },
    { chunkSize: 0.001 } // Small chunks for demo
  );

  console.log('Streaming data in chunks:');
  for await (const chunk of stream) {
    console.log(`  Chunk ${chunk.chunkIndex + 1}/${chunk.totalChunks} - Progress: ${chunk.progress}%`);
  }

  // Example 10: Merging Datasets
  console.log('\n=== Example 10: Merging Datasets ===');
  const humidity = new DataArray(
    [
      [65, 70, 75],
      [68, 72, 76]
    ],
    {
      dims: ['lat', 'lon'],
      coords: {
        lat: [40.0, 40.5],
        lon: [-74.0, -73.5, -73.0]
      }
    }
  );

  const humidityData = new Dataset({ humidity });
  const combinedData = weatherData.merge(humidityData);

  console.log('Combined dataset variables:', combinedData.dataVars);

  // Example 11: Mapping over Dataset variables
  console.log('\n=== Example 11: Mapping over Dataset ===');
  const normalizedData = weatherData.map((dataArray) => {
    const values = dataArray.data as number[][];
    const flat = values.flat();
    const min = Math.min(...flat);
    const max = Math.max(...flat);
    const range = max - min;

    const normalizedValues = values.map(row =>
      row.map(val => (val - min) / range)
    );

    return new DataArray(normalizedValues, {
      dims: dataArray.dims,
      coords: dataArray.coords,
      attrs: { ...dataArray.attrs, normalized: true },
      name: dataArray.name
    });
  });

  const normalizedTemp = normalizedData.getVariable('temperature');
  console.log('Normalized temperature data:', normalizedTemp.data);

  // Example 12: Serialization
  console.log('\n=== Example 12: Serialization ===');
  const tempObject = temperatures.toObject();
  console.log('Temperature as object:', JSON.stringify(tempObject, null, 2));

  const datasetObject = weatherData.toObject();
  console.log(
    'Dataset structure:',
    JSON.stringify(
      {
        dataVars: Object.keys(datasetObject.dataVars),
        dims: datasetObject.dims,
        sizes: datasetObject.sizes
      },
      null,
      2
    )
  );
}

// Run the examples
main().catch(console.error);
