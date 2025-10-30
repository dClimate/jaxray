/**
 * Example: Concatenating Datasets
 *
 * This example demonstrates how to concatenate datasets along a dimension,
 * which is particularly useful for combining time-series data from multiple
 * sources (e.g., finalized and non-finalized weather data).
 */

import { Dataset, DataArray } from '../src/index.js';

async function basicConcatExample() {
  console.log('=== Basic Concatenation Example ===\n');

  // Create first dataset (historical data: 2020-2022)
  const historicalTemp = new DataArray(
    [
      [15.2, 16.8, 14.5],
      [18.3, 19.1, 17.6],
      [20.5, 21.2, 19.8]
    ],
    {
      dims: ['time', 'location'],
      coords: {
        time: ['2020-01-01', '2021-01-01', '2022-01-01'],
        location: ['Station-A', 'Station-B', 'Station-C']
      },
      name: 'temperature'
    }
  );

  const historicalDataset = new Dataset(
    { temperature: historicalTemp },
    { attrs: { source: 'historical_archive' } }
  );

  // Create second dataset (recent data: 2023-2024)
  const recentTemp = new DataArray(
    [
      [22.1, 23.4, 21.7],
      [24.2, 25.1, 23.9]
    ],
    {
      dims: ['time', 'location'],
      coords: {
        time: ['2023-01-01', '2024-01-01'],
        location: ['Station-A', 'Station-B', 'Station-C']
      },
      name: 'temperature'
    }
  );

  const recentDataset = new Dataset(
    { temperature: recentTemp },
    { attrs: { source: 'recent_observations' } }
  );

  // Concatenate datasets along time dimension
  const combined = historicalDataset.concat(recentDataset, { dim: 'time' });

  console.log('Combined dataset info:');
  console.log(`  Dimensions: ${combined.dims.join(', ')}`);
  console.log(`  Time points: ${combined.sizes.time}`);
  console.log(`  Locations: ${combined.sizes.location}`);
  console.log(`  Is lazy: ${combined.getVariable('temperature').isLazy}\n`);

  // Query only historical data
  console.log('Querying historical data (2020-2021):');
  const historical = await combined.sel({ time: ['2020-01-01', '2021-01-01'] });
  const historicalComputed = await historical.compute();
  console.log(historicalComputed.getVariable('temperature').data);
  console.log();

  // Query only recent data
  console.log('Querying recent data (2023-2024):');
  const recent = await combined.sel({ time: ['2023-01-01', '2024-01-01'] });
  const recentComputed = await recent.compute();
  console.log(recentComputed.getVariable('temperature').data);
  console.log();

  // Query across both datasets
  console.log('Querying across both datasets (2021-2023):');
  const crossDataset = await combined.sel({
    time: ['2021-01-01', '2022-01-01', '2023-01-01']
  });
  const crossComputed = await crossDataset.compute();
  console.log(crossComputed.getVariable('temperature').data);
  console.log();
}

async function weatherDataExample() {
  console.log('=== Weather Data Example (Finalized + Non-Finalized) ===\n');

  // Simulate finalized weather data (older, quality-controlled)
  const finalizedTemp = new DataArray(
    [
      [10.5, 11.2],
      [12.1, 13.5],
      [14.8, 15.2],
      [16.3, 17.1],
      [18.9, 19.4]
    ],
    {
      dims: ['time', 'station'],
      coords: {
        time: ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05'],
        station: ['NYC', 'BOS']
      }
    }
  );

  const finalizedPressure = new DataArray(
    [
      [1013, 1015],
      [1012, 1014],
      [1014, 1016],
      [1013, 1015],
      [1015, 1017]
    ],
    {
      dims: ['time', 'station'],
      coords: {
        time: ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05'],
        station: ['NYC', 'BOS']
      }
    }
  );

  const finalizedDataset = new Dataset(
    { temperature: finalizedTemp, pressure: finalizedPressure },
    { attrs: { status: 'finalized', qc_level: 'complete' } }
  );

  // Simulate non-finalized weather data (newer, preliminary)
  const nonFinalizedTemp = new DataArray(
    [
      [20.1, 21.3],
      [22.5, 23.8],
      [24.2, 25.1],
      [25.8, 26.4]
    ],
    {
      dims: ['time', 'station'],
      coords: {
        time: ['2024-06', '2024-07', '2024-08', '2024-09'],
        station: ['NYC', 'BOS']
      }
    }
  );

  const nonFinalizedPressure = new DataArray(
    [
      [1016, 1018],
      [1017, 1019],
      [1015, 1017],
      [1014, 1016]
    ],
    {
      dims: ['time', 'station'],
      coords: {
        time: ['2024-06', '2024-07', '2024-08', '2024-09'],
        station: ['NYC', 'BOS']
      }
    }
  );

  const nonFinalizedDataset = new Dataset(
    { temperature: nonFinalizedTemp, pressure: nonFinalizedPressure },
    { attrs: { status: 'provisional', qc_level: 'preliminary' } }
  );

  // Concatenate finalized and non-finalized data
  const fullDataset = finalizedDataset.concat(nonFinalizedDataset, { dim: 'time' });

  console.log('Full dataset (finalized + non-finalized):');
  console.log(`  Time range: ${fullDataset.coords.time[0]} to ${fullDataset.coords.time[fullDataset.coords.time.length - 1]}`);
  console.log(`  Total months: ${fullDataset.sizes.time}`);
  console.log(`  Variables: ${fullDataset.dataVars.join(', ')}`);
  console.log();

  // Query specific time range spanning both datasets
  console.log('Querying Apr-Jul 2024 (spans both datasets):');
  const spring2024 = await fullDataset.sel({
    time: ['2024-04', '2024-05', '2024-06', '2024-07'],
    station: 'NYC'
  });
  const computed = await spring2024.compute();
  console.log('Temperature:', computed.getVariable('temperature').data);
  console.log('Pressure:', computed.getVariable('pressure').data);
  console.log();
}

async function lazyLoadingExample() {
  console.log('=== Lazy Loading Example ===\n');

  // Simulate a lazy dataset (e.g., from Zarr/IPFS)
  const lazyLoader1 = async (ranges: any) => {
    console.log('  Fetching from Dataset 1 with ranges:', ranges);
    const timeRange = ranges.time;
    if (typeof timeRange === 'object') {
      const count = timeRange.stop - timeRange.start;
      return Array.from({ length: count }, (_, i) => [
        (timeRange.start + i) * 10,
        (timeRange.start + i) * 10 + 5
      ]);
    }
    return [[0, 5]];
  };

  const lazyLoader2 = async (ranges: any) => {
    console.log('  Fetching from Dataset 2 with ranges:', ranges);
    const timeRange = ranges.time;
    if (typeof timeRange === 'object') {
      const count = timeRange.stop - timeRange.start;
      return Array.from({ length: count }, (_, i) => [
        (timeRange.start + i) * 100,
        (timeRange.start + i) * 100 + 50
      ]);
    }
    return [[0, 50]];
  };

  const lazy1 = new DataArray(null, {
    lazy: true,
    virtualShape: [3, 2],
    lazyLoader: lazyLoader1,
    dims: ['time', 'location'],
    coords: {
      time: ['2020', '2021', '2022'],
      location: ['A', 'B']
    }
  });

  const lazy2 = new DataArray(null, {
    lazy: true,
    virtualShape: [2, 2],
    lazyLoader: lazyLoader2,
    dims: ['time', 'location'],
    coords: {
      time: ['2023', '2024'],
      location: ['A', 'B']
    }
  });

  const ds1 = new Dataset({ data: lazy1 });
  const ds2 = new Dataset({ data: lazy2 });

  const combined = ds1.concat(ds2, { dim: 'time' });

  console.log('Querying only from first dataset (2020-2021):');
  const result1 = await combined.sel({ time: ['2020', '2021'] });
  const computed1 = await result1.compute();
  console.log(computed1.getVariable('data').data);
  console.log();

  console.log('Querying only from second dataset (2023-2024):');
  const result2 = await combined.sel({ time: ['2023', '2024'] });
  const computed2 = await result2.compute();
  console.log(computed2.getVariable('data').data);
  console.log();

  console.log('Querying across both datasets (2021-2023):');
  const result3 = await combined.sel({ time: ['2021', '2022', '2023'] });
  const computed3 = await result3.compute();
  console.log(computed3.getVariable('data').data);
  console.log();
}

// Run examples
async function main() {
  await basicConcatExample();
  await weatherDataExample();
  await lazyLoadingExample();
}

main().catch(console.error);
