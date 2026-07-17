import { describe, expect, test } from 'vitest';
import { ZarrBackend } from '../../src/backends/zarr';
import { reshape, reshapeFlat } from '../../src/utils';
import { MemoryZarrStore } from '../helpers/MemoryZarrStore';

interface ReshapeCase {
  label: string;
  shape: number[];
  expected: unknown;
}

const cases: ReshapeCase[] = [
  {
    label: '[6]',
    shape: [6],
    expected: [0, 1, 2, 3, 4, 5]
  },
  {
    label: '[2,3]',
    shape: [2, 3],
    expected: [
      [0, 1, 2],
      [3, 4, 5]
    ]
  },
  {
    label: '[2,3,4]',
    shape: [2, 3, 4],
    expected: [
      [
        [0, 1, 2, 3],
        [4, 5, 6, 7],
        [8, 9, 10, 11]
      ],
      [
        [12, 13, 14, 15],
        [16, 17, 18, 19],
        [20, 21, 22, 23]
      ]
    ]
  },
  {
    label: '[1,1,2]',
    shape: [1, 1, 2],
    expected: [[[0, 1]]]
  },
  {
    label: '[3,4,5]',
    shape: [3, 4, 5],
    expected: [
      [
        [0, 1, 2, 3, 4],
        [5, 6, 7, 8, 9],
        [10, 11, 12, 13, 14],
        [15, 16, 17, 18, 19]
      ],
      [
        [20, 21, 22, 23, 24],
        [25, 26, 27, 28, 29],
        [30, 31, 32, 33, 34],
        [35, 36, 37, 38, 39]
      ],
      [
        [40, 41, 42, 43, 44],
        [45, 46, 47, 48, 49],
        [50, 51, 52, 53, 54],
        [55, 56, 57, 58, 59]
      ]
    ]
  },
  {
    label: '[2,2,2,2]',
    shape: [2, 2, 2, 2],
    expected: [
      [
        [
          [0, 1],
          [2, 3]
        ],
        [
          [4, 5],
          [6, 7]
        ]
      ],
      [
        [
          [8, 9],
          [10, 11]
        ],
        [
          [12, 13],
          [14, 15]
        ]
      ]
    ]
  }
];

function elementCount(shape: number[]): number {
  return shape.reduce((count, size) => count * size, 1);
}

describe('zarr flat data reshape equivalence', () => {
  for (const { label, shape, expected } of cases) {
    test(`plain array preserves the legacy nested result for shape ${label}`, () => {
      const data = Array.from({ length: elementCount(shape) }, (_, index) => index);

      expect(reshapeFlat(data, shape)).toEqual(expected);
      expect(reshapeFlat(data, shape)).toEqual(
        reshape(Array.from(data), shape)
      );
    });

    test(`Float64Array preserves the legacy nested result for shape ${label}`, () => {
      const data = Float64Array.from(
        { length: elementCount(shape) },
        (_, index) => index
      );

      expect(reshapeFlat(data, shape)).toEqual(expected);
      expect(reshapeFlat(data, shape)).toEqual(
        reshape(Array.from(data), shape)
      );
    });
  }

  test('backend read preserves the legacy nested result', async () => {
    const values = new Float64Array([0, 1, 2, 3, 4, 5]);
    const store = new MemoryZarrStore({
      'zarr.json': { node_type: 'group', attributes: {} },
      'data/zarr.json': {
        node_type: 'array',
        shape: [2, 3],
        data_type: 'float64',
        dimension_names: ['y', 'x']
      }
    });
    store.set('data/c/0/0', new Uint8Array(values.buffer.slice(0)));

    const dataset = await ZarrBackend.open(store);
    const computed = await dataset.getVariable('data').compute();

    expect(computed.data).toEqual([
      [0, 1, 2],
      [3, 4, 5]
    ]);
  });

  test('backend read preserves a 3D legacy nested result', async () => {
    const values = Float64Array.from({ length: 24 }, (_, index) => index);
    const store = new MemoryZarrStore({
      'zarr.json': { node_type: 'group', attributes: {} },
      'data/zarr.json': {
        node_type: 'array',
        shape: [2, 3, 4],
        data_type: 'float64',
        dimension_names: ['t', 'y', 'x']
      }
    });
    store.set('data/c/0/0/0', new Uint8Array(values.buffer.slice(0)));

    const dataset = await ZarrBackend.open(store);
    const computed = await dataset.getVariable('data').compute();

    expect(computed.data).toEqual([
      [
        [0, 1, 2, 3],
        [4, 5, 6, 7],
        [8, 9, 10, 11]
      ],
      [
        [12, 13, 14, 15],
        [16, 17, 18, 19],
        [20, 21, 22, 23]
      ]
    ]);
  });
});

// Isolated Node benchmark on Apple M1 Max, shape [45, 300, 300] (4.05M
// Float64 values), after 2 warmups: 7-run medians were 86.4ms for the current
// Array.from + reshape path (84.6ms best) and 80.5ms for the direct flat-builder
// prototype (79.3ms best), a 1.07x separation. This is below the required 2x
// gate, so this RED artifact intentionally has no timing assertion.
