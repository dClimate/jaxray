/**
 * Tests for lazy selection utilities
 */

import { describe, test, expect, vi } from 'vitest';
import {
  mapIndexToOriginal,
  performLazySelection,
  LazySelectionParams
} from '../../src/utils/lazy-selection.js';
import { Attributes, LazyIndexRange } from '../../src/types.js';

describe('mapIndexToOriginal', () => {
  test('should return current index when no mapping exists', () => {
    const result = mapIndexToOriginal(undefined, 'x', 5);
    expect(result).toBe(5);
  });

  test('should return current index when dimension not in mapping', () => {
    const mapping = { y: [10, 20, 30] };
    const result = mapIndexToOriginal(mapping, 'x', 5);
    expect(result).toBe(5);
  });

  test('should map to original index when mapping exists', () => {
    const mapping = { x: [10, 20, 30, 40, 50] };
    const result = mapIndexToOriginal(mapping, 'x', 2);
    expect(result).toBe(30);
  });

  test('should handle multiple dimensions', () => {
    const mapping = {
      x: [5, 10, 15, 20],
      y: [100, 200, 300]
    };
    expect(mapIndexToOriginal(mapping, 'x', 1)).toBe(10);
    expect(mapIndexToOriginal(mapping, 'y', 2)).toBe(300);
  });

  test('should handle zero index', () => {
    const mapping = { x: [7, 14, 21] };
    const result = mapIndexToOriginal(mapping, 'x', 0);
    expect(result).toBe(7);
  });
});

describe('performLazySelection', () => {
  // Helper to create mock lazy loader
  const createMockLoader = (expectedData?: any) => {
    return vi.fn(async (ranges: Record<string, LazyIndexRange>) => {
      return expectedData || ranges;
    });
  };

  describe('no selection (undefined)', () => {
    test('should preserve dimension when no selection is provided', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: {},
        dims: ['x', 'y'],
        shape: [5, 10],
        coords: {
          x: [0, 1, 2, 3, 4],
          y: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.dims).toEqual(['x', 'y']);
      expect(result.virtualShape).toEqual([5, 10]);
      expect(result.coords.x).toEqual([0, 1, 2, 3, 4]);
      expect(result.coords.y).toEqual([10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
    });

    test('should create identity mapping when no selection provided', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: {},
        dims: ['x'],
        shape: [3],
        coords: { x: [10, 20, 30] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.originalIndexMapping.x).toEqual([0, 1, 2]);
    });
  });

  describe('scalar selection', () => {
    test('should remove dimension on scalar numeric selection', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: 2 },
        dims: ['x', 'y'],
        shape: [5, 10],
        coords: {
          x: [0, 1, 2, 3, 4],
          y: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.dims).toEqual(['y']);
      expect(result.virtualShape).toEqual([10]);
      expect(result.coords.x).toBeUndefined();
      expect(result.coords.y).toEqual([10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
    });

    test('should handle string coordinate selection', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { labels: 'b' },
        dims: ['labels', 'x'],
        shape: [3, 5],
        coords: {
          labels: ['a', 'b', 'c'],
          x: [0, 1, 2, 3, 4]
        },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.dims).toEqual(['x']);
      expect(result.coords.labels).toBeUndefined();
    });

    test('should handle Date coordinate selection', () => {
      const loader = createMockLoader();
      const dates = [
        new Date('2020-01-01'),
        new Date('2020-01-02'),
        new Date('2020-01-03')
      ];
      const params: LazySelectionParams = {
        selection: { time: new Date('2020-01-02') },
        dims: ['time', 'x'],
        shape: [3, 5],
        coords: {
          time: dates,
          x: [0, 1, 2, 3, 4]
        },
        attrs: {
          _coordAttrs: {
            time: { units: 'days since 1970-01-01' }
          }
        },
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.dims).toEqual(['x']);
    });

    test('should use originalIndexMapping when provided', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: 20 }, // Select coordinate value 20 (at index 1)
        dims: ['x', 'y'],
        shape: [3, 2],
        coords: {
          x: [10, 20, 30],
          y: [100, 200]
        },
        attrs: {},
        originalIndexMapping: {
          x: [5, 10, 15], // Already remapped from parent
          y: [0, 1]
        },
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      // When we select coordinate value 20 (index 1), it should map to original index 10
      const testLoader = result.lazyLoader;
      testLoader({ y: { start: 0, stop: 2 } });

      expect(loader).toHaveBeenCalledWith(
        expect.objectContaining({
          x: 10 // Should use the originalIndexMapping
        })
      );
    });
  });

  describe('array selection', () => {
    test('should handle array selection', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: [10, 20, 30] }, // Select coordinate values (at indices 1, 2, 3)
        dims: ['x', 'y'],
        shape: [5, 3],
        coords: {
          x: [0, 10, 20, 30, 40],
          y: [100, 200, 300]
        },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.dims).toEqual(['x', 'y']);
      expect(result.virtualShape).toEqual([3, 3]);
      expect(result.coords.x).toEqual([10, 20, 30]);
    });

    test('should create correct mapping for array selection', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: [10, 30, 40] }, // Select coordinates at indices 1, 3, 4
        dims: ['x'],
        shape: [5],
        coords: { x: [0, 10, 20, 30, 40] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      // Array selection creates mapping from min to max index (1 to 4)
      expect(result.originalIndexMapping.x).toEqual([1, 2, 3, 4]);
    });

    test('should handle non-contiguous array selections', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: [0, 20, 40] }, // Select coordinates at indices 0, 2, 4
        dims: ['x'],
        shape: [5],
        coords: { x: [0, 10, 20, 30, 40] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      // Array selection creates mapping from min to max index (0 to 4)
      expect(result.originalIndexMapping.x).toEqual([0, 1, 2, 3, 4]);
      expect(result.coords.x).toEqual([0, 10, 20, 30, 40]);
    });
  });

  describe('slice selection', () => {
    test('should handle slice with start and stop', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: { start: 10, stop: 30 } }, // Start at 10 (index 1), stop at 30 (index 3, inclusive)
        dims: ['x', 'y'],
        shape: [5, 2],
        coords: {
          x: [0, 10, 20, 30, 40],
          y: [100, 200]
        },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.dims).toEqual(['x', 'y']);
      expect(result.virtualShape).toEqual([3, 2]);
      expect(result.coords.x).toEqual([10, 20, 30]);
      expect(result.originalIndexMapping.x).toEqual([1, 2, 3]);
    });

    test('should handle slice with only start', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: { start: 20 } }, // Start at coordinate value 20 (index 2)
        dims: ['x'],
        shape: [5],
        coords: { x: [0, 10, 20, 30, 40] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.virtualShape).toEqual([3]);
      expect(result.coords.x).toEqual([20, 30, 40]);
      expect(result.originalIndexMapping.x).toEqual([2, 3, 4]);
    });

    test('should handle slice with only stop', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: { stop: 30 } }, // Stop at coordinate value 30 (index 3, exclusive)
        dims: ['x'],
        shape: [5],
        coords: { x: [0, 10, 20, 30, 40] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.virtualShape).toEqual([4]);
      expect(result.coords.x).toEqual([0, 10, 20, 30]);
      expect(result.originalIndexMapping.x).toEqual([0, 1, 2, 3]);
    });

    test('should handle 1 slice', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: { start: 30, stop: 30 } }, // Empty: start == stop
        dims: ['x'],
        shape: [5],
        coords: { x: [0, 10, 20, 30, 40] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.virtualShape).toEqual([1]);
      expect(result.coords.x).toEqual([30]);
      expect(result.originalIndexMapping.x).toEqual([3]);
    });

    test('should handle slice with coordinate values', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: { start: 10, stop: 30 } },
        dims: ['x'],
        shape: [5],
        coords: { x: [0, 10, 20, 30, 40] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      // start=10 maps to index 1, stop=30 maps to index 3, so we get indices 1,2,3
      expect(result.virtualShape).toEqual([3]);
      expect(result.coords.x).toEqual([10, 20, 30]);
    });
  });

  describe('lazy loader behavior', () => {
    test('should pass fixed indices for scalar selections', async () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: 2 },
        dims: ['x', 'y'],
        shape: [5, 3],
        coords: {
          x: [0, 1, 2, 3, 4],
          y: [10, 20, 30]
        },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);
      await result.lazyLoader({ y: { start: 0, stop: 3 } });

      expect(loader).toHaveBeenCalledWith({
        x: 2,
        y: { start: 0, stop: 3 }
      });
    });

    test('should transform ranges using original index mapping', async () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: { start: 10, stop: 40 } }, // Select 10,20,30,40 (indices 1-4)
        dims: ['x', 'y'],
        shape: [5, 2],
        coords: {
          x: [0, 10, 20, 30, 40],
          y: [100, 200]
        },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      // Request first 2 elements of the virtual array (indices 0-1)
      await result.lazyLoader({ x: { start: 0, stop: 2 }, y: { start: 0, stop: 2 } });

      // Should map to original indices 1-2
      expect(loader).toHaveBeenCalledWith({
        x: { start: 1, stop: 3 },
        y: { start: 0, stop: 2 }
      });
    });

    test('should handle undefined requested ranges', async () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: { start: 10, stop: 30 } }, // Select 10,20,30 (indices 1-3)
        dims: ['x'],
        shape: [5],
        coords: { x: [0, 10, 20, 30, 40] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);
      await result.lazyLoader({});

      // Should request full range of selected data
      expect(loader).toHaveBeenCalledWith({
        x: { start: 1, stop: 4 }
      });
    });

    test('should handle scalar requested index', async () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: { start: 10, stop: 30 } }, // Select coords 10, 20, 30 (indices 1-3)
        dims: ['x'],
        shape: [5],
        coords: { x: [0, 10, 20, 30, 40] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);
      // Result has coords [10, 20, 30], so virtual index 1 maps to original index 2
      await result.lazyLoader({ x: 1 });

      // Virtual index 1 should map to original index 2
      expect(loader).toHaveBeenCalledWith({
        x: 2
      });
    });

    test('should clamp out-of-bounds scalar indices', async () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: { start: 10, stop: 30 } }, // Select coords 10, 20, 30 (indices 1-3)
        dims: ['x'],
        shape: [5],
        coords: { x: [0, 10, 20, 30, 40] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);
      // Result has 3 elements, requesting index 100 should clamp to last element (original index 3)
      await result.lazyLoader({ x: 100 });

      // Should clamp to max original index (3)
      expect(loader).toHaveBeenCalledWith({
        x: 3
      });
    });

    test('should handle single element selection', async () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: { start: 30, stop: 30 } }, // Single element at coordinate 30 (index 3)
        dims: ['x'],
        shape: [5],
        coords: { x: [0, 10, 20, 30, 40] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);
      // Selection of {start: 30, stop: 30} results in one element at index 3
      expect(result.virtualShape).toEqual([1]);
      await result.lazyLoader({ x: { start: 0, stop: 1 } });

      expect(loader).toHaveBeenCalledWith({
        x: { start: 3, stop: 4 }
      });
    });
  });

  describe('attributes and metadata', () => {
    test('should deep clone attributes', () => {
      const loader = createMockLoader();
      const attrs: Attributes = {
        description: 'test data',
        nested: { value: 42 }
      };

      const params: LazySelectionParams = {
        selection: {},
        dims: ['x'],
        shape: [3],
        coords: { x: [0, 1, 2] },
        attrs,
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.attrs).toEqual(attrs);
      expect(result.attrs).not.toBe(attrs);
      expect((result.attrs as any).nested).not.toBe((attrs as any).nested);
    });

    test('should preserve name', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: {},
        dims: ['x'],
        shape: [3],
        coords: { x: [0, 1, 2] },
        attrs: {},
        name: 'myArray',
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.name).toBe('myArray');
    });
  });

  describe('complex multi-dimensional selections', () => {
    test('should handle mixed selection types', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: {
          x: 20, // Select coordinate value 20 (index 2)
          y: { start: 200, stop: 300 }, // Start at 200 (index 1), stop at 300 (index 2, exclusive so up to index 3)
          z: [1000, 3000] // Select coordinate values at indices 0 and 2
        },
        dims: ['x', 'y', 'z'],
        shape: [5, 4, 3],
        coords: {
          x: [0, 10, 20, 30, 40],
          y: [100, 200, 300, 400],
          z: [1000, 2000, 3000]
        },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.dims).toEqual(['y', 'z']);
      expect(result.virtualShape).toEqual([2, 3]); // y has 2 elements (200, 300), z has full range 0-2
      expect(result.coords.y).toEqual([200, 300]);
      expect(result.coords.z).toEqual([1000, 2000, 3000]); // Array selection includes full min-max range
    });

    test('should compose multiple lazy selections correctly', async () => {
      const baseLoader = createMockLoader();

      // First selection: slice x from 1 to 4 (coordinate values, indices 1-4)
      const params1: LazySelectionParams = {
        selection: { x: { start: 1, stop: 4 } },
        dims: ['x', 'y'],
        shape: [10, 5],
        coords: {
          x: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
          y: [10, 20, 30, 40, 50]
        },
        attrs: {},
        lazyLoader: baseLoader
      };

      const result1 = performLazySelection(params1);

      // After first selection, we have coords [1, 2, 3, 4] (4 elements)
      expect(result1.virtualShape).toEqual([4, 5]);

      // Second selection: slice x from coordinate value 2 to 3 (gets element at index 1 in the sliced array)
      const params2: LazySelectionParams = {
        selection: { x: { start: 2, stop: 3 } },
        dims: result1.dims,
        shape: result1.virtualShape,
        coords: result1.coords,
        attrs: result1.attrs,
        originalIndexMapping: result1.originalIndexMapping,
        lazyLoader: result1.lazyLoader
      };

      const result2 = performLazySelection(params2);

      // Final virtual shape should be [2, 5] (two x elements, 5 y elements)
      expect(result2.virtualShape).toEqual([2, 5]);

      // Request the data
      await result2.lazyLoader({ x: { start: 0, stop: 2 }, y: { start: 0, stop: 5 } });

      // Should map to original index 2
      expect(baseLoader).toHaveBeenCalledWith({
        x: { start: 2, stop: 4 },
        y: { start: 0, stop: 5 }
      });
    });

    test('should compose array selections correctly', async () => {
      const baseLoader = createMockLoader();

      // First selection: array selection picks indices [1, 2, 3, 4]
      const params1: LazySelectionParams = {
        selection: { x: [1, 2, 3, 4] },
        dims: ['x', 'y'],
        shape: [10, 5],
        coords: {
          x: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
          y: [10, 20, 30, 40, 50]
        },
        attrs: {},
        lazyLoader: baseLoader
      };

      const result1 = performLazySelection(params1);

      // After first selection, we have coords [1, 2, 3, 4] (4 elements)
      expect(result1.virtualShape).toEqual([4, 5]);
      expect(result1.coords.x).toEqual([1, 2, 3, 4]);

      // Second selection: array selection from the result
      const params2: LazySelectionParams = {
        selection: { x: [2, 3] }, // Pick coordinate values 2 and 3
        dims: result1.dims,
        shape: result1.virtualShape,
        coords: result1.coords,
        attrs: result1.attrs,
        originalIndexMapping: result1.originalIndexMapping,
        lazyLoader: result1.lazyLoader
      };

      const result2 = performLazySelection(params2);

      // Final virtual shape should be [2, 5]
      expect(result2.virtualShape).toEqual([2, 5]);
      expect(result2.coords.x).toEqual([2, 3]);

      // Request the data
      await result2.lazyLoader({ x: { start: 0, stop: 2 }, y: { start: 0, stop: 5 } });

      // Should map to original indices 2-4 (coordinates 2 and 3 are at original indices 2-3)
      expect(baseLoader).toHaveBeenCalledWith({
        x: { start: 2, stop: 4 },
        y: { start: 0, stop: 5 }
      });
    });

    test('should compose slice on array selection', async () => {
      const baseLoader = createMockLoader();

      // First selection: array selection
      const params1: LazySelectionParams = {
        selection: { x: [10, 20, 30, 40, 50] },
        dims: ['x'],
        shape: [10],
        coords: {
          x: [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
        },
        attrs: {},
        lazyLoader: baseLoader
      };

      const result1 = performLazySelection(params1);
      expect(result1.coords.x).toEqual([10, 20, 30, 40, 50]);

      // Second selection: slice on the array result
      const params2: LazySelectionParams = {
        selection: { x: { start: 20, stop: 40 } },
        dims: result1.dims,
        shape: result1.virtualShape,
        coords: result1.coords,
        attrs: result1.attrs,
        originalIndexMapping: result1.originalIndexMapping,
        lazyLoader: result1.lazyLoader
      };

      const result2 = performLazySelection(params2);

      expect(result2.virtualShape).toEqual([3]);
      expect(result2.coords.x).toEqual([20, 30, 40]);

      // Request the data
      await result2.lazyLoader({ x: { start: 0, stop: 3 } });

      // Should map to original indices 2-5 (coords [20,30,40] are at original indices 2,3,4)
      expect(baseLoader).toHaveBeenCalledWith({
        x: { start: 2, stop: 5 }
      });
    });

    test('should compose array on slice selection', async () => {
      const baseLoader = createMockLoader();

      // First selection: slice
      const params1: LazySelectionParams = {
        selection: { x: { start: 1, stop: 5 } },
        dims: ['x'],
        shape: [10],
        coords: {
          x: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        },
        attrs: {},
        lazyLoader: baseLoader
      };

      const result1 = performLazySelection(params1);
      expect(result1.coords.x).toEqual([1, 2, 3, 4, 5]);

      // Second selection: array on the slice result
      const params2: LazySelectionParams = {
        selection: { x: [2, 4] },
        dims: result1.dims,
        shape: result1.virtualShape,
        coords: result1.coords,
        attrs: result1.attrs,
        originalIndexMapping: result1.originalIndexMapping,
        lazyLoader: result1.lazyLoader
      };

      const result2 = performLazySelection(params2);

      expect(result2.virtualShape).toEqual([3]); // Array min=2, max=4, so indices 1-3 in result1
      expect(result2.coords.x).toEqual([2, 3, 4]);

      // Request the data
      await result2.lazyLoader({ x: { start: 0, stop: 3 } });

      // Coords [2,3,4] are at virtual indices [1,2,3] in result1,
      // which map to original indices [2,3,4]
      expect(baseLoader).toHaveBeenCalledWith({
        x: { start: 2, stop: 5 }
      });
    });
  });

  describe('edge cases', () => {
    test('should handle single element dimension', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: {},
        dims: ['x'],
        shape: [1],
        coords: { x: [42] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.virtualShape).toEqual([1]);
      expect(result.coords.x).toEqual([42]);
    });

    test('should handle single length dimension after selection', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: { start: 3, stop: 3 } }, // Same start and stop = 1 selection
        dims: ['x'],
        shape: [5],
        coords: { x: [0, 1, 2, 3, 4] },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.virtualShape).toEqual([1]);
      expect(result.coords.x).toEqual([3]);
    });

    test('should preserve coordinates for unselected dimensions', () => {
      const loader = createMockLoader();
      const params: LazySelectionParams = {
        selection: { x: 10 }, // Select coordinate value 10 (which is at index 1)
        dims: ['x', 'y', 'z'],
        shape: [3, 4, 5],
        coords: {
          x: [0, 10, 20],
          y: [100, 200, 300, 400],
          z: [1, 2, 3, 4, 5]
        },
        attrs: {},
        lazyLoader: loader
      };

      const result = performLazySelection(params);

      expect(result.coords.y).toEqual([100, 200, 300, 400]);
      expect(result.coords.z).toEqual([1, 2, 3, 4, 5]);
    });
  });
});
