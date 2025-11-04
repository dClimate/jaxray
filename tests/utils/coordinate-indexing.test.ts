/**
 * Tests for coordinate indexing utilities
 */

import { describe, test, expect } from 'vitest';
import {
  findCoordinateIndex,
  findIndexFallback,
  findNearestIndex,
  findFfillIndex,
  findBfillIndex,
  isCoordsSorted,
  binarySearchNearest,
  binarySearchFfill,
  binarySearchBfill
} from '../../src/utils/coordinate-indexing';

describe('findCoordinateIndex', () => {
  describe('exact match', () => {
    test('should find exact numeric match', () => {
      const coords = [10, 20, 30, 40, 50];
      const index = findCoordinateIndex(coords, 30, undefined, 'x');
      expect(index).toBe(2);
    });

    test('should find exact string match', () => {
      const coords = ['a', 'b', 'c', 'd'];
      const index = findCoordinateIndex(coords, 'c', undefined, 'labels');
      expect(index).toBe(2);
    });

    test('should throw error when exact match not found', () => {
      const coords = [10, 20, 30];
      expect(() => findCoordinateIndex(coords, 25, undefined, 'x'))
        .toThrow(/not found/);
    });
  });

  describe('evenly-spaced optimization', () => {
    test('should use O(1) arithmetic for evenly-spaced coordinates', () => {
      const coords = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
      const index = findCoordinateIndex(coords, 60, undefined, 'x');
      expect(index).toBe(6);
    });

    test('should handle evenly-spaced negative coordinates', () => {
      const coords = [-50, -40, -30, -20, -10, 0, 10, 20, 30];
      const index = findCoordinateIndex(coords, -20, undefined, 'x');
      expect(index).toBe(3);
    });

    test('should handle evenly-spaced decimal coordinates', () => {
      const coords = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0];
      const index = findCoordinateIndex(coords, 1.5, undefined, 'x');
      expect(index).toBe(3);
    });

    test('should use nearest method on evenly-spaced coords', () => {
      const coords = [0, 10, 20, 30, 40, 50];
      const index = findCoordinateIndex(coords, 23, { method: 'nearest' }, 'x');
      expect(index).toBe(2); // 20 is closer than 30
    });

    test('should use ffill method on evenly-spaced coords', () => {
      const coords = [0, 10, 20, 30, 40, 50];
      const index = findCoordinateIndex(coords, 23, { method: 'ffill' }, 'x');
      expect(index).toBe(2); // Last value <= 23
    });

    test('should use bfill method on evenly-spaced coords', () => {
      const coords = [0, 10, 20, 30, 40, 50];
      const index = findCoordinateIndex(coords, 23, { method: 'bfill' }, 'x');
      expect(index).toBe(3); // First value >= 23
    });

    test('should throw when value not close enough to any coordinate', () => {
      const coords = [0, 10, 20, 30, 40, 50];
      // Value 25 is 5 away from both 20 and 30, so with tolerance of 2 in index space
      // it should fail on exact match since it's not close to an integer index
      expect(() => findCoordinateIndex(coords, 25, { tolerance: 2 }, 'x'))
        .toThrow(/not found/);
    });

    test('should accept value within tolerance on evenly-spaced coords', () => {
      const coords = [0, 10, 20, 30, 40, 50];
      const index = findCoordinateIndex(coords, 22, { tolerance: 3 }, 'x');
      expect(index).toBe(2); // 22 is close enough to 20
    });
  });

  describe('non-evenly-spaced coordinates', () => {
    test('should fallback to linear search for non-evenly-spaced', () => {
      const coords = [1, 3, 7, 15, 31, 63];
      const index = findCoordinateIndex(coords, 15, undefined, 'x');
      expect(index).toBe(3);
    });

    test('should use nearest on non-evenly-spaced', () => {
      const coords = [1, 3, 7, 15, 31, 63];
      const index = findCoordinateIndex(coords, 10, { method: 'nearest' }, 'x');
      expect(index).toBe(2); // 7 is closer
    });
  });

  describe('time coordinates', () => {
    test('should handle Date objects', () => {
      const coords = [
        new Date('2020-01-01'),
        new Date('2020-01-02'),
        new Date('2020-01-03')
      ];
      const value = new Date('2020-01-02');
      const index = findCoordinateIndex(coords, value, undefined, 'time', {
        units: 'days since 1970-01-01'
      });
      expect(index).toBe(1);
    });

    test('should convert date strings to numeric for CF time', () => {
      const coords = [0, 1, 2, 3, 4]; // days since epoch
      const index = findCoordinateIndex(coords, '1970-01-02', undefined, 'time', {
        units: 'days since 1970-01-01'
      });
      expect(index).toBe(1);
    });
  });
});

describe('findIndexFallback', () => {
  test('should find exact match', () => {
    const coords = [10, 20, 30, 40];
    const index = findIndexFallback(coords, 30);
    expect(index).toBe(2);
  });

  test('should throw on no exact match', () => {
    const coords = [10, 20, 30, 40];
    expect(() => findIndexFallback(coords, 25))
      .toThrow(/not found/);
  });

  test('should delegate to nearest method', () => {
    const coords = [10, 20, 30, 40];
    const index = findIndexFallback(coords, 23, 'nearest');
    expect(index).toBe(1); // 20 is closer
  });

  test('should delegate to ffill method', () => {
    const coords = [10, 20, 30, 40];
    const index = findIndexFallback(coords, 25, 'ffill');
    expect(index).toBe(1); // 20 is last value <= 25
  });

  test('should delegate to bfill method', () => {
    const coords = [10, 20, 30, 40];
    const index = findIndexFallback(coords, 25, 'bfill');
    expect(index).toBe(2); // 30 is first value >= 25
  });
});

describe('findNearestIndex', () => {
  test('should find nearest value', () => {
    const coords = [10, 20, 30, 40, 50];
    expect(findNearestIndex(coords, 23)).toBe(1); // 20
    expect(findNearestIndex(coords, 27)).toBe(2); // 30
    expect(findNearestIndex(coords, 25)).toBe(1); // 20 (equidistant, picks first)
  });

  test('should work with negative coordinates', () => {
    const coords = [-50, -30, -10, 10, 30, 50];
    expect(findNearestIndex(coords, -15)).toBe(2); // -10
    expect(findNearestIndex(coords, 5)).toBe(3); // 10 is closer (distance 5 vs 15)
  });

  test('should respect tolerance', () => {
    const coords = [10, 20, 30, 40];
    expect(() => findNearestIndex(coords, 100, 5))
      .toThrow(/within tolerance/);
  });

  test('should accept value within tolerance', () => {
    const coords = [10, 20, 30, 40];
    const index = findNearestIndex(coords, 22, 5);
    expect(index).toBe(1);
  });

  test('should throw error for non-numeric coordinates', () => {
    const coords = ['a', 'b', 'c'] as any;
    expect(() => findNearestIndex(coords, 'b' as any))
      .toThrow(/numeric coordinates/);
  });

  test('should use binary search for large sorted arrays', () => {
    // Create large sorted array (> 20 elements)
    const coords = Array.from({ length: 100 }, (_, i) => i * 10);
    const index = findNearestIndex(coords, 555, undefined, 'x');
    expect(index).toBe(55); // 550 is nearest (distance 5 vs 560 distance 5, but 550 comes first)
  });
});

describe('findFfillIndex', () => {
  test('should find last value <= target', () => {
    const coords = [10, 20, 30, 40, 50];
    expect(findFfillIndex(coords, 25)).toBe(1); // 20
    expect(findFfillIndex(coords, 30)).toBe(2); // 30 (exact match)
    expect(findFfillIndex(coords, 35)).toBe(2); // 30
  });

  test('should throw if no value <= target', () => {
    const coords = [10, 20, 30];
    expect(() => findFfillIndex(coords, 5))
      .toThrow(/No coordinate <=/);
  });

  test('should respect tolerance', () => {
    const coords = [10, 20, 30, 40];
    expect(() => findFfillIndex(coords, 100, 5))
      .toThrow(/within tolerance/);
  });

  test('should use binary search for large sorted arrays', () => {
    const coords = Array.from({ length: 100 }, (_, i) => i * 10);
    const index = findFfillIndex(coords, 555, undefined, 'x');
    expect(index).toBe(55); // 550
  });

  test('should throw error for non-numeric coordinates', () => {
    const coords = ['a', 'b', 'c'] as any;
    expect(() => findFfillIndex(coords, 'b' as any))
      .toThrow(/numeric coordinates/);
  });
});

describe('findBfillIndex', () => {
  test('should find first value >= target', () => {
    const coords = [10, 20, 30, 40, 50];
    expect(findBfillIndex(coords, 25)).toBe(2); // 30
    expect(findBfillIndex(coords, 30)).toBe(2); // 30 (exact match)
    expect(findBfillIndex(coords, 15)).toBe(1); // 20
  });

  test('should throw if no value >= target', () => {
    const coords = [10, 20, 30];
    expect(() => findBfillIndex(coords, 50))
      .toThrow(/No coordinate >=/);
  });

  test('should respect tolerance', () => {
    const coords = [10, 20, 30, 40];
    expect(() => findBfillIndex(coords, 5, 2))
      .toThrow(/within tolerance/);
  });

  test('should use binary search for large sorted arrays', () => {
    const coords = Array.from({ length: 100 }, (_, i) => i * 10);
    const index = findBfillIndex(coords, 555, undefined, 'x');
    expect(index).toBe(56); // 560
  });

  test('should throw error for non-numeric coordinates', () => {
    const coords = ['a', 'b', 'c'] as any;
    expect(() => findBfillIndex(coords, 'b' as any))
      .toThrow(/numeric coordinates/);
  });
});

describe('isCoordsSorted', () => {
  test('should detect ascending order', () => {
    expect(isCoordsSorted([1, 2, 3, 4, 5])).toBe(true);
    expect(isCoordsSorted([10, 20, 30, 40])).toBe(true);
  });

  test('should detect descending order', () => {
    expect(isCoordsSorted([5, 4, 3, 2, 1])).toBe(true);
    expect(isCoordsSorted([40, 30, 20, 10])).toBe(true);
  });

  test('should detect unsorted', () => {
    expect(isCoordsSorted([1, 3, 2, 4])).toBe(false);
    expect(isCoordsSorted([10, 20, 15, 30])).toBe(false);
  });

  test('should handle single element', () => {
    expect(isCoordsSorted([42])).toBe(true);
  });

  test('should handle two elements', () => {
    expect(isCoordsSorted([1, 2])).toBe(true);
    expect(isCoordsSorted([2, 1])).toBe(true);
  });

  test('should handle duplicates as sorted', () => {
    expect(isCoordsSorted([1, 2, 2, 3])).toBe(true);
    expect(isCoordsSorted([3, 2, 2, 1])).toBe(true);
  });
});

describe('binarySearchNearest', () => {
  test('should find nearest in ascending array', () => {
    const coords = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    expect(binarySearchNearest(coords, 55, true)).toBe(4); // 50 (distance 5 vs 60 distance 5, picks first)
    expect(binarySearchNearest(coords, 25, true)).toBe(1); // 20
    expect(binarySearchNearest(coords, 95, true)).toBe(8); // 90 (distance 5 vs 100 distance 5, picks first)
  });

  test('should find exact match in ascending array', () => {
    const coords = [10, 20, 30, 40, 50];
    expect(binarySearchNearest(coords, 30, true)).toBe(2);
  });

  test('should find nearest in descending array', () => {
    const coords = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10];
    expect(binarySearchNearest(coords, 55, false)).toBe(4); // 60 (distance 5 vs 50 distance 5, picks first which is 60)
    expect(binarySearchNearest(coords, 75, false)).toBe(2); // 80
  });

  test('should handle edge cases', () => {
    const coords = [1, 2, 3, 4, 5];
    expect(binarySearchNearest(coords, 0, true)).toBe(0); // 1
    expect(binarySearchNearest(coords, 100, true)).toBe(4); // 5
  });
});

describe('binarySearchFfill', () => {
  test('should find largest value <= target in ascending', () => {
    const coords = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    expect(binarySearchFfill(coords, 55, true)).toBe(4); // 50
    expect(binarySearchFfill(coords, 60, true)).toBe(5); // 60 (exact)
    expect(binarySearchFfill(coords, 25, true)).toBe(1); // 20
  });

  test('should return -1 if no value <= target', () => {
    const coords = [10, 20, 30, 40, 50];
    expect(binarySearchFfill(coords, 5, true)).toBe(-1);
  });

  test('should find in descending array', () => {
    const coords = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10];
    expect(binarySearchFfill(coords, 55, false)).toBe(5); // 50
    expect(binarySearchFfill(coords, 15, false)).toBe(9); // 10
  });

  test('should handle edge at beginning', () => {
    const coords = [10, 20, 30, 40];
    expect(binarySearchFfill(coords, 10, true)).toBe(0);
  });

  test('should handle edge at end', () => {
    const coords = [10, 20, 30, 40];
    expect(binarySearchFfill(coords, 100, true)).toBe(3);
  });
});

describe('binarySearchBfill', () => {
  test('should find smallest value >= target in ascending', () => {
    const coords = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    expect(binarySearchBfill(coords, 55, true)).toBe(5); // 60
    expect(binarySearchBfill(coords, 60, true)).toBe(5); // 60 (exact)
    expect(binarySearchBfill(coords, 25, true)).toBe(2); // 30
  });

  test('should return -1 if no value >= target', () => {
    const coords = [10, 20, 30, 40, 50];
    expect(binarySearchBfill(coords, 100, true)).toBe(-1);
  });

  test('should find in descending array', () => {
    const coords = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10];
    expect(binarySearchBfill(coords, 55, false)).toBe(4); // 60
    expect(binarySearchBfill(coords, 85, false)).toBe(1); // 90
  });

  test('should handle edge at beginning', () => {
    const coords = [10, 20, 30, 40];
    expect(binarySearchBfill(coords, 5, true)).toBe(0);
  });

  test('should handle edge at end', () => {
    const coords = [10, 20, 30, 40];
    expect(binarySearchBfill(coords, 40, true)).toBe(3);
  });
});

describe('performance characteristics', () => {
  test('should handle large evenly-spaced array efficiently', () => {
    // 10000 elements - should use O(1) arithmetic
    const coords = Array.from({ length: 10000 }, (_, i) => i);
    const start = Date.now();
    const index = findCoordinateIndex(coords, 5555, undefined, 'x');
    const elapsed = Date.now() - start;

    expect(index).toBe(5555);
    expect(elapsed).toBeLessThan(10); // Should be nearly instant
  });

  test('should handle large sorted array with binary search', () => {
    // Non-evenly-spaced but sorted - should use O(log n) binary search
    const coords = Array.from({ length: 10000 }, (_, i) => i * i);
    const start = Date.now();
    const index = findNearestIndex(coords, 5000, undefined, 'x');
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(10); // Binary search should be fast
  });
});
