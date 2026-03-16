n# AGENTS.md

Instructions for AI coding agents working on the jaxray codebase.

## Project Overview

jaxray is a TypeScript library providing Python xarray-like labeled multi-dimensional arrays for JavaScript. It supports Zarr format, IPFS backends, lazy evaluation, streaming, and XChaCha20-Poly1305 encryption.

## Build & Test

```bash
npm run build          # TypeScript compilation (tsc)
npm test               # vitest run (all tests)
npx vitest run <file>  # Run specific test file
npm run test:coverage  # Coverage report
```

Test timeout is 30 seconds. Some integration tests (IPFS, zarr-ipfs) require network access and may be slow.

## Architecture

```
src/
├── DataArray.ts              # Core labeled array — sel(), isel(), aggregations
├── Dataset.ts                # Container for multiple DataArrays with shared coords
├── types.ts                  # All type definitions (Selection, CoordinateValue, etc.)
├── utils.ts                  # Array helpers (reshape, flatten, getShape, deepClone)
├── core/data-block.ts        # Lazy vs materialized data abstraction
├── utils/
│   ├── coordinate-indexing.ts # Coordinate→index resolution (O(1)/O(log n)/O(n))
│   ├── lazy-selection.ts      # sel() on lazy arrays without materializing
│   ├── data-operations.ts     # Slice/select/aggregate on raw arrays
│   └── rolling-operations.ts  # Rolling window math
├── backends/
│   ├── zarr.ts                # ZarrBackend.open() — reads Zarr v3 stores into Dataset
│   └── ipfs/                  # IPFS gateway, HAMT stores, sharded stores
├── ops/
│   ├── concat.ts              # Dataset concatenation
│   └── where.ts               # Conditional filtering
└── time/cf-time.ts            # CF-conventions time parsing and conversion
```

## Key Patterns

### Lazy evaluation
DataArrays from Zarr are lazy — they store a `lazyLoader` function and `virtualShape` instead of data. Data is only fetched when `sel()` resolves or `.data` is accessed. When modifying selection logic, handle both the lazy path (`performLazySelection` in `lazy-selection.ts`) and the eager path (`_selectData` in `DataArray.ts`).

### Time coordinates
Zarr time coordinates go through this pipeline:
1. Raw numeric CF time values (e.g., `631` = days since epoch) are loaded from Zarr
2. `normalizeCoordinateValues()` in `zarr.ts` converts them to **ISO strings** using `cfTimeToDate().toISOString()`
3. Coordinates are stored as ISO strings in the Dataset/DataArray
4. `findCoordinateIndex()` in `coordinate-indexing.ts` handles lookups, converting Date objects and ISO strings to numeric values for comparison when CF time attributes (`units`, `calendar`) are available

When users pass `Date` objects to `sel()`, the coordinate indexing must match them against ISO string coords. The `_coordAttrs` attribute on DataArrays stores the original CF attributes needed for this conversion.

### Immutability
All operations return new instances. Never mutate existing DataArray/Dataset state.

### Coordinate indexing performance
`findCoordinateIndex()` uses a three-tier strategy:
- **O(1)** arithmetic for evenly-spaced numeric coordinates
- **O(log n)** binary search for sorted coordinates (arrays > 20 elements)
- **O(n)** linear scan fallback

## Common Pitfalls

- **Lazy vs eager paths**: `sel()` has two completely separate code paths depending on whether the DataArray is lazy. Changes to selection logic must be applied to both `performLazySelection()` and `_selectData()`.
- **Dimension dropping**: When a single scalar value is selected (not a range or array), the dimension is dropped. The check in `sel()` must cover all scalar types: `number`, `string`, `bigint`, and `Date`.
- **`_coordAttrs` plumbing**: Time coordinate lookups need CF attributes (`units`, `calendar`) passed through `_coordAttrs` in the DataArray's attrs. If adding a new code path that creates DataArrays, make sure `_coordAttrs` is preserved.
- **ISO string matching**: Coordinates from Zarr time dimensions are ISO strings (e.g., `"2002-01-01T00:00:00.000Z"`). Date objects won't match via `===` or `indexOf` — use `.toISOString()` for comparison or convert both to numeric.

## Testing Conventions

- Tests live in `tests/` mirroring the `src/` structure
- Use `vitest` with `describe`/`test`/`expect`
- Test helper: `tests/helpers/MemoryZarrStore.ts` provides an in-memory Zarr store for unit tests
- When changing error messages, update corresponding `.toThrow()` assertions in tests
- Integration tests (`zarr-ipfs.test.ts`, `ipfs-open-store.test.ts`) hit the network — these may fail or be slow in CI

## Style

- ES Modules (`import`/`export`, `.js` extensions in imports)
- TypeScript strict mode
- No linter or formatter configured — follow existing code style
- Prefer `const` over `let`, avoid `var`
- Use `async`/`await` over raw Promises
