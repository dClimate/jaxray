/**
 * Fetches up-to-date IPFS CIDs from the dClimate STAC API.
 *
 * This avoids hardcoding CIDs that go stale when datasets are republished.
 * Results are cached for the lifetime of the process so the API is hit at most once.
 */

const STAC_ITEMS_URL = 'https://api.stac.dclimate.net/collections/ecmwf_era5/items?limit=50';

interface StacItem {
  id: string;
  assets: Record<string, { href: string }>;
}

interface StacResponse {
  features: StacItem[];
}

let cached: Promise<StacResponse> | null = null;

function fetchItems(): Promise<StacResponse> {
  if (!cached) {
    cached = fetch(STAC_ITEMS_URL).then(r => {
      if (!r.ok) throw new Error(`STAC API returned ${r.status}`);
      return r.json() as Promise<StacResponse>;
    });
  }
  return cached;
}

function extractCid(item: StacItem): string {
  const zarrAsset = item.assets['zarr'] ?? item.assets['data'] ?? Object.values(item.assets)[0];
  if (!zarrAsset) throw new Error(`No asset found for item ${item.id}`);
  // href looks like "ipfs://bafyr4i..." or just the CID
  const href = zarrAsset.href;
  const match = href.match(/(bafyr[a-z0-9]+)/i);
  if (!match) throw new Error(`Could not extract CID from href "${href}" for item ${item.id}`);
  return match[1];
}

function findItem(items: StacItem[], idSubstring: string): StacItem {
  const item = items.find(f => f.id.includes(idSubstring));
  if (!item) throw new Error(`No STAC item matching "${idSubstring}" found`);
  return item;
}

/**
 * Returns the current CID for a finalized ERA5 temperature_2m dataset.
 */
export async function getFinalizedCid(): Promise<string> {
  const { features } = await fetchItems();
  return extractCid(findItem(features, 'temperature_2m-finalized'));
}

/**
 * Returns the current CID for a non-finalized ERA5 temperature_2m dataset.
 */
export async function getNonFinalizedCid(): Promise<string> {
  const { features } = await fetchItems();
  return extractCid(findItem(features, 'temperature_2m-non_finalized'));
}

/**
 * Returns both finalized and non-finalized CIDs for the same variable (temperature_2m).
 */
export async function getTestCids(): Promise<{ finalized: string; nonFinalized: string }> {
  const [finalized, nonFinalized] = await Promise.all([
    getFinalizedCid(),
    getNonFinalizedCid(),
  ]);
  return { finalized, nonFinalized };
}
