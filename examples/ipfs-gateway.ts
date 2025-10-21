/**
 * Example: Opening Zarr datasets stored on IPFS via dClimate gateway
 */

import { CID } from "multiformats/cid";
import { Dataset, HamtStore, ShardedStore, createIpfsElements, openIpfsStore } from "../src";

const GATEWAY_URL = "https://ipfs-gateway.dclimate.net";
const SHARDED_CID = "bafyr4iacuutc5bgmirkfyzn4igi2wys7e42kkn674hx3c4dv4wrgjp2k2u";
const HAMT_CID = "bafyr4ihpa7gtcpdmcuoqvdde6x2dll6maskygcbruplqe525ptfpybdh7i";

async function openShardedDataset() {
    console.log("=== Sharded Zarr via IPFS Gateway ===");
    const ipfsElements = createIpfsElements(GATEWAY_URL);
    const store = await ShardedStore.open(SHARDED_CID, ipfsElements);
    const dataset = await Dataset.open_zarr(store);

    console.log("Variables:", dataset.dataVars);
    console.log("Dimensions:", dataset.dims);

    const firstVar = dataset.dataVars[0];
    if (firstVar) {
        const variable = dataset.getVariable(firstVar);
        console.log(`Sample variable "${firstVar}"`, {
            dims: variable.dims,
            shape: variable.shape,
            attrs: variable.attrs,
        });
    }

    const selected = await dataset.sel({
        latitude: 45,
        longitude: 34,
        time: "1987-05-03T23:00:00",
    });
    console.log("Selection result dims:", selected.dims);
}

async function openHamtDataset() {
    console.log("\n=== HAMT-backed Zarr via IPFS Gateway ===");
    const ipfsElements = createIpfsElements(GATEWAY_URL);
    const rootCid = CID.parse(HAMT_CID);
    const store = new HamtStore(rootCid, ipfsElements);
    const dataset = await Dataset.open_zarr(store);

    console.log("Variables:", dataset.dataVars);
    console.log("Dimensions:", dataset.dims);

    const firstVar = dataset.dataVars[0];
    if (firstVar) {
        const variable = dataset.getVariable(firstVar);
        console.log(`Sample variable "${firstVar}"`, {
            dims: variable.dims,
            shape: variable.shape,
            attrs: variable.attrs,
        });
    }

    const selection = await dataset.sel({
        latitude: { start: 40, stop: 42 },
        longitude: { start: 30, stop: 32 },
        time: "1987-05-03T23:00:00",
    });
    console.log("Selection dims:", selection.dims);
}

async function main() {
    await openShardedDataset();
    await openHamtDataset();
    console.log("\n=== Auto-detected store ===");
    const { type, store } = await openIpfsStore(HAMT_CID);
    console.log(`Auto detected store type: ${type}`);
    const dataset = await Dataset.open_zarr(store);
    console.log("Auto dataset dims:", dataset.dims);
}

main().catch((err) => {
    console.error("Failed to run IPFS gateway example:", err);
    process.exitCode = 1;
});
