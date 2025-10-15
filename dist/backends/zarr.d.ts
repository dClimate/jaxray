import { Dataset } from "../Dataset";
export interface ZarrStore {
    get(key: string): Promise<Uint8Array | undefined>;
    has?(key: string): Promise<boolean>;
    listMetadataKeys?(): string[];
}
export interface ZarrMetadata {
    [key: string]: {
        shape: number[];
        dimension_names?: string[];
        attributes?: {
            [key: string]: any;
        };
        chunk_grid?: {
            configuration: {
                chunk_shape: number[];
            };
        };
        node_type?: "array" | "group";
        data_type?: string;
    };
}
type OpenOptions = {
    group?: string;
    consolidated?: boolean;
};
export declare class ZarrBackend {
    /**
     * Open a Zarr store as a Dataset
     * @param store - A ZarrStore implementation (e.g., ShardedStore, S3Store, LocalStore)
     * @param options - Options including group path
     */
    static open(store: ZarrStore, options?: OpenOptions): Promise<Dataset>;
}
export {};
//# sourceMappingURL=zarr.d.ts.map