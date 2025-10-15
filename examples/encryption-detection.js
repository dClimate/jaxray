/**
 * Example: Encryption Detection in Jaxray
 *
 * This example demonstrates how to detect encryption in Zarr datasets
 */

import { Dataset, DataArray, ZARR_ENCODINGS } from '../dist/index.js';

// Example 1: Detect encryption in a manually created dataset
console.log('Example 1: Manually created dataset with encryption');
console.log('='.repeat(50));

const encryptedData = new DataArray([1, 2, 3, 4, 5], {
  dims: ['time'],
  coords: { time: [0, 1, 2, 3, 4] },
  attrs: {
    codecs: [
      { name: 'bytes', configuration: { endian: 'little' } },
      { name: 'xchacha20poly1305', configuration: { key: 'secret' } }
    ]
  }
});

const dataset1 = new Dataset({ temperature: encryptedData });

console.log('Dataset created with encrypted data');
console.log('Detecting encryption...');
const isEncrypted = dataset1.detectEncryption();
console.log(`Encryption detected: ${isEncrypted}`);
console.log(`Dataset.isEncrypted: ${dataset1.isEncrypted}`);
console.log();

// Example 2: Dataset without encryption
console.log('Example 2: Dataset without encryption');
console.log('='.repeat(50));

const plainData = new DataArray([10, 20, 30, 40, 50], {
  dims: ['time'],
  coords: { time: [0, 1, 2, 3, 4] },
  attrs: {
    codecs: [
      { name: 'bytes', configuration: { endian: 'little' } },
      { name: 'gzip', configuration: { level: 5 } }
    ]
  }
});

const dataset2 = new Dataset({ temperature: plainData });

console.log('Dataset created with non-encrypted data');
console.log('Detecting encryption...');
const isPlain = dataset2.detectEncryption();
console.log(`Encryption detected: ${isPlain}`);
console.log(`Dataset.isEncrypted: ${dataset2.isEncrypted}`);
console.log();

// Example 3: Check which encoding algorithms are considered encryption
console.log('Example 3: Encryption algorithms');
console.log('='.repeat(50));
console.log('Algorithms considered as encryption:');
ZARR_ENCODINGS.forEach(codec => {
  console.log(`  - ${codec}`);
});
console.log();

// Example 4: Mixed dataset (some encrypted, some not)
console.log('Example 4: Mixed dataset');
console.log('='.repeat(50));

const plainTemp = new DataArray([10, 20, 30], {
  dims: ['time'],
  coords: { time: [0, 1, 2] },
  attrs: {
    codecs: [{ name: 'gzip' }]
  }
});

const encryptedPressure = new DataArray([1000, 1010, 1020], {
  dims: ['time'],
  coords: { time: [0, 1, 2] },
  attrs: {
    codecs: [
      { name: 'bytes' },
      { name: 'xchacha20poly1305' }
    ]
  }
});

const mixedDataset = new Dataset({
  temperature: plainTemp,
  pressure: encryptedPressure
});

console.log('Dataset created with both encrypted and non-encrypted variables');
console.log('Variables:');
console.log('  - temperature: gzip compression (not encrypted)');
console.log('  - pressure: xchacha20poly1305 encryption');
console.log('Detecting encryption...');
console.log(`Encryption detected: ${mixedDataset.detectEncryption()}`);
console.log(`Dataset.isEncrypted: ${mixedDataset.isEncrypted}`);
console.log('(If ANY variable is encrypted, the dataset is marked as encrypted)');
console.log();

console.log('All examples completed!');
