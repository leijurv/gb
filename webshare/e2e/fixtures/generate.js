// Generate test fixtures for e2e tests
// Creates encrypted, zstd-compressed test files

import { execSync } from 'child_process';
import { createCipheriv, randomBytes, createHash } from 'crypto';
import { writeFileSync, readFileSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Test content
const TEST_TEXT = `Hello from the webshare e2e test!
This is a simple text file that has been:
1. Compressed with zstd
2. Encrypted with AES-128-CTR

If you can read this in the browser, the test passed!
`;

function generateFixture(name, content, compression) {
  console.log(`Generating fixture: ${name}`);

  const contentBuffer = Buffer.from(content, 'utf-8');

  // Calculate SHA256 of original content (base64url encoded, no padding)
  const sha256 = createHash('sha256').update(contentBuffer).digest();
  const sha256Base64Url = sha256.toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');

  let compressedBuffer;
  if (compression === 'zstd') {
    // Compress with zstd using CLI
    const tempIn = join(__dirname, `${name}.tmp`);
    const tempOut = join(__dirname, `${name}.tmp.zst`);
    writeFileSync(tempIn, contentBuffer);
    execSync(`zstd -f -q "${tempIn}" -o "${tempOut}"`);
    compressedBuffer = readFileSync(tempOut);
    execSync(`rm "${tempIn}" "${tempOut}"`);
  } else {
    compressedBuffer = contentBuffer;
  }

  // Generate random AES-128 key
  const key = randomBytes(16);
  const keyHex = key.toString('hex');

  // For this test, we put the data at offset 0 in the "blob"
  // In real usage, offset allows multiple files in one S3 object
  const offset = 0;

  // Create AES-CTR cipher with counter starting at block 0
  // Counter is 16 bytes, starts at 0 for offset 0
  const counter = Buffer.alloc(16, 0);
  const cipher = createCipheriv('aes-128-ctr', key, counter);

  // Encrypt the compressed data
  const encrypted = Buffer.concat([
    cipher.update(compressedBuffer),
    cipher.final()
  ]);

  // Write encrypted blob
  const blobPath = join(__dirname, `${name}.bin`);
  writeFileSync(blobPath, encrypted);

  // Generate URL hash parameters
  const params = {
    name: `${name}.txt`,
    url: `http://localhost:3456/fixtures/${name}.bin`,
    key: keyHex,
    offset: offset,
    length: encrypted.length,
    size: contentBuffer.length,
    sha256: sha256Base64Url,
    cmp: compression,
  };

  // Write params as JSON for test to read
  const paramsPath = join(__dirname, `${name}.params.json`);
  writeFileSync(paramsPath, JSON.stringify(params, null, 2));

  // Generate the URL hash string
  const hashString = new URLSearchParams(params).toString();

  console.log(`  Content size: ${contentBuffer.length} bytes`);
  console.log(`  Compressed size: ${compressedBuffer.length} bytes`);
  console.log(`  Encrypted blob: ${blobPath}`);
  console.log(`  SHA256: ${sha256Base64Url}`);
  console.log(`  URL hash: #${hashString}`);
  console.log('');

  return params;
}

// Generate a fixture with intentionally wrong SHA256
function generateBadHashFixture(name, content, compression) {
  const params = generateFixture(name, content, compression);

  // Corrupt the SHA256 hash
  const badHash = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
  params.sha256 = badHash;

  // Overwrite params file with bad hash
  const paramsPath = join(__dirname, `${name}.params.json`);
  writeFileSync(paramsPath, JSON.stringify(params, null, 2));

  console.log(`  Modified SHA256 to invalid: ${badHash}`);
  console.log('');

  return params;
}

// Generate fixtures
console.log('Generating e2e test fixtures...\n');

generateFixture('test-zstd', TEST_TEXT, 'zstd');
generateFixture('test-plain', TEST_TEXT, '');
generateBadHashFixture('test-bad-hash', TEST_TEXT, 'zstd');

console.log('Done!');
