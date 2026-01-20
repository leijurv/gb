// Generate test fixtures for e2e tests
// Creates encrypted, optionally compressed test files

import { execSync } from 'child_process';
import { createCipheriv, createHash } from 'crypto';
import { writeFileSync, readFileSync, unlinkSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// --- Helper functions ---

function sha256Base64Url(buffer) {
  const hash = createHash('sha256').update(buffer).digest();
  return hash.toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

// Fixed test key (not random to avoid git churn on regeneration)
const TEST_KEY = Buffer.from('0123456789abcdef0123456789abcdef', 'hex');

function encrypt(buffer) {
  const counter = Buffer.alloc(16, 0);
  const cipher = createCipheriv('aes-128-ctr', TEST_KEY, counter);
  const encrypted = Buffer.concat([cipher.update(buffer), cipher.final()]);
  return { encrypted, keyHex: TEST_KEY.toString('hex') };
}

function compressZstd(buffer, name) {
  const tempIn = join(__dirname, `${name}.tmp`);
  const tempOut = join(__dirname, `${name}.tmp.zst`);
  writeFileSync(tempIn, buffer);
  execSync(`zstd -f -q "${tempIn}" -o "${tempOut}"`);
  const compressed = readFileSync(tempOut);
  unlinkSync(tempIn);
  unlinkSync(tempOut);
  return compressed;
}

function compressLepton(jpegPath, name) {
  const leptonPath = join(__dirname, `${name}.lep`);
  execSync(`lepton "${jpegPath}" "${leptonPath}"`, { stdio: 'pipe' });
  const compressed = readFileSync(leptonPath);
  unlinkSync(leptonPath);
  return compressed;
}

function writeFixture(name, filename, originalSize, sha256, compressedBuffer, compression) {
  const { encrypted, keyHex } = encrypt(compressedBuffer);
  const blobPath = join(__dirname, `${name}.bin`);
  writeFileSync(blobPath, encrypted);

  const params = {
    name: filename,
    url: `http://localhost:3456/fixtures/${name}.bin`,
    key: keyHex,
    offset: 0,
    length: encrypted.length,
    size: originalSize,
    sha256,
    cmp: compression,
  };

  writeFileSync(join(__dirname, `${name}.params.json`), JSON.stringify(params, null, 2));
  return params;
}

function logFixture(name, originalSize, compressedSize, sha256) {
  console.log(`  Original: ${originalSize} bytes, Compressed: ${compressedSize} bytes`);
  console.log(`  SHA256: ${sha256}\n`);
}

// --- Fixture generators ---

function generateTextFixture(name, content, compression) {
  console.log(`Generating: ${name}`);
  const original = Buffer.from(content, 'utf-8');
  const sha256 = sha256Base64Url(original);
  const compressed = compression === 'zstd' ? compressZstd(original, name) : original;
  const params = writeFixture(name, `${name}.txt`, original.length, sha256, compressed, compression);
  logFixture(name, original.length, compressed.length, sha256);
  return params;
}

function generateBadHashFixture(name, content, compression) {
  const params = generateTextFixture(name, content, compression);
  params.sha256 = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
  writeFileSync(join(__dirname, `${name}.params.json`), JSON.stringify(params, null, 2));
  console.log(`  Modified SHA256 to invalid\n`);
  return params;
}

function generateLeptonFixture(name) {
  console.log(`Generating: ${name}`);
  const jpegPath = join(__dirname, `${name}.jpg`);

  // Create a 123x456 gradient test image
  execSync(`python3 -c "
from PIL import Image
img = Image.new('RGB', (123, 456))
for x in range(123):
    for y in range(456):
        img.putpixel((x, y), (x * 2, y // 2, (x + y) % 256))
img.save('${jpegPath}', 'JPEG', quality=85)
"`);

  const original = readFileSync(jpegPath);
  const sha256 = sha256Base64Url(original);
  const compressed = compressLepton(jpegPath, name);
  unlinkSync(jpegPath);

  const params = writeFixture(name, `${name}.jpg`, original.length, sha256, compressed, 'lepton');
  logFixture(name, original.length, compressed.length, sha256);
  return params;
}

// --- Main ---

const TEST_TEXT = `Hello from the webshare e2e test!
This is a simple text file that has been:
1. Compressed with zstd
2. Encrypted with AES-128-CTR

If you can read this in the browser, the test passed!
`;

console.log('Generating e2e test fixtures...\n');

generateTextFixture('test-zstd', TEST_TEXT, 'zstd');
generateTextFixture('test-plain', TEST_TEXT, '');
generateBadHashFixture('test-bad-hash', TEST_TEXT, 'zstd');
generateLeptonFixture('test-lepton');

console.log('Done!');
