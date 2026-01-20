import { test, expect } from '@playwright/test';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixturesDir = join(__dirname, '..', 'fixtures');

// Load fixture params
function loadFixtureParams(name) {
  const paramsPath = join(fixturesDir, `${name}.params.json`);
  return JSON.parse(readFileSync(paramsPath, 'utf-8'));
}

// Build URL hash from params
function buildUrlHash(params) {
  return '#' + new URLSearchParams(params).toString();
}

test.describe('Hash mode file share', () => {
  test('renders zstd-compressed text file content', async ({ page }) => {
    const params = loadFixtureParams('test-zstd');
    const hash = buildUrlHash(params);
    const url = `/gb/webshare/${hash}`;

    // Enable console logging for debugging
    page.on('console', msg => console.log(`[browser] ${msg.type()}: ${msg.text()}`));
    page.on('pageerror', err => console.log(`[browser error] ${err.message}`));

    // Navigate to the share page
    await page.goto(url);

    // Wait for filename to be displayed (indicates params were parsed)
    await expect(page.locator('#filename')).toHaveText('test-zstd.txt', { timeout: 10000 });

    // Wait for the text preview to appear in the media container
    // The app fetches and displays text content in a container div inside #media-container
    const mediaContainer = page.locator('#media-container');
    await expect(mediaContainer).toBeVisible({ timeout: 15000 });

    // The text should be rendered inside the media container
    // Check for the distinctive test content
    await expect(mediaContainer).toContainText('Hello from the webshare e2e test!', { timeout: 15000 });
    await expect(mediaContainer).toContainText('If you can read this in the browser, the test passed!');
  });

  test('renders uncompressed text file content', async ({ page }) => {
    const params = loadFixtureParams('test-plain');
    const hash = buildUrlHash(params);
    const url = `/gb/webshare/${hash}`;

    page.on('console', msg => console.log(`[browser] ${msg.type()}: ${msg.text()}`));

    await page.goto(url);

    await expect(page.locator('#filename')).toHaveText('test-plain.txt', { timeout: 10000 });

    const mediaContainer = page.locator('#media-container');
    await expect(mediaContainer).toBeVisible({ timeout: 15000 });
    await expect(mediaContainer).toContainText('Hello from the webshare e2e test!', { timeout: 15000 });
  });

  test('shows file metadata correctly', async ({ page }) => {
    const params = loadFixtureParams('test-zstd');
    const hash = buildUrlHash(params);

    await page.goto(`/gb/webshare/${hash}`);

    // Check filename
    await expect(page.locator('#filename')).toHaveText('test-zstd.txt');

    // Check metadata shows size and compression
    const meta = page.locator('#meta');
    await expect(meta).toContainText('MB'); // Size in MB format
    await expect(meta).toContainText('compressed'); // Shows compressed size
  });

  test('download button is enabled for valid share', async ({ page }) => {
    const params = loadFixtureParams('test-zstd');
    const hash = buildUrlHash(params);

    await page.goto(`/gb/webshare/${hash}`);

    // Wait for SW to be ready (button should be enabled)
    const downloadBtn = page.locator('#download');
    await expect(downloadBtn).toBeVisible();

    // Button should eventually be enabled (after SW initializes)
    // Note: May be disabled briefly during initialization, mobile emulation can be slower
    await expect(downloadBtn).toBeEnabled({ timeout: 20000 });
  });

  test('shows error and does not display content when SHA256 hash is wrong', async ({ page }) => {
    const params = loadFixtureParams('test-bad-hash');
    const hash = buildUrlHash(params);
    const url = `/gb/webshare/${hash}`;

    page.on('console', msg => console.log(`[browser] ${msg.type()}: ${msg.text()}`));

    await page.goto(url);

    // Wait for filename to be displayed (page loaded)
    await expect(page.locator('#filename')).toHaveText('test-bad-hash.txt', { timeout: 10000 });

    // The status element should show an integrity error
    const status = page.locator('#status');
    await expect(status).toBeVisible({ timeout: 15000 });
    await expect(status).toContainText('Integrity check failed', { timeout: 15000 });

    // The media container should NOT contain the test text (integrity check prevents display)
    const mediaContainer = page.locator('#media-container');
    await expect(mediaContainer).not.toContainText('Hello from the webshare e2e test!');
    await expect(mediaContainer).not.toContainText('If you can read this');
  });
});
