// Simple test server for e2e tests
// Serves static files from parent directory and test fixtures

import { createServer } from 'http';
import { readFile, readdir } from 'fs/promises';
import { join, extname } from 'path';
import { fileURLToPath } from 'url';
import { createHmac } from 'crypto';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const parentDir = join(__dirname, '..');
const fixturesDir = join(__dirname, 'fixtures');

// Load password-based share metadata from fixtures
async function loadShareMeta() {
  const files = await readdir(fixturesDir);
  const metaFiles = files.filter(f => f.endsWith('.meta.json'));
  const shares = new Map();

  for (const file of metaFiles) {
    const content = await readFile(join(fixturesDir, file), 'utf-8');
    const meta = JSON.parse(content);
    shares.set(meta.password, {
      ...meta,
      binFile: file.replace('.meta.json', '.bin')
    });
  }

  return shares;
}

// Derive content key from password (matches generate.js and worker.js)
function deriveShareContentKey(masterKey, password) {
  const hmac = createHmac('sha256', Buffer.from(masterKey, 'hex'));
  hmac.update('content:' + password);
  return hmac.digest().slice(0, 16).toString('hex');
}

let shareMetaCache = null;

const PORT = process.env.PORT || 3000;

const mimeTypes = {
  '.html': 'text/html',
  '.js': 'application/javascript',
  '.json': 'application/json',
  '.wasm': 'application/wasm',
  '.css': 'text/css',
  '.txt': 'text/plain',
  '.bin': 'application/octet-stream',
};

function getMimeType(filePath) {
  return mimeTypes[extname(filePath)] || 'application/octet-stream';
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  let pathname = url.pathname;

  console.log(`[server] ${req.method} ${pathname}`);

  // Handle Range requests for S3 mock
  const rangeHeader = req.headers.range;

  try {
    let filePath;

    // Handle /share-data/{password}.json for password-based shares
    if (pathname.startsWith('/share-data/')) {
      let password = pathname.slice('/share-data/'.length).replace(/\.json$/, '');

      // Load share metadata (cached)
      if (!shareMetaCache) {
        shareMetaCache = await loadShareMeta();
      }

      const meta = shareMetaCache.get(password);
      if (!meta) {
        console.log(`[server] 404: share not found for password "${password}"`);
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'Share not found' }));
        return;
      }

      // Return presigned-style URL and content key (mimics worker.js response)
      const response = {
        url: `http://localhost:${PORT}/fixtures/${meta.binFile}`,
        key: deriveShareContentKey(meta.masterKey, password)
      };

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(response));
      return;
    }

    // Route /gb/webshare/* to parent directory (mimics GitHub Pages structure)
    if (pathname.startsWith('/gb/webshare/')) {
      const subPath = pathname.slice('/gb/webshare/'.length);
      if (subPath === '' || subPath === 'index.html') {
        filePath = join(parentDir, 'index.html');
      } else {
        filePath = join(parentDir, subPath);
      }
    }
    // Route /fixtures/* to fixtures directory (test data)
    else if (pathname.startsWith('/fixtures/')) {
      filePath = join(fixturesDir, pathname.slice('/fixtures/'.length));
    }
    // Route lepton files regardless of base path (for password-based URLs like /testpassword/lepton/...)
    else if (pathname.includes('/lepton/')) {
      const leptonPath = pathname.slice(pathname.indexOf('/lepton/') + 1);
      filePath = join(parentDir, leptonPath);
    }
    // Root redirects to /gb/webshare/
    else if (pathname === '/') {
      res.writeHead(302, { Location: '/gb/webshare/' });
      res.end();
      return;
    }
    // Password-based share URLs: /{password}/ or /{password}/{filename}
    // Serve index.html for these (the app parses the password from the path)
    else if (/^\/[^/]+\/?/.test(pathname) && !pathname.includes('.')) {
      filePath = join(parentDir, 'index.html');
    }
    else {
      res.writeHead(404);
      res.end('Not found');
      return;
    }

    const content = await readFile(filePath);
    const contentType = getMimeType(filePath);

    // Handle Range requests (for S3 mock)
    if (rangeHeader) {
      const match = /bytes=(\d+)-(\d+)?/.exec(rangeHeader);
      if (match) {
        const start = parseInt(match[1]);
        const end = match[2] ? parseInt(match[2]) : content.length - 1;
        const chunk = content.slice(start, end + 1);

        res.writeHead(206, {
          'Content-Type': contentType,
          'Content-Range': `bytes ${start}-${end}/${content.length}`,
          'Content-Length': chunk.length,
          'Accept-Ranges': 'bytes',
        });
        res.end(chunk);
        return;
      }
    }

    const headers = {
      'Content-Type': contentType,
      'Content-Length': content.length,
    };

    // Add Service-Worker-Allowed header for service worker (matches worker.js)
    if (filePath.endsWith('share-sw.js')) {
      headers['Service-Worker-Allowed'] = '/';
    }

    res.writeHead(200, headers);
    res.end(content);

  } catch (err) {
    if (err.code === 'ENOENT') {
      console.log(`[server] 404: ${pathname}`);
      res.writeHead(404);
      res.end('Not found');
    } else {
      console.error(`[server] Error:`, err);
      res.writeHead(500);
      res.end('Internal server error');
    }
  }
});

server.listen(PORT, () => {
  console.log(`Test server running at http://localhost:${PORT}`);
});
