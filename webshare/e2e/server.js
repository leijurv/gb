// Simple test server for e2e tests
// Serves static files from parent directory and test fixtures

import { createServer } from 'http';
import { readFile } from 'fs/promises';
import { join, extname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const parentDir = join(__dirname, '..');
const fixturesDir = join(__dirname, 'fixtures');

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
    // Root redirects to /gb/webshare/
    else if (pathname === '/') {
      res.writeHead(302, { Location: '/gb/webshare/' });
      res.end();
      return;
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

    res.writeHead(200, {
      'Content-Type': contentType,
      'Content-Length': content.length,
    });
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

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Test server running at http://localhost:${PORT}`);
});
