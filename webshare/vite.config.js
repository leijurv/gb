import { defineConfig } from 'vite';
import { viteStaticCopy } from 'vite-plugin-static-copy'
import { readFileSync } from 'fs';
import { createHash } from 'crypto';

const swHash = createHash('sha256').update(readFileSync('share-sw.js')).digest('hex').slice(0, 8);

// Set BUNDLE_ZSTD=true to embed zstd in the service worker (larger bundle, no external fetch)
// Default: fetch zstd from GitHub Pages at runtime
const bundleZstd = process.env.BUNDLE_ZSTD === 'true';

const defines = {
  ZSTD_IS_BUNDLED: bundleZstd ? 'true' : 'false',
  SW_HASH: JSON.stringify(swHash)
};

if (bundleZstd) {
  defines.ZSTD_JS_BASE64 = JSON.stringify(readFileSync('zstd/zstd.js').toString('base64'));
  defines.ZSTD_WASM_BASE64 = JSON.stringify(readFileSync('zstd/zstd.wasm').toString('base64'));
}

export default defineConfig({
  define: defines,
  plugins: [
    {
      name: 'rename-files',
      generateBundle(options, bundle) {
        // Rename share-sw.js to share-sw.js.txt
        const chunk = bundle['share-sw.js'];
        chunk.fileName = 'share-sw.js.txt';
        bundle['share-sw.js.txt'] = chunk;
        delete bundle['share-sw.js'];
      }
    },
    viteStaticCopy({
      targets: [
        {
            src: 'index.html',
            dest: '.',
            transform: (content) => content.replace('/gb/webshare/share-sw.js', `/gb/webshare/share-sw-${swHash}.js`)
        },
      ],
    }),
  ],
  build: {
    lib: {
      entry: 'worker.js',
      formats: ['es'],
      fileName: 'worker'
    },
    rollupOptions: {
      input: {
        'share-sw': './share-sw.js',
        'worker': './worker.js'
      },
      output: {
        entryFileNames: '[name].js',  // Outputs as worker.js and share-sw.js
        chunkFileNames: '[name].js',
      },
      external: ['./index.html', './share-sw.js.txt', './zstd.js.txt', './zstd.wasm.bin']
    },
    outDir: 'dist',
    emptyOutDir: true
  }
});
