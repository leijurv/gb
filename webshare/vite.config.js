import { defineConfig } from 'vite';
import { viteStaticCopy } from 'vite-plugin-static-copy'
import { readFileSync } from 'fs';
import { createHash } from 'crypto';
import { transform } from 'esbuild';

const swHash = createHash('sha256').update(readFileSync('share-sw.js')).digest('hex').slice(0, 8);
const SERVED_JS_FILES = ['share-sw.js'];

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
        for (let filename of SERVED_JS_FILES) {
          const chunk = bundle[filename];
          const dotTxt = `${filename}.txt`
          chunk.fileName = dotTxt;
          bundle[dotTxt] = chunk;
          delete bundle[filename];
        }
      }
    },
    {
      name: 'minify-except-sw',
      async renderChunk(code, chunk) {
        if (chunk.fileName !== 'share-sw.js') {
          const result = await transform(code, { minify: true });
          return result.code;
        }
        return null;
      }
    },
    {
      name: 'minify-except-sw',
      async renderChunk(code, chunk) {
        if (chunk.fileName !== 'share-sw.js') {
          const result = await transform(code, { minify: true });
          return result.code;
        }
        return null;
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
        'worker': './worker.js',
      },
      output: {
        entryFileNames: '[name].js',  // Outputs as worker.js and share-sw.js
        chunkFileNames: '[name].js',
      },
      external: ['./index.html', './share-sw.js.txt' ]
    },
    outDir: 'dist',
    emptyOutDir: true,
    minify: false
  }
});
