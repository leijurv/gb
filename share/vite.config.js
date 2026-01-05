import { defineConfig } from 'vite';
import { viteStaticCopy } from 'vite-plugin-static-copy'
import { readFileSync } from 'fs';

export default defineConfig({
  define: {
    ZSTD_IS_BUNDLED: 'true',
    ZSTD_JS_BASE64: JSON.stringify(readFileSync('zstd/zstd.js').toString('base64')),
    ZSTD_WASM_BASE64: JSON.stringify(readFileSync('zstd/zstd.wasm').toString('base64'))
  },
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
            src: 'share.html',
            dest: '.'
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
      external: ['./share.html', './share-sw.js.txt', './zstd.js.txt', './zstd.wasm.bin']
    },
    outDir: 'dist',
    emptyOutDir: true
  }
});
