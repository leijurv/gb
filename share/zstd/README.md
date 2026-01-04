# zstd.js and zstd.wasm

Decompress-only build of [kig/zstd-emscripten](https://github.com/kig/zstd-emscripten), which wraps Facebook's official [zstd](https://github.com/facebook/zstd) library.

## Why this exists

- **Streaming decompression**: Most zstd WASM libraries don't support true incremental streaming. This one exposes `ZSTD_decompressStream_simpleArgs` for chunk-by-chunk decompression.
- **No window size limits**: Pure JS alternatives like fzstd have a 2^25 byte (32MB) backreference limit. This uses the full libzstd with no such restriction.
- **Service worker compatible**: Built with `ENVIRONMENT='web,worker'` to use fetch instead of XMLHttpRequest.
- **Forward compatible**: Zstd's frame format is stable - older decoders can decompress files from newer encoders.

## Build instructions

### 1. Install Emscripten

```bash
git clone https://github.com/emscripten-core/emsdk.git
cd emsdk
./emsdk install latest
./emsdk activate latest
source ./emsdk_env.sh
```

### 2. Clone zstd-emscripten

```bash
git clone --recursive https://github.com/kig/zstd-emscripten.git
cd zstd-emscripten
```

### 3. Modify exported functions for decompress-only

Edit `exported_functions_decompress.txt`:

```
_ZSTD_createDStream
_ZSTD_freeDStream
_ZSTD_initDStream
_ZSTD_DStreamInSize
_ZSTD_DStreamOutSize
_ZSTD_decompressStream_simpleArgs
_ZSTD_isError
_ZSTD_getErrorName
_malloc
_free
```

### 4. Modify cmake flags

Edit `cmake/emscripten/CMakeLists.txt`, find the `zstd_decompress` target and update `CFLAGS_C`:

```cmake
ADD_EXECUTABLE(zstd_decompress ${EMSCRIPTEN_DIR}/zstd.js.c)
SET(CFLAGS_C "--bind -Oz -s EXPORT_NAME=\"'ZSTD'\" -s TOTAL_MEMORY=16MB -s ALLOW_MEMORY_GROWTH=1 -s MODULARIZE=1 -s DISABLE_EXCEPTION_CATCHING=1 --memory-init-file 0 -s ENVIRONMENT='web,worker' -s EXPORTED_RUNTIME_METHODS=['HEAPU8','HEAP32','UTF8ToString'] -s EXPORTED_FUNCTIONS=@${DECOMPRESS_EXPORTED_FUNCTIONS_FILE}")
SET_TARGET_PROPERTIES(zstd_decompress PROPERTIES LINK_FLAGS ${CFLAGS_C})
TARGET_LINK_LIBRARIES(zstd_decompress libzstd_decompress_static)
```

Key flags:
- `-Oz`: Optimize for size
- `-s ENVIRONMENT='web,worker'`: Use fetch instead of XMLHttpRequest (required for service workers)
- `-s EXPORTED_RUNTIME_METHODS=['HEAPU8','HEAP32','UTF8ToString']`: Export memory views for JS access
- `-s ALLOW_MEMORY_GROWTH=1`: Allow heap to grow for large files

### 5. Build

```bash
mkdir -p build && cd build
emcmake cmake ../cmake/
emmake make zstd_decompress -j4
```

Output files are in `build/emscripten/`:
- `zstd_decompress.js` → rename to `zstd.js`
- `zstd_decompress.wasm` → rename to `zstd.wasm`

## Sizes

| File | Raw | Gzipped |
|------|-----|---------|
| zstd.js | 18KB | 5KB |
| zstd.wasm | 75KB | 27KB |
| **Total** | **93KB** | **32KB** |

Compare to the full build (compress + decompress): 322KB raw, ~97KB gzipped.

## License

- zstd-emscripten JS wrapper: MIT License
- zstd library: BSD 3-Clause License (Facebook)
