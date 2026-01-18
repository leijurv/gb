// Service Worker for gb/webshare
// Handles encrypted downloads with streaming decryption and decompression

// Relay logs to page via BroadcastChannel for easier debugging (especially Firefox)
const logChannel = new BroadcastChannel('sw-logs');
function swLog(...args) {
    console.log(...args);
    try { logChannel.postMessage(args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ')); } catch(e) {}
}

// Load dependencies with SRI verification
// (fetch + verify + eval since we're in a module context)
const dependencies = [
    {
        url: 'https://cdn.jsdelivr.net/npm/client-zip@2.5.0/worker.js',
        integrity: 'sha256-2Tam4C0dWa/3we/LNzQL/w+H2On+vSMJkf19sgS1lZA='
    },
    {
        url: 'https://cdn.jsdelivr.net/npm/js-sha256@0.11.0/src/sha256.min.js',
        integrity: 'sha384-QjbMdgv/hWELlDRbhj6tXsKlzXhrlrSIGNqgdQVvxYQpo+vA+4jOWramMq56bPSg'
    }
];

// zstd-emscripten WASM library for streaming decompression (from kig/zstd-emscripten)
// Uses official libzstd compiled to WASM - no window size limitations
// Zstd frame format is stable: old decoders can decompress new encoder output
let ZStd = null;
let zstdLoading = null;  // Promise for lazy loading

const zstdJs = {
    url: 'https://leijurv.github.io/gb/webshare/zstd/zstd.js',
    integrity: 'sha384-78z6VYTAbieb/uF4afnBJAjPzgKIxv0MgekAbfoEKEDZIVwGWxfKPToG5VKCU3TR'
};
const zstdWasm = {
    url: 'https://leijurv.github.io/gb/webshare/zstd/zstd.wasm',
    integrity: 'sha384-Wu2F+8X9C4paG5l9Zp/TMb/hJhIX0rB9bIsxZmdqixBwdIymHYS3RQognZWsAk0I'
};

async function loadZstdWasm() {
    let code, wasmBinary;
    if (typeof ZSTD_IS_BUNDLED !== 'undefined' && ZSTD_IS_BUNDLED) {
        code = atob(ZSTD_JS_BASE64);
        const base64 = ZSTD_WASM_BASE64;
        if (typeof Uint8Array.fromBase64 === 'function') {
            wasmBinary = Uint8Array.fromBase64(base64).buffer;
        } else {
            wasmBinary = Uint8Array.from(atob(base64), c => c.charCodeAt(0)).buffer;
        }
    } else {
        // Fetch both files in parallel with integrity verification
        const [jsResponse, wasmResponse] = await Promise.all([
            fetch(zstdJs.url, { integrity: zstdJs.integrity }),
            fetch(zstdWasm.url, { integrity: zstdWasm.integrity })
        ]);
        if (!jsResponse.ok) throw new Error(`Failed to fetch ${zstdJs.url}: ${jsResponse.status}`);
        if (!wasmResponse.ok) throw new Error(`Failed to fetch ${zstdWasm.url}: ${wasmResponse.status}`);

        code = await jsResponse.text()
        wasmBinary = await wasmResponse.arrayBuffer()
    }

    eval(code);
    // Pass wasmBinary to the factory so emscripten doesn't fetch it
    ZStd = await ZSTD({ wasmBinary: wasmBinary });
}

// Lazy load zstd only when needed
async function ensureZstdLoaded() {
    if (ZStd) return;
    if (!zstdLoading) {
        zstdLoading = loadZstdWasm();
    }
    await zstdLoading;
}

// Lepton JPEG codec - Rust WASM with rayon multi-threading
// Main page hosts the worker since SWs can't spawn Workers
// SW communicates with page via MessageChannel
let leptonRequestId = 0;

async function decodeLeptonViaPage(data) {
    // Find a client (page) to handle the decode
    const clients = await self.clients.matchAll({ type: 'window' });
    if (clients.length === 0) {
        throw new Error('No page available for lepton decode');
    }

    const id = ++leptonRequestId;
    const client = clients[0];

    return new Promise((resolve, reject) => {
        const { port1, port2 } = new MessageChannel();

        port1.onmessage = (e) => {
            if (e.data.error) {
                reject(new Error(e.data.error));
            } else {
                resolve(e.data.result);
            }
        };

        client.postMessage({ type: 'lepton-decode', id, data }, [port2, data.buffer]);
    });
}

// No-op since page handles initialization
async function ensureLeptonLoaded() {}

async function loadDependencies() {
    const tasks = [];
    for (const dep of dependencies) {
        tasks.push((async () => {
            const response = await fetch(dep.url, { integrity: dep.integrity });
            if (!response.ok) throw new Error(`Failed to fetch ${dep.url}: ${response.status}`);
            const code = await response.text();
            // Safe to eval since integrity was verified by fetch
            // self.eval does eval in the global scope which is necessary for client-zip which exports functions with just `var`
            self.eval(code);
        })());
    }
    await Promise.all(tasks);
}

const depsLoaded = loadDependencies();

const cachedJsonByPassword = new Map();
async function requestParamsFromPage(password) {
    return requestParamsFromPage0(password, 'need-params', data => cachedJsonByPassword.set(password, data));
}

// Ask page for params - broadcast to all clients, first response wins
async function requestParamsFromPage0(id, type, callback) {
    const clients = await self.clients.matchAll({ type: 'window' });
    if (clients.length === 0) return null;

    return new Promise((resolve) => {
        let resolved = false;
        const timeout = setTimeout(() => {
            if (!resolved) { resolved = true; resolve(null); }
        }, 2000);

        for (const client of clients) {
            const { port1, port2 } = new MessageChannel();
            port1.onmessage = (e) => {
                if (!resolved && e.data) {
                    resolved = true;
                    clearTimeout(timeout);
                    callback(e.data);
                    resolve(e.data);
                }
            };
            client.postMessage({ type: type, id }, [port2]);
        }
    });
}


// Parse expiry from S3 presigned URL (X-Amz-Date + X-Amz-Expires)
function parseS3Expiry(url) {
    try {
        const urlObj = new URL(url);
        const amzDate = urlObj.searchParams.get('X-Amz-Date'); // format: 20260102T090602Z
        const amzExpires = urlObj.searchParams.get('X-Amz-Expires'); // seconds
        if (amzDate && amzExpires) {
            const year = amzDate.slice(0, 4);
            const month = amzDate.slice(4, 6);
            const day = amzDate.slice(6, 8);
            const hour = amzDate.slice(9, 11);
            const min = amzDate.slice(11, 13);
            const sec = amzDate.slice(13, 15);
            const created = new Date(`${year}-${month}-${day}T${hour}:${min}:${sec}Z`);
            return created.getTime() + parseInt(amzExpires) * 1000;
        }
    } catch (e) {}
    return null;
}

function isUrlExpired(url) {
    const expiresAt = parseS3Expiry(url);
    return expiresAt && expiresAt < (Date.now() + 5000);
}

async function queryAndParseParameters(shortUrlKey) {
    const response = await fetch(`/share-data/${shortUrlKey}.json`);
    if (!response.ok) {
        if (response.status === 410) {
            // Share has expired
            throw new Error(`Share has expired`);
        }
        throw new Error(`Failed to fetch fresh URL: ${response.status}`);
    }
    const json = await response.json();
    return json.map(p => {
        return parseParameters(p);
    });
}

function parseParameters(p, shortUrlKey) {
    let out = {
        compression: p.cmp,
        key: p.key.toLowerCase(),
        length: parseInt(p.length),
        offset: parseInt(p.offset),
        size: parseInt(p.size),
        name: p.name,
        filename: p.name,
        path: p.path,
        sha256: p.sha256,
        url: p.url,
        index: p.index
    };
    if (shortUrlKey) {
        out.shortUrlKey = shortUrlKey;
    }
    return out;
}




// Get presigned URL, fetching fresh one only if current is expired or missing
// Returns null if share has expired (and notifies clients)
async function getPresignedUrl(p) {
    // Check if URL exists and is still valid (with a few second buffer)
    if (p.url && !isUrlExpired(p.url)) {
        return p.url;
    }

    if (!p.shortUrlKey) {
        return null; // the url is expired/missing and we can't fetch a new one
    }

    // Fetch fresh URL - use index suffix for individual files in a folder
    const urlKey = p.index !== undefined ? `${p.shortUrlKey}-${p.index}` : p.shortUrlKey;
    const response = await fetch(`/share-data/${urlKey}.json`);
    if (!response.ok) {
        if (response.status === 410) {
            // Share has expired
            try {
                const errorData = await response.json();
                if (errorData.error === 'expired' && errorData.expires_at) {
                    notifyClients({ type: 'expired', expires_at: errorData.expires_at, id });
                    return null;
                }
            } catch (e) {}
        }
        throw new Error(`Failed to fetch fresh URL: ${response.status}`);
    }
    const data = await response.json();
    // Find matching file by sha256 and update URL
    for (let d of data) {
        if (d.sha256 === p.sha256) {
            p.url = d.url;
            return p.url;
        }
    }
    throw new Error('response json doesnt have our sha256');
}

async function notifyClients(message) {
    const clients = await self.clients.matchAll({ type: 'window' });
    for (const client of clients) {
        client.postMessage(message);
    }
}

function bytesToBase64Url(bytes) {
    const base64 = btoa(String.fromCharCode(...bytes));
    return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function hexToBytes(hex) {
    return new Uint8Array(hex.match(/.{2}/g).map(b => parseInt(b, 16)));
}

function getMimeType(filename) {
    const ext = filename.split('.').pop().toLowerCase();
    const mimeTypes = {
        'mp4': 'video/mp4',
        'mkv': 'video/x-matroska',
        'webm': 'video/webm',
        'mov': 'video/quicktime',
        'avi': 'video/x-msvideo',
        'png': 'image/png',
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
        'gif': 'image/gif',
        'webp': 'image/webp',
        'mp3': 'audio/mpeg',
        'wav': 'audio/wav',
        'ogg': 'audio/ogg',
        'flac': 'audio/flac',
        'm4a': 'audio/mp4',
        'aac': 'audio/aac',
        'pdf': 'application/pdf',
    };
    return mimeTypes[ext] || 'application/octet-stream';
}

// AES-CTR decryption transform that handles arbitrary byte offsets.
// Each chunk is fully processed immediately by padding both ends:
// prepend zeros to align to block boundary, append zeros to round up.
function createDecryptTransform(keyBytes, startOffset) {
    let cryptoKey = null;
    let currentPos = startOffset;

    function makeCounter(bytePos) {
        const counter = new Uint8Array(16);
        let n = Math.floor(bytePos / 16);
        for (let i = 15; i >= 0 && n > 0; i--) {
            counter[i] = n & 0xff;
            n = Math.floor(n / 256);
        }
        return counter;
    }

    return new TransformStream({
        async start() {
            cryptoKey = await crypto.subtle.importKey(
                'raw', keyBytes, { name: 'AES-CTR' }, false, ['encrypt']
            );
        },
        async transform(chunk, controller) {
            if (chunk.length === 0) return;

            // Pad to align: [prefix zeros to block boundary][chunk][suffix zeros to round up]
            const prefixLen = currentPos % 16;
            const paddedLen = Math.ceil((prefixLen + chunk.length) / 16) * 16;
            const toDecrypt = new Uint8Array(paddedLen);
            toDecrypt.set(chunk, prefixLen);

            const counter = makeCounter(currentPos - prefixLen);

            // CTR mode is symmetric: encrypt(ciphertext) = decrypt(ciphertext)
            const decrypted = await crypto.subtle.encrypt(
                { name: 'AES-CTR', counter, length: 128 },
                cryptoKey,
                toDecrypt
            );

            // Extract only the bytes corresponding to the original chunk
            controller.enqueue(new Uint8Array(decrypted).slice(prefixLen, prefixLen + chunk.length));
            currentPos += chunk.length;
        }
    });
}


function createZstdDecompressTransform() {
    /* don't await */ ensureZstdLoaded();  // Start loading in parallel with data download

    // WASM streaming decompression using zstd-emscripten
    // Buffer size for input/output chunks (128KB each)
    const BUFFER_SIZE = 131072;

    let dctx = null;
    let buffersPtr = null;
    let buffIn, buffInPos, buffOut, buffOutPos;

    return new TransformStream({
        async start() {
            await ensureZstdLoaded();
            // Allocate WASM memory for buffers
            // Layout: [inPos(4)] [input(BUFFER_SIZE-4)] [outPos(4)] [output(BUFFER_SIZE-4)]
            buffersPtr = ZStd._malloc(BUFFER_SIZE * 2);
            buffIn = new Uint8Array(ZStd.HEAPU8.buffer, buffersPtr + 4, BUFFER_SIZE - 4);
            buffInPos = new Int32Array(ZStd.HEAPU8.buffer, buffersPtr, 1);
            buffOut = new Uint8Array(ZStd.HEAPU8.buffer, buffersPtr + BUFFER_SIZE + 4, BUFFER_SIZE - 4);
            buffOutPos = new Int32Array(ZStd.HEAPU8.buffer, buffersPtr + BUFFER_SIZE, 1);

            // Create decompression stream context
            dctx = ZStd._ZSTD_createDStream();
        },
        transform(chunk, controller) {
            // Process input in BUFFER_SIZE chunks
            for (let i = 0; i < chunk.byteLength; i += buffIn.byteLength) {
                const block = chunk.slice(i, i + buffIn.byteLength);
                buffIn.set(block);
                buffInPos[0] = 0;

                // Decompress until all input consumed
                while (buffInPos[0] < block.byteLength) {
                    buffOutPos[0] = 0;
                    const ret = ZStd._ZSTD_decompressStream_simpleArgs(
                        dctx,
                        buffOut.byteOffset, buffOut.byteLength, buffOutPos.byteOffset,
                        buffIn.byteOffset, block.byteLength, buffInPos.byteOffset
                    );
                    if (ZStd._ZSTD_isError(ret)) {
                        const errPtr = ZStd._ZSTD_getErrorName(ret);
                        const errMsg = ZStd.UTF8ToString(errPtr);
                        throw new Error('ZSTD decompression error: ' + errMsg);
                    }
                    if (buffOutPos[0] > 0) {
                        // Copy output and enqueue (must copy since buffer will be reused)
                        controller.enqueue(new Uint8Array(buffOut.slice(0, buffOutPos[0])));
                    }
                }
            }
        },
        flush(controller) {
            // Flush any remaining data (should be none for complete streams)
            buffInPos[0] = 0;
            buffOutPos[0] = 0;
            const ret = ZStd._ZSTD_decompressStream_simpleArgs(
                dctx,
                buffOut.byteOffset, buffOut.byteLength, buffOutPos.byteOffset,
                buffIn.byteOffset, 0, buffInPos.byteOffset
            );
            if (!ZStd._ZSTD_isError(ret) && buffOutPos[0] > 0) {
                controller.enqueue(new Uint8Array(buffOut.slice(0, buffOutPos[0])));
            }

            // Cleanup WASM resources
            if (dctx) {
                ZStd._ZSTD_freeDStream(dctx);
                dctx = null;
            }
            if (buffersPtr) {
                ZStd._free(buffersPtr);
                buffersPtr = null;
            }
        }
    });
}

function createLeptonDecompressTransform() {
    /* don't await */ ensureLeptonLoaded();  // Start loading in parallel with data download

    // Lepton is not a streaming decoder - must buffer entire input
    const chunks = [];

    return new TransformStream({
        transform(chunk, controller) {
            chunks.push(chunk);
        },
        async flush(controller) {
            // Load lepton on-demand
            await ensureLeptonLoaded();

            // Combine all chunks
            const totalLength = chunks.reduce((sum, c) => sum + c.length, 0);
            const input = new Uint8Array(totalLength);
            let offset = 0;
            for (const chunk of chunks) {
                input.set(chunk, offset);
                offset += chunk.length;
            }

            // Decode lepton to JPEG via page's worker (multi-threaded)
            const result = await decodeLeptonViaPage(input);
            controller.enqueue(new Uint8Array(result));
        }
    });
}

// Verifies SHA-256 integrity of stream, throws on mismatch
function createIntegrityTransform(id, expectedSha256, filename) {
    const hasher = sha256.create();
    return new TransformStream({
        transform(chunk, controller) {
            hasher.update(chunk);
            controller.enqueue(chunk);
        },
        async flush() {
            const hashBytes = new Uint8Array(hasher.arrayBuffer());
            const actualSha256 = bytesToBase64Url(hashBytes);
            if (actualSha256 !== expectedSha256) {
                const message = `Integrity check failed for "${filename}": expected ${expectedSha256}, got ${actualSha256}`;
                await notifyClients({ type: 'error', message, id });
                throw new Error(message);
            }
        }
    });
}

function createProgressTransform(id, size) {
    let totalBytes = 0;
    let lastProgressTime = 0;

    return new TransformStream({
        transform(chunk, controller) {
            totalBytes += chunk.length;
            controller.enqueue(chunk);

            const now = Date.now();
            if (now - lastProgressTime > 100) {
                lastProgressTime = now;
                notifyClients({ type: 'progress', bytes: totalBytes, size: size, id: id });
            }
        },
        flush() {
            notifyClients({ type: 'complete', bytes: totalBytes, id: id });
        }
    });
}

async function fetchS3Range(url, start, end) {
    const expectedLength = end - start + 1;
    const response = await fetch(url, {
        headers: { 'Range': `bytes=${start}-${end}` }
    });
    if (response.status !== 206) {
        throw new Error('S3 fetch failed: expected 206, got ' + response.status);
    }
    const contentLength = parseInt(response.headers.get('Content-Length'));
    if (contentLength !== expectedLength) {
        throw new Error(`S3 Content-Length mismatch: expected ${expectedLength}, got ${contentLength}`);
    }
    // we can't verify Content-Range entirely because we don't know the total size of the blob, and verifying Content-Length alone is (more than) enough to be confident that S3 is behaving
    return response;
}


function parseRangeHeader(rangeHeader, totalSize) {
    // Parse "bytes=start-end" or "bytes=start-" or "bytes=-suffix"
    const match = /^bytes=(\d*)-(\d*)$/.exec(rangeHeader);
    if (!match) return null;

    let start, end;
    if (match[1] === '' && match[2] !== '') {
        // bytes=-500 means last 500 bytes
        const suffix = parseInt(match[2]);
        start = Math.max(0, totalSize - suffix);
        end = totalSize - 1;
    } else if (match[1] !== '' && match[2] === '') {
        // bytes=500- means from 500 to end
        start = parseInt(match[1]);
        end = totalSize - 1;
    } else {
        start = parseInt(match[1]);
        end = parseInt(match[2]);
    }

    if (start > end || start >= totalSize) return null;
    end = Math.min(end, totalSize - 1);

    return { start, end };
}

// Add COOP/COEP headers to enable SharedArrayBuffer (coi-serviceworker style)
function addCoiHeaders(response) {
    if (response.status === 0) {
        return response;  // Opaque response, can't modify
    }

    const newHeaders = new Headers(response.headers);
    newHeaders.set('Cross-Origin-Opener-Policy', 'same-origin');
    newHeaders.set('Cross-Origin-Embedder-Policy', 'require-corp');

    return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: newHeaders
    });
}

// Proxy lepton files from GitHub Pages with COOP/COEP headers for SharedArrayBuffer support
// Cache responses in memory for SW lifetime (these files never change)
const LEPTON_GITHUB_BASE = 'https://leijurv.github.io/gb/webshare/lepton';
const leptonCache = new Map();  // path -> { body: ArrayBuffer|string, contentType: string }

// SRI hashes for lepton files (verified on fetch from GitHub Pages)
const leptonIntegrity = {
    '/lepton_rust.js': 'sha384-pPPidBbrirqvbJ4vVQ24oOM3nCkO2B3L6BQg9yInBWsXB6aT7ffLASo7YGMwObfN',
    '/lepton_rust.wasm': 'sha384-fLOeSO2Kkh5ngSpJR2f4ZRFgNIlcVEihXYjCFz+qOQ7TmRWGCtZN0PJSQ+DkRrpe',
    '/lepton-worker.js': 'sha384-7yfbqiu+j4M4LyS0G0E/wvcqZxvorhkXsLMPrVEmpVE3xtm3KfIt4zW7oTUVjbx1',
    '/snippets/wasm-bindgen-rayon-38edf6e439f6d70d/src/workerHelpers.no-bundler.js': 'sha384-hUWHQYRixf7a9bhQ/Ga09aCXo/QEhlkN2B9PaK598KNdt8gZpRvb8Pizjbi9H6Um'
};

function getLeptonContentType(path) {
    if (path.endsWith('.js')) return 'application/javascript';
    if (path.endsWith('.wasm')) return 'application/wasm';
    return 'application/octet-stream';
}

async function proxyLeptonFile(pathname) {
    // Extract the path after /lepton/ (handles both /lepton/... and /gb/webshare/lepton/...)
    const leptonPath = pathname.substring(pathname.indexOf('/lepton/') + '/lepton'.length);
    const contentType = getLeptonContentType(leptonPath);
    const isText = contentType.includes('javascript');

    // Check cache first
    const cached = leptonCache.get(leptonPath);
    if (cached) {
        const headers = new Headers({
            'Content-Type': contentType,
            'Cross-Origin-Opener-Policy': 'same-origin',
            'Cross-Origin-Embedder-Policy': 'require-corp'
        });
        // Clone the body for each response (can't reuse same ArrayBuffer)
        const body = isText ? cached.body : cached.body.slice(0);
        return new Response(body, { status: 200, headers });
    }

    // Verify integrity hash is known for this file
    const integrity = leptonIntegrity[leptonPath];
    if (!integrity) {
        return new Response(`Unknown lepton file: ${leptonPath}`, { status: 404 });
    }

    // Fetch from GitHub Pages with SRI verification
    const githubUrl = LEPTON_GITHUB_BASE + leptonPath;
    const response = await fetch(githubUrl, { integrity });
    if (!response.ok) {
        return new Response(`Failed to fetch ${leptonPath}`, { status: response.status });
    }

    // Cache the response body (integrity already verified by fetch)
    const body = isText ? await response.text() : await response.arrayBuffer();
    leptonCache.set(leptonPath, { body });

    // Return response with COOP/COEP headers
    const headers = new Headers({
        'Content-Type': contentType,
        'Cross-Origin-Opener-Policy': 'same-origin',
        'Cross-Origin-Embedder-Policy': 'require-corp'
    });
    return new Response(isText ? body : body.slice(0), { status: 200, headers });
}

async function uncompressedRangedGet(range, params, notifyId) {
    await depsLoaded;
    const keyBytes = hexToBytes(params.key);

    // Get presigned URL (fetches fresh one if expired)
    const s3Url = await getPresignedUrl(params);
    if (!s3Url) {
        notifyClients({ type: 'error', message: 'This share link has expired', id: notifyId });
        return new Response('Share link expired', { status: 410 });
    }

    // Calculate the byte range in the encrypted blob
    const blobStart = params.offset + range.start;
    const blobEnd = params.offset + range.end;
    const rangeLength = range.end - range.start + 1;

    let s3Response;
    try {
        s3Response = await fetchS3Range(s3Url, blobStart, blobEnd);
    } catch (e) {
        console.log(e);
        return new Response(e.message, { status: 502 });
    }
    const stream = s3Response.body
        .pipeThrough(createDecryptTransform(keyBytes, blobStart));

    const headers = new Headers({
        'Content-Type': getMimeType(params.filename),
        'Content-Range': `bytes ${range.start}-${range.end}/${params.size}`,
        'Content-Length': String(rangeLength),
        'Accept-Ranges': 'bytes'
    });

    return new Response(stream, { status: 206, headers });
}

async function fullFileGet(params, canSeek, isMediaPlayback, clientEvents, notifyId) {
    await depsLoaded;
    const keyBytes = hexToBytes(params.key);

    const s3Url = await getPresignedUrl(params);
    if (!s3Url) {
        notifyClients({ type: 'error', message: 'This share link has expired', id: notifyId });
        return new Response('Share link expired', { status: 410 });
    }

    let s3Response;
    try {
        s3Response = await fetchS3Range(s3Url, params.offset, params.offset + params.length - 1);
    } catch (e) {
        console.log(e);
        return new Response(e.message, { status: 502 });
    }
    let stream = s3Response.body
        .pipeThrough(createDecryptTransform(keyBytes, params.offset));

    if (params.compression === 'zstd') {
        stream = stream.pipeThrough(createZstdDecompressTransform());
    } else if (params.compression === 'lepton') {
        stream = stream.pipeThrough(createLeptonDecompressTransform());
    }

    stream = stream.pipeThrough(createIntegrityTransform(notifyId, params.sha256, params.filename));

    // Only report progress for interactive downloads
    if (!isMediaPlayback && clientEvents) {
        stream = stream.pipeThrough(createProgressTransform(notifyId, params.size));
    }

    const headers = new Headers({
        'Content-Type': getMimeType(params.filename),
        'Content-Length': String(params.size)
    });

    // Only set Content-Disposition for downloads, not media playback
    if (!isMediaPlayback) {
        // RFC 5987 encoding for non-ASCII filenames
        const encodedFilename = encodeURIComponent(params.filename).replace(/'/g, '%27');
        headers.set('Content-Disposition', `attachment; filename*=UTF-8''${encodedFilename}`);
    }

    if (canSeek) {
        headers.set('Accept-Ranges', 'bytes');
    }

    return new Response(stream, { headers });
}

// lazily creates responses so we can update the urls as they are needed
async function* fileResponses(paramArray, id) {
    try {
        for (let p of paramArray) {
            yield fullFileGet(p, false, false, false, id);
        }
    } catch(e) {
        // downloadZip seems to be swallowing errors so print here them if any happen
        console.log('exception: ', e);
        notifyClients({ type: 'error', message: e.message, id: id });
        throw e;
    }
}

function fileMetadata(paramArray) {
    let out = [];
    for (let p of paramArray) {
        out.push({name: p.filename, size: p.size});
    }
    return out;
}

self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);

    // Ping endpoint to verify SW is intercepting fetches
    if (url.pathname.endsWith('/gb-sw-ping')) {
        event.respondWith(new Response('pong', { status: 200 }));
        return;
    }

    // Proxy lepton files with COOP/COEP headers for multi-threading support
    if (url.pathname.includes('/lepton/') && url.origin === location.origin) {
        event.respondWith(proxyLeptonFile(url.pathname));
        return;
    }

    // For non-download requests, add COOP/COEP headers to enable SharedArrayBuffer
    if (!url.pathname.endsWith('/gb-download')) {
        // Only intercept same-origin requests (not CDN resources, etc.)
        if (url.origin === location.origin) {
            event.respondWith(
                fetch(event.request).then(addCoiHeaders)
            );
        }
        return;
    }

    const parameterizedUrlParams = ['name', 'url', 'key', 'offset', 'length', 'size', 'sha256', 'cmp'];
    const password = url.searchParams.get('password');
    const hash = url.searchParams.get('hash');
    event.respondWith((async () => {
        let paramsArray;
        if (password) {
            let cachedParams = cachedJsonByPassword.get(password);
            if (cachedParams && !isUrlExpired(cachedParams.url)) {
                paramsArray = cachedParams;
            } else {
                let pageParams = await requestParamsFromPage(password);
                if (!pageParams) {
                    return new Response('Error: unknown zip file id', { status: 404 });
                }
                paramsArray = pageParams;
            }
            if (hash) {
                paramsArray = paramsArray.filter(p => p.sha256 === hash);
            }
        } else {
            let params = {};
            for (key of parameterizedUrlParams) {
                params[key] = url.searchParams.get(key);
            }
            paramsArray = [parseParameters(params)];
        }
        if (paramsArray.length === 0) {
            throw new Error('uh oh stinky paramsArray.length should not be 0');
        }


        if (paramsArray.length === 1) {
            const isMediaPlayback = url.searchParams.get('media') === 'true';
            const rangeHeader = event.request.headers.get('Range');

            // Check cache first, then ask page if SW was restarted
            // TODO: if the cache has an expired url get one from the client
            const p = paramsArray[0];
            const notifyId = password ? password : p.sha256;

            const canSeek = (p.compression === '' || p.compression === 'none');
            // Handle Range request for uncompressed files
            if (canSeek && rangeHeader) {
                const range = parseRangeHeader(rangeHeader, p.size);
                if (range) {
                    const response = await uncompressedRangedGet(range, p, notifyId);
                    return addCoiHeaders(response);
                }
            }

            // Full file request (or compressed file, or invalid range)
            const response = await fullFileGet(p, canSeek, isMediaPlayback, true, notifyId);
            return addCoiHeaders(response);
        } else if (password) {
            let downloadFilename = url.searchParams.get('download-filename');
            let array;
            let cachedParams = cachedJsonByPassword.get(password);
            if (cachedParams) {
                array = cachedParams;
            } else {
                let pageParams = await requestParamsFromPage(password);
                if (!pageParams) {
                    return new Response('Error: unknown zip file id', { status: 404 });
                }
                array = pageParams;
            }

            let responses = fileResponses(array, password);
            let zipResponse = downloadZip(responses, {metadata: fileMetadata(array)});
            const headers = new Headers(zipResponse.headers);
            headers.set('Content-Disposition', `attachment; filename="${downloadFilename}"`);
            const length = parseInt(headers.get("Content-Length"))
            const body = zipResponse.body.pipeThrough(createProgressTransform(password, length));
            return new Response(body, {
                status: zipResponse.status,
                statusText: zipResponse.statusText,
                headers: headers
            });
        } else {
            console.log('didnt provide zip-id or hash');
            return new Response('Error: didnt provide zip-id or hash parameter', { status: 400 });
        }
    })());
});

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));
