// Relay logs to page via BroadcastChannel for easier debugging (especially Firefox)
const logChannel = new BroadcastChannel('sw-logs');
function swLog(...args) {
    console.log(...args);
    try { logChannel.postMessage(args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ')); } catch(e) {}
}

// Load dependencies with SRI verification
// (importScripts doesn't support integrity, so we fetch + verify + eval via blob URL)
const dependencies = [
    {
        url: 'https://cdn.jsdelivr.net/npm/js-sha256@0.11.0/src/sha256.min.js',
        integrity: 'sha384-QjbMdgv/hWELlDRbhj6tXsKlzXhrlrSIGNqgdQVvxYQpo+vA+4jOWramMq56bPSg'
    }
];

// zstd-emscripten WASM library for streaming decompression (from kig/zstd-emscripten)
// Uses official libzstd compiled to WASM - no window size limitations
// Zstd frame format is stable: old decoders can decompress new encoder output
let ZStd = null;

const zstdJs = {
    url: 'zstd/zstd.js',  // relative to service worker
    integrity: 'sha384-78z6VYTAbieb/uF4afnBJAjPzgKIxv0MgekAbfoEKEDZIVwGWxfKPToG5VKCU3TR'
};
const zstdWasm = {
    url: 'zstd/zstd.wasm',
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

async function loadDependencies() {
    const tasks = [];
    for (const dep of dependencies) {
        tasks.push((async () => {
            const response = await fetch(dep.url, { integrity: dep.integrity });
            if (!response.ok) throw new Error(`Failed to fetch ${dep.url}: ${response.status}`);
            const code = await response.text();
            // Safe to eval since integrity was verified by fetch
            eval(code);
        })());
    }
    tasks.push(loadZstdWasm());
    await Promise.all(tasks);
}

const depsLoaded = loadDependencies();

const downloadParamsMap = new Map();

// Ask page for params - broadcast to all clients, first response wins
async function requestParamsFromPage(id) {
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
                    downloadParamsMap.set(e.data.sha256, e.data);
                    resolve(e.data);
                }
            };
            client.postMessage({ type: 'need-params', id }, [port2]);
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

// Get presigned URL, fetching fresh one only if current is expired
async function getPresignedUrl(p) {
    if (!p.shortUrlKey) {
        return p.url;
    }

    // Check if cached URL is still valid (with 5 second buffer)
    const expiresAt = parseS3Expiry(p.url);
    if (expiresAt && expiresAt > Date.now() + 5000) {
        return p.url;
    }

    // Fetch fresh URL
    const response = await fetch(`/share-data/${p.shortUrlKey}.json`);
    if (!response.ok) {
        throw new Error(`Failed to fetch fresh URL: ${response.status}`);
    }
    const data = await response.json();

    // Update cached URL for future requests
    p.url = data.url;

    return data.url;
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

function calcIVAndSeek(offset) {
    const blockNum = Math.floor(offset / 16);
    const iv = new Uint8Array(16);
    let n = blockNum;
    for (let i = 15; i >= 0 && n > 0; i--) {
        iv[i] = n & 0xff;
        n = Math.floor(n / 256);
    }
    return { iv, remainingSeek: offset % 16 };
}

function createDecryptTransform(keyBytes, startOffset) {
    const { iv, remainingSeek } = calcIVAndSeek(startOffset);
    let cryptoKey = null;
    let counter = new Uint8Array(iv);
    let skipBytes = remainingSeek;
    let pendingBytes = new Uint8Array(0);

    function incrementCounter(ctr, times) {
        for (let t = 0; t < times; t++) {
            for (let i = 15; i >= 0; i--) {
                ctr[i]++;
                if (ctr[i] !== 0) break;
            }
        }
    }

    return new TransformStream({
        async start() {
            cryptoKey = await crypto.subtle.importKey(
                'raw', keyBytes, { name: 'AES-CTR' }, false, ['encrypt']
            );
        },
        async transform(chunk, controller) {
            const combined = new Uint8Array(pendingBytes.length + chunk.length);
            combined.set(pendingBytes, 0);
            combined.set(chunk, pendingBytes.length);

            const fullBlocks = Math.floor(combined.length / 16);
            const toProcess = fullBlocks * 16;

            if (toProcess > 0) {
                const toDecrypt = combined.slice(0, toProcess);
                // CTR mode is symmetric: encrypt(ciphertext) = decrypt(ciphertext)
                // Web Crypto only exposes encrypt() for AES-CTR, which works for decryption too
                const decrypted = await crypto.subtle.encrypt(
                    { name: 'AES-CTR', counter: counter, length: 64 },
                    cryptoKey,
                    toDecrypt
                );

                incrementCounter(counter, fullBlocks);

                let result = new Uint8Array(decrypted);

                if (skipBytes > 0) {
                    result = result.slice(skipBytes);
                    skipBytes = 0;
                }

                if (result.length > 0) {
                    controller.enqueue(result);
                }
            }

            pendingBytes = combined.slice(toProcess);
        },
        async flush(controller) {
            if (pendingBytes.length > 0) {
                const padded = new Uint8Array(16);
                padded.set(pendingBytes, 0);
                // CTR mode is symmetric (see comment above)
                const decrypted = await crypto.subtle.encrypt(
                    { name: 'AES-CTR', counter: counter, length: 64 },
                    cryptoKey,
                    padded
                );
                let result = new Uint8Array(decrypted).slice(0, pendingBytes.length);

                if (skipBytes > 0) {
                    result = result.slice(skipBytes);
                }

                if (result.length > 0) {
                    controller.enqueue(result);
                }
            }
        }
    });
}

function createLengthLimitTransform(maxLength) {
    let written = 0;
    return new TransformStream({
        transform(chunk, controller) {
            const remaining = maxLength - written;
            if (remaining <= 0) return;
            if (chunk.length <= remaining) {
                controller.enqueue(chunk);
                written += chunk.length;
            } else {
                controller.enqueue(chunk.slice(0, remaining));
                written += remaining;
            }
        }
    });
}

function createZstdDecompressTransform() {
    // WASM streaming decompression using zstd-emscripten
    // Buffer size for input/output chunks (128KB each)
    const BUFFER_SIZE = 131072;

    let dctx = null;
    let buffersPtr = null;
    let buffIn, buffInPos, buffOut, buffOutPos;

    return new TransformStream({
        start() {
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

function createHashAndProgressTransform(expectedSha256) {
    const hasher = sha256.create();
    let totalBytes = 0;
    let lastProgressTime = 0;

    return new TransformStream({
        transform(chunk, controller) {
            hasher.update(chunk);
            totalBytes += chunk.length;
            controller.enqueue(chunk);

            const now = Date.now();
            if (now - lastProgressTime > 100) {
                lastProgressTime = now;
                notifyClients({ type: 'progress', bytes: totalBytes, id: expectedSha256 });
            }
        },
        flush() {
            const hashBytes = new Uint8Array(hasher.arrayBuffer());
            const hashBase64Url = bytesToBase64Url(hashBytes);
            notifyClients({ type: 'complete', bytes: totalBytes, sha256: hashBase64Url, id: expectedSha256 });
        }
    });
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

self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);

    // Ping endpoint to verify SW is intercepting fetches
    if (url.pathname.endsWith('/gb-sw-ping')) {
        event.respondWith(new Response('pong', { status: 200 }));
        return;
    }

    if (!url.pathname.endsWith('/gb-download')) return;

    const id = url.searchParams.get('id');
    const isMediaPlayback = url.searchParams.get('media') === 'true';
    const rangeHeader = event.request.headers.get('Range');

    event.respondWith((async () => {
        // Check cache first, then ask page if SW was restarted
        let p = id ? downloadParamsMap.get(id) : null;
        if (!p) {
            p = await requestParamsFromPage(id);
        }
        if (!p) {
            swLog('[SW] Error: unknown hash', id);
            return new Response('Error: unknown hash', { status: 404 });
        }

        const canSeek = (p.compression === '' || p.compression === 'none');

        // Handle Range request for uncompressed files
        if (canSeek && rangeHeader) {
            const range = parseRangeHeader(rangeHeader, p.size);
            if (range) {
                await depsLoaded;
                const keyBytes = hexToBytes(p.key);

                // Get presigned URL (fetches fresh one if expired)
                const s3Url = await getPresignedUrl(p);

                // Calculate the byte range in the encrypted blob
                const blobStart = p.offset + range.start;
                const blobEnd = p.offset + range.end;
                const rangeLength = range.end - range.start + 1;

                // Align to AES block boundary
                const alignedStart = Math.floor(blobStart / 16) * 16;
                const fetchEnd = blobEnd;

                const s3Range = 'bytes=' + alignedStart + '-' + fetchEnd;
                const s3Response = await fetch(s3Url, {
                    headers: { 'Range': s3Range }
                });

                if (!s3Response.ok && s3Response.status !== 206) {
                    return new Response('S3 fetch failed: ' + s3Response.status, { status: 502 });
                }

                const stream = s3Response.body
                    .pipeThrough(createDecryptTransform(keyBytes, blobStart))
                    .pipeThrough(createLengthLimitTransform(rangeLength));

                const headers = new Headers({
                    'Content-Type': getMimeType(p.filename),
                    'Content-Range': `bytes ${range.start}-${range.end}/${p.size}`,
                    'Content-Length': String(rangeLength),
                    'Accept-Ranges': 'bytes'
                });

                return new Response(stream, { status: 206, headers });
            }
        }

        // Full file request (or compressed file, or invalid range)
        await depsLoaded;
        const keyBytes = hexToBytes(p.key);

        // Get presigned URL (fetches fresh one if expired)
        const s3Url = await getPresignedUrl(p);

        const offset = p.offset;
        const length = p.length;
        const alignedOffset = Math.floor(offset / 16) * 16;
        const remainingSeek = offset % 16;
        const fetchLength = length + remainingSeek;

        const s3RangeHeader = 'bytes=' + alignedOffset + '-' + (alignedOffset + fetchLength - 1);
        const s3Response = await fetch(s3Url, {
            headers: { 'Range': s3RangeHeader }
        });

        if (!s3Response.ok && s3Response.status !== 206) {
            return new Response('S3 fetch failed: ' + s3Response.status, { status: 502 });
        }

        let stream = s3Response.body
            .pipeThrough(createDecryptTransform(keyBytes, offset))
            .pipeThrough(createLengthLimitTransform(length));

        if (p.compression === 'zstd') {
            stream = stream.pipeThrough(createZstdDecompressTransform());
        }

        // Only track progress/hash for actual downloads, not media playback
        if (!isMediaPlayback) {
            stream = stream.pipeThrough(createHashAndProgressTransform(p.sha256));
        }

        const headers = new Headers({
            'Content-Type': getMimeType(p.filename),
            'Content-Length': String(p.size)
        });

        // Only set Content-Disposition for downloads, not media playback
        if (!isMediaPlayback) {
            // RFC 5987 encoding for non-ASCII filenames
            const encodedFilename = encodeURIComponent(p.filename).replace(/'/g, '%27');
            headers.set('Content-Disposition', `attachment; filename*=UTF-8''${encodedFilename}`);
        }

        if (canSeek) {
            headers.set('Accept-Ranges', 'bytes');
        }

        return new Response(stream, { headers });
    })());
});

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));
