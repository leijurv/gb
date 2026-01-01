importScripts('https://cdn.jsdelivr.net/npm/fzstd@0.1.1/umd/index.min.js');
importScripts('https://cdn.jsdelivr.net/npm/js-sha256@0.11.0/src/sha256.min.js');

const downloadParamsMap = new Map();

self.addEventListener('message', (event) => {
    if (event.data.type === 'download') {
        const params = event.data.params;
        downloadParamsMap.set(params.sha256, params);
    }
});

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
    let decompressor;
    let outputChunks = [];

    return new TransformStream({
        start() {
            decompressor = new fzstd.Decompress((chunk) => {
                outputChunks.push(chunk);
            });
        },
        transform(chunk, controller) {
            outputChunks = [];
            decompressor.push(chunk);
            for (const out of outputChunks) {
                controller.enqueue(out);
            }
        },
        flush(controller) {
            outputChunks = [];
            decompressor.push(new Uint8Array(0), true);
            for (const out of outputChunks) {
                controller.enqueue(out);
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

    if (!url.pathname.endsWith('/gb-download')) return;

    const id = url.searchParams.get('id');
    const p = id ? downloadParamsMap.get(id) : null;
    if (!p) return;

    const isMediaPlayback = url.searchParams.get('media') === 'true';
    const canSeek = (p.compression === '' || p.compression === 'none');
    const rangeHeader = event.request.headers.get('Range');

    // Handle Range request for uncompressed files
    if (canSeek && rangeHeader) {
        const range = parseRangeHeader(rangeHeader, p.size);
        if (range) {
            event.respondWith((async () => {
                const keyBytes = hexToBytes(p.key);

                // Calculate the byte range in the encrypted blob
                const blobStart = p.offset + range.start;
                const blobEnd = p.offset + range.end;
                const rangeLength = range.end - range.start + 1;

                // Align to AES block boundary
                const alignedStart = Math.floor(blobStart / 16) * 16;
                const skipBytes = blobStart - alignedStart;
                const fetchEnd = blobEnd;

                const s3Range = 'bytes=' + alignedStart + '-' + fetchEnd;
                const s3Response = await fetch(p.url, {
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
            })());
            return;
        }
    }

    // Full file request (or compressed file)
    event.respondWith((async () => {
        const keyBytes = hexToBytes(p.key);
        const offset = p.offset;
        const length = p.length;
        const alignedOffset = Math.floor(offset / 16) * 16;
        const remainingSeek = offset % 16;
        const fetchLength = length + remainingSeek;

        const s3RangeHeader = 'bytes=' + alignedOffset + '-' + (alignedOffset + fetchLength - 1);
        const s3Response = await fetch(p.url, {
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
            headers.set('Content-Disposition', 'attachment; filename="' + p.filename.replace(/"/g, '\\"') + '"');
        }

        if (canSeek) {
            headers.set('Accept-Ranges', 'bytes');
        }

        return new Response(stream, { headers });
    })());
});

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));
