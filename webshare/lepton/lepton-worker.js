// Dedicated Web Worker for multi-threaded Lepton WASM decompression
// This worker can use ES module imports (unlike Service Workers)
// Loaded via SW proxy which adds COOP/COEP headers for SharedArrayBuffer

// Use relative import - works regardless of deployment path
const leptonModule = await import('./lepton_rust.js');

// Check for thread count override via URL param (e.g., ?threads=8)
const threadOverride = new URLSearchParams(location.search).get('threads');

let numThreads = 1;  // Default single-threaded

async function initialize() {
    await leptonModule.default();  // init WASM

    // Try to initialize thread pool - requires cross-origin isolation for SharedArrayBuffer
    // GitHub Pages doesn't provide COOP/COEP headers, so this may fail
    if (typeof SharedArrayBuffer !== 'undefined' && crossOriginIsolated) {
        try {
            const defaultThreads = Math.min(8, navigator.hardwareConcurrency || 4);
            numThreads = threadOverride ? parseInt(threadOverride, 10) : defaultThreads;
            await leptonModule.initThreadPool(numThreads);
        } catch (e) {
            numThreads = 1;
        }
    }
    console.log(`[Lepton] ${numThreads === 1 ? 'Single-threaded' : `Multi-threaded (${numThreads} threads)`}`);
}

const initPromise = initialize().then(() => {
    // Signal to page that worker is fully ready
    self.postMessage({ type: 'ready', numThreads });
});

self.onmessage = async (e) => {
    const { id, data } = e.data;

    try {
        await initPromise;
        const decoded = leptonModule.decode_lepton(data, numThreads);
        // Copy result - WASM memory can't be transferred directly
        const result = new Uint8Array(decoded);
        self.postMessage({ id, result }, [result.buffer]);
    } catch (error) {
        self.postMessage({ id, error: error.message });
    }
};
