// Dedicated Web Worker for multi-threaded Lepton WASM decompression
// This worker can use ES module imports (unlike Service Workers)
// Loaded via SW proxy which adds COOP/COEP headers for SharedArrayBuffer

let leptonModule = null;
let numThreads = 1;

async function doInit() {
    // Use relative import - works regardless of deployment path
    leptonModule = await import('./lepton_rust.js');

    const threadOverride = new URLSearchParams(location.search).get('threads');

    await leptonModule.default();  // init WASM

    // Initialize thread pool
    const defaultThreads = Math.min(8, navigator.hardwareConcurrency || 4);
    numThreads = threadOverride ? parseInt(threadOverride, 10) : defaultThreads;
    await leptonModule.initThreadPool(numThreads);

    console.log(`[Lepton] Multi-threaded (${numThreads} threads)`);
}

const initPromise = doInit();

initPromise.then(() => {
    self.postMessage({ type: 'ready', numThreads });
}).catch((error) => {
    console.error('[Lepton] Initialization failed:', error);
    self.postMessage({ type: 'init-error', message: error.message || 'Unknown error' });
});

self.onmessage = async (e) => {
    const { id, data } = e.data;

    try {
        await initPromise;
        const decoded = leptonModule.decode_lepton(data, numThreads);
        const result = new Uint8Array(decoded);
        self.postMessage({ id, result }, [result.buffer]);
    } catch (error) {
        self.postMessage({ id, error: error.message });
    }
};
