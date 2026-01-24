// Single-threaded Web Worker for Lepton WASM decompression
// Used as fallback for browsers that don't support SharedArrayBuffer/multi-threading (e.g., WebKit)

let leptonModule = null;

async function doInit() {
    // Use relative import - works regardless of deployment path
    try {
        leptonModule = await import('./lepton_rust_st.js');
    } catch (e) {
        throw new Error('Failed to import lepton_rust_st.js: ' + e.message);
    }

    try {
        await leptonModule.default();  // init WASM
    } catch (e) {
        throw new Error('Failed to init lepton WASM: ' + e.message);
    }

    console.log('[Lepton] Single-threaded mode initialized');
}

const initPromise = doInit();

initPromise.then(() => {
    // Signal to page that worker is fully ready
    self.postMessage({ type: 'ready', numThreads: 1 });
}).catch((error) => {
    console.error('[Lepton ST] Initialization failed:', error);
    self.postMessage({ type: 'init-error', message: error.message || 'Unknown error' });
});

self.onmessage = async (e) => {
    const { id, data } = e.data;

    try {
        await initPromise;
        // Single-threaded version doesn't take num_threads parameter
        const decoded = leptonModule.decode_lepton(data);
        // Copy result - WASM memory can't be transferred directly
        const result = new Uint8Array(decoded);
        self.postMessage({ id, result }, [result.buffer]);
    } catch (error) {
        self.postMessage({ id, error: error.message });
    }
};
