async function beginSignature(env) {
  const amzDate = new Date().toISOString().replace(/[:-]|\.\d{3}/g, '');
  const dateStamp = amzDate.slice(0, 8);
  const scope = `${dateStamp}/${env.S3_REGION}/s3/aws4_request`;

  let endpoint = env.S3_ENDPOINT;
  if (!endpoint.includes(env.S3_REGION)) {
    endpoint = `s3.${env.S3_REGION}.${endpoint}`;
  }

  let key = `AWS4${env.S3_SECRET_KEY}`;
  for (const part of [dateStamp, env.S3_REGION, 's3', 'aws4_request']) {
    key = await hmac(key, part);
  }

  return { key, scope, amzDate, endpoint };
}

async function generatePresignedUrl(env, filePath, signingKey, expiresIn = 30) {
  const { key, scope, amzDate, endpoint } = signingKey;

  const url = new URL(`https://${env.S3_BUCKET}.${endpoint}/${filePath}`);

  url.searchParams.set('X-Amz-Algorithm', 'AWS4-HMAC-SHA256');
  url.searchParams.set('X-Amz-Credential', `${env.S3_ACCESS_KEY}/${scope}`);
  url.searchParams.set('X-Amz-Date', amzDate);
  url.searchParams.set('X-Amz-Expires', String(expiresIn));
  url.searchParams.set('X-Amz-SignedHeaders', 'host');

  const canonicalRequest = `GET\n/${filePath}\n${url.searchParams}\nhost:${url.host}\n\nhost\nUNSIGNED-PAYLOAD`;
  const stringToSign = `AWS4-HMAC-SHA256\n${amzDate}\n${scope}\n${await sha256(canonicalRequest)}`;

  url.searchParams.set('X-Amz-Signature', toHex(await hmac(key, stringToSign)));

  return url.toString();
}

function encode(str) {
  return new TextEncoder().encode(str);
}

function toHex(buf) {
  return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
}

async function sha256(str) {
  return toHex(await crypto.subtle.digest('SHA-256', encode(str)));
}

async function hmac(key, msg) {
  key = typeof key === 'string' ? encode(key) : key;
  key = await crypto.subtle.importKey('raw', key, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return crypto.subtle.sign('HMAC', key, encode(msg));
}

function fromHex(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
  }
  return bytes;
}

// Derive the S3 filename from password using HMAC(masterKey, "filename:" + password)
async function deriveShareFilename(masterKey, password) {
  const sig = await hmac(fromHex(masterKey), "filename:" + password);
  // Use first 16 bytes (32 hex chars) to match Go implementation
  return toHex(sig).slice(0, 32);
}

// Derive the AES content key from password using HMAC(masterKey, "content:" + password)
// Returns raw key bytes (Uint8Array) for flexibility - caller imports as needed
async function deriveShareContentKey(masterKey, password) {
  const sig = await hmac(fromHex(masterKey), "content:" + password);
  // Return first 16 bytes for AES-128
  return new Uint8Array(sig).slice(0, 16);
}

// Decrypt share JSON using AES-GCM with synthetic IV
// Expects: nonce (12 bytes) || ciphertext || tag (16 bytes)
// keyBytes should be raw Uint8Array
async function decryptShareJSON(ciphertext, keyBytes) {
  const key = await crypto.subtle.importKey('raw', keyBytes, { name: 'AES-GCM' }, false, ['decrypt']);
  // Extract nonce from first 12 bytes
  const data = new Uint8Array(ciphertext);
  const nonce = data.slice(0, 12);
  const encrypted = data.slice(12);
  const plaintext = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: nonce }, key, encrypted);
  return new TextDecoder().decode(plaintext);
}

// Decrypt a byte range using AES-CTR (the CTR component of GCM)
// plaintextStart is the offset in the plaintext (not ciphertext)
// nonce is the 12-byte GCM nonce
// ciphertextBytes is the raw ciphertext bytes (not including nonce prefix)
async function decryptRange(ciphertextBytes, keyBytes, nonce, plaintextStart) {
  const key = await crypto.subtle.importKey('raw', keyBytes, { name: 'AES-CTR' }, false, ['decrypt']);

  // GCM uses CTR mode internally. The counter block is: nonce (12 bytes) || counter (4 bytes, big-endian)
  // Counter starts at 2 for the actual data (0 is unused, 1 is for the auth tag computation)
  // So plaintext byte N is encrypted with counter = 2 + floor(N / 16)
  const blockNum = Math.floor(plaintextStart / 16);
  const counter = new Uint8Array(16);
  counter.set(nonce, 0);
  // Set counter value (big-endian 32-bit at bytes 12-15), starting at 2
  const counterVal = 2 + blockNum;
  counter[12] = (counterVal >>> 24) & 0xff;
  counter[13] = (counterVal >>> 16) & 0xff;
  counter[14] = (counterVal >>> 8) & 0xff;
  counter[15] = counterVal & 0xff;

  // Pad input to align with block boundary
  const offsetInBlock = plaintextStart % 16;
  const paddedLen = Math.ceil((offsetInBlock + ciphertextBytes.length) / 16) * 16;
  const toDecrypt = new Uint8Array(paddedLen);
  toDecrypt.set(ciphertextBytes, offsetInBlock);

  const decrypted = await crypto.subtle.decrypt(
    { name: 'AES-CTR', counter, length: 128 },
    key,
    toDecrypt
  );

  // Extract only the bytes corresponding to the original input
  return new Uint8Array(decrypted).slice(offsetInBlock, offsetInBlock + ciphertextBytes.length);
}

import share from "./index.html"
import service from "./share-sw.js.txt"

export default {
  async fetch(request, env, ctx) {
      try {
          const missing = [];
          if (!env.S3_ENDPOINT) missing.push('S3_ENDPOINT');
          if (!env.S3_REGION) missing.push('S3_REGION');
          if (!env.S3_BUCKET) missing.push('S3_BUCKET');
          if (!env.S3_ACCESS_KEY) missing.push('S3_ACCESS_KEY');
          if (!env.S3_SECRET_KEY) missing.push('S3_SECRET_KEY');
          if (env.S3_GB_PATH === undefined) missing.push('S3_GB_PATH');
          if (!env.SHARE_MASTER_KEY) missing.push('SHARE_MASTER_KEY');
          // S3_GB_PATH is allowed to be empty string
          if (missing.length > 0) {
              return new Response(
                  `Missing required environment variables: ${missing.join(', ')}\n\n` +
                  'Please configure them in Settings > Variables\n\n' +
                  'Example values:\n' +
                  'S3_ENDPOINT: backblazeb2.com\n' +
                  'S3_REGION: us-west-002\n' +
                  'S3_BUCKET: my-backup\n' +
                  'S3_ACCESS_KEY: AKIAIOSFODNN7EXAMPLE\n' +
                  'S3_SECRET_KEY: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n' +
                  'S3_GB_PATH: \n' +
                  'SHARE_MASTER_KEY: (run gb webshare-secrets to get this)',
                  { status: 500 }
              );
          }

          const url = new URL(request.url);
          if (url.pathname.startsWith('/share-data/')) {
            const signingKey = beginSignature(env);
            let password = url.pathname.slice("/share-data/".length);
            // Remove .json suffix to get the actual password
            password = password.replace(/\.json$/, '');
            const shareKeyBytes = await deriveShareContentKey(env.SHARE_MASTER_KEY, password);

            // Derive the S3 filename from the password
            const s3Filename = await deriveShareFilename(env.SHARE_MASTER_KEY, password);
            const s3Path = `${env.S3_GB_PATH}share/${s3Filename}`;

            // Check for range mode: ?range=X-Y&nonce=HEX
            const rangeParam = url.searchParams.get('range');
            const nonceParam = url.searchParams.get('nonce');

            if (rangeParam && nonceParam) {
              // Range mode: fetch specific byte range, decrypt with CTR, sign URL
              const rangeMatch = rangeParam.match(/^(\d+)-(\d+)$/);
              if (!rangeMatch) {
                return new Response("Invalid range format, expected X-Y", { status: 400 });
              }
              const plaintextStart = parseInt(rangeMatch[1]);
              const plaintextEnd = parseInt(rangeMatch[2]);
              if (plaintextStart > plaintextEnd) {
                return new Response("Invalid range: start > end", { status: 400 });
              }

              const nonce = fromHex(nonceParam);
              if (nonce.length !== 12) {
                return new Response("Invalid nonce length", { status: 400 });
              }

              // Fetch the byte range from S3
              // Ciphertext layout: nonce (12 bytes) || encrypted data || tag (16 bytes)
              // Plaintext byte X maps to ciphertext byte 12 + X
              const ciphertextStart = 12 + plaintextStart;
              const ciphertextEnd = 12 + plaintextEnd;

              const presignedJsonUrl = await generatePresignedUrl(env, s3Path, await signingKey);
              const response = await fetch(presignedJsonUrl, {
                headers: { 'Range': `bytes=${ciphertextStart}-${ciphertextEnd}` }
              });
              if (!response.ok && response.status !== 206) {
                if (response.status === 404) {
                  return new Response("404", { status: 404 });
                }
                return new Response(`Error reading from S3: ${response.status} ${response.statusText}`, { status: 500 });
              }

              const ciphertextBytes = new Uint8Array(await response.arrayBuffer());

              // Decrypt using AES-CTR
              let plaintext;
              try {
                plaintext = await decryptRange(ciphertextBytes, shareKeyBytes, nonce, plaintextStart);
              } catch (e) {
                return new Response("Decryption failed: " + e.message, { status: 500 });
              }

              const plaintextStr = new TextDecoder().decode(plaintext);

              // Boundary check: must start and end with newline
              // JSONL format has \n before and after every entry
              if (!plaintextStr.startsWith('\n') || !plaintextStr.endsWith('\n')) {
                return new Response("Range must start and end with newline", { status: 400 });
              }

              // Extract the JSON line (trim surrounding newlines)
              const jsonLine = plaintextStr.slice(1, -1);

              // Verify it's valid JSON and a single line (no embedded newlines)
              if (jsonLine.includes('\n')) {
                return new Response("Range contains multiple lines", { status: 400 });
              }

              let json;
              try {
                json = JSON.parse(jsonLine);
              } catch (e) {
                return new Response("Invalid JSON in range", { status: 400 });
              }

              // Check for revocation
              if (json.revoked) {
                return new Response(JSON.stringify(json), { status: 403, headers: { "Content-Type": "application/json" } });
              }

              // Sign the URL
              const now = Math.floor(Date.now() / 1000);
              let presignedExpiry = 30;
              if (json.expires_at) {
                const expiresAt = parseInt(json.expires_at);
                if (now >= expiresAt) {
                  return new Response(JSON.stringify({ error: "expired", expires_at: expiresAt }), { status: 410, headers: { "Content-Type": "application/json" } });
                }
                presignedExpiry = Math.min(presignedExpiry, expiresAt - now);
              }
              json.url = await generatePresignedUrl(env, json.path, await signingKey, presignedExpiry);

              return new Response(JSON.stringify(json), { headers: { "Content-Type": "application/json" } });
            }

            // Listing mode: return metadata for browser to fetch and decrypt
            // Browser will extract nonce from the first 12 bytes of the encrypted file
            const presignedJsonUrl = await generatePresignedUrl(env, s3Path, await signingKey, 60);

            return new Response(JSON.stringify({
              url: presignedJsonUrl,
              key: toHex(shareKeyBytes)
            }), { headers: { "Content-Type": "application/json" } });
          }

          const path = url.pathname.slice(1); // remove leading "/"
          if (path === `gb/webshare/share-sw-${SW_HASH}.js`) {
            return new Response(service,  { headers: {
              'Content-Type': "application/javascript; charset=utf-8",
              'Content-Length': String(service.length),
              'Cache-Control': 'public, max-age=3600',
              'Service-Worker-Allowed': '/'
            }});
          }
          return new Response(share,  { headers: {
            'Content-Type': "text/html; charset=utf-8",
            'Content-Length': String(share.length),
            'Cache-Control': 'public, max-age=3600'
          }});
      } catch (e) {
          console.error("Crash:", e);
          return new Response(String(e), { status: 500 });
      }
  }
};
