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
async function deriveShareContentKey(masterKey, password) {
  const sig = await hmac(fromHex(masterKey), "content:" + password);
  // Return first 16 bytes for AES-128
  const keyBytes = new Uint8Array(sig).slice(0, 16);
  const key = await crypto.subtle.importKey('raw', keyBytes, { name: 'AES-GCM' }, false, ['decrypt']);
  return key;
}

// Decrypt share JSON using AES-GCM with synthetic IV
// Expects: nonce (12 bytes) || ciphertext || tag (16 bytes)
async function decryptShareJSON(ciphertext, key, password) {
  // Extract nonce from first 12 bytes
  const data = new Uint8Array(ciphertext);
  const nonce = data.slice(0, 12);
  const encrypted = data.slice(12);
  const plaintext = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: nonce }, key, encrypted);
  return new TextDecoder().decode(plaintext);
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
            // Check for -N suffix pattern (e.g., "password-5.json" -> extract index 5)
            let fileIndex = null;
            const indexMatch = password.match(/^(.+)-(\d+)(\.json)$/);
            if (indexMatch) {
              password = indexMatch[1] + indexMatch[3]; // Reconstruct as "password.json"
              fileIndex = parseInt(indexMatch[2]);
            }
            // Remove .json suffix to get the actual password
            password = password.replace(/\.json$/, '');
            const shareKey = deriveShareContentKey(env.SHARE_MASTER_KEY, password);

            // Derive the S3 filename from the password
            const s3Filename = await deriveShareFilename(env.SHARE_MASTER_KEY, password);

            const presignedJsonUrl = await generatePresignedUrl(env, `${env.S3_GB_PATH}share/${s3Filename}`, await signingKey);
            const response = await fetch(presignedJsonUrl);
            if (!response.ok) {
              if (response.status === 404) {
                return new Response("404", { status: 404 })
              }
              return new Response(`Error reading from S3: ${response.status} ${response.statusText}`, { status: 500 });
            }

            // Decrypt the share JSON
            const encryptedData = await response.arrayBuffer();
            let jsonText;
            try {
              jsonText = await decryptShareJSON(encryptedData, await shareKey, password);
            } catch (e) {
              return new Response("Decryption failed", { status: 500 });
            }

            const now = Math.floor(Date.now() / 1000);
            async function setUrl(json) {
              let presignedExpiry = 30; // default
              if (json.expires_at) {
                const expiresAt = parseInt(json.expires_at);
                if (now >= expiresAt) {
                  return new Response(JSON.stringify({ error: "expired", expires_at: expiresAt }), { status: 410, headers: { "Content-Type": "application/json" } });
                }
                // Clamp presigned URL expiry to not exceed the share expiry
                presignedExpiry = Math.min(presignedExpiry, expiresAt - now);
              }
              json.url = await generatePresignedUrl(env, json.path, await signingKey, presignedExpiry);
            }

            let json = JSON.parse(jsonText);
            // revoked probably
            if (!Array.isArray(json)) {
              return new Response(JSON.stringify(json), { status: 403, headers: { "Content-Type": "application/json" } });
            }

            // Add index to each item so client can request individual files later
            json.forEach((item, i) => { item.index = i; });

            if (fileIndex !== null) {
              if (fileIndex < 0 || fileIndex >= json.length) {
                return new Response("File index out of range", { status: 404 });
              }
              json = [json[fileIndex]];
              // Sign URL only for individual file requests
              await Promise.all(json.map(inner => setUrl(inner)));
            } else if (json.length === 1) {
              // Single file share - sign the URL
              await Promise.all(json.map(inner => setUrl(inner)));
            }
            // For folder listings (multiple files, no specific index), don't sign URLs
            // Client will request individual file URLs when needed

            return new Response(JSON.stringify(json), { headers: { "Content-Type": "application/json" } });
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
