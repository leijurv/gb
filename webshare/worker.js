import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

function createS3Client(env) {
  let endpoint = env.S3_ENDPOINT;
  if (!endpoint.includes(env.S3_REGION)) endpoint = `s3.${env.S3_REGION}.${endpoint}`;

  return new S3Client({
    region: env.S3_REGION,
    endpoint: `https://${endpoint}`,
    credentials: {
      accessKeyId: env.S3_ACCESS_KEY,
      secretAccessKey: env.S3_SECRET_KEY,
    },
  });
}

async function generatePresignedUrl(env, filePath, expiresIn = 30) {
  const client = createS3Client(env);

  const command = new GetObjectCommand({
    Bucket: env.S3_BUCKET,
    Key: filePath,
  });

  const presignedUrl = await getSignedUrl(client, command, {
    expiresIn,
  });

  return presignedUrl;
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
                  'S3_GB_PATH: ',
                  { status: 500 }
              );
          }

          const url = new URL(request.url);
          if (url.pathname.startsWith('/share-data/')) {
            const key = url.pathname.slice("/share-data/".length);
            const presignedJsonUrl = await generatePresignedUrl(env, `${env.S3_GB_PATH}share/${key}`);
            const response = await fetch(presignedJsonUrl);
            if (!response.ok) {
              if (response.status === 404) {
                return new Response("404", { status: 404 })
              }
              return new Response(`Error reading from S3: ${response.status} ${response.statusText}`, { status: 500 });
            }
            let json = await response.json();
            json.url = await generatePresignedUrl(env, json.path);
            return new Response(JSON.stringify(json), { headers: { "Content-Type": "application/json" } });
          }

          const path = url.pathname.slice(1); // remove leading "/"
          if (path === "gb/webshare/share-sw.js") {
            return new Response(service,  { headers: {
              'Content-Type': "application/javascript; charset=utf-8",
              'Content-Length': String(share.length),
              'Cache-Control': 'public, max-age=3600'
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
