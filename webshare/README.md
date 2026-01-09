# GB SHARE
`gb share` allows backed up files to be downloaded in a browser from s3 through [presigned urls](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html)
without compromising on security or convenience

By default `gb share` will generate a url to https://leijurv.github.io/gb/share with a long set of url parameters that the javascript 
can use to download, decrypt, and decompress a file. This requires no setup from the user and gb doesn't have to make any requests
to any servers for it to work

To allow for persistent and short URLs this directory includes everything needed to setup a cloudflare worker that serves
a copy of the website that stores all of the file information in your s3 bucket, to be looked up using a short randomly generated password string

The following sections will explain how to setup the worker

### S3 CORS
For the browser to be able to make requests to s3 from github pages or your domain, CORS has to be configured in your 
bucket to allow files to be shared to other origins
![cors.png](cors.png)

## Deploying with wrangler
### Setup wrangler
Install wrangler with your package manager or `npm install -g wrangler`

run `wrangler login` 

wrangler will open a browser window to log into cloudflare

### Deploy
```
npm install
wrangler deploy
wrangler secret bulk <<< "$(gb webshare-secrets)"
```
`gb webshare-secrets` automatically generates all the secrets needed to run the worker. 
You will be prompted to provide a new S3 key just for the worker but this is optional but recommended.
S3 presigned urls can only be revoked by revoking the key that was used to make them

### 4. Configure the worker route
Your domain needs to be proxied by cloudflare and your domain needs to be configured to use your worker.
You can do this by creating a workers route. My configuration looks like this
![img.png](img.png)

### 5. Configure gb to create password URLs by default
Open .gb.conf in your editor, set `share_use_password_url` to `true`, and `share_password_url` to the url of your worker

When running `gb share` gb will now by default generate short links to your worker

gb also allows you to configure `share_url_password_length` to generate shorter passwords in the url. 
The default is 8.

## Self Hosting
The worker script is relatively simple and does not depend on any cloudflare features.
It is not currently supported, but it should easily be possible to write a simple script that is compatible with other server side 
js runtimes and just calls the `fetch` function in the worker script.