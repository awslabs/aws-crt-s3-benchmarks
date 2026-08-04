# s3-benchrunner-alt-js

s3-benchrunner for [@aws-sdk/lib-tm-alt](https://github.com/aws/aws-sdk-js-v3/tree/main/lib/lib-tm-alt) — the alternate client per worker thread based transfer manager

## Preparing workload files

Currently we are testing with file-based workloads. Prepare the local files and S3 objects:

```sh
cd aws-crt-s3-benchmarks
python3 scripts/prep-s3-files.py \
  --bucket s3-newinstance-benchmarks \
  --region us-west-2 \
  --files-dir ~/files \
  --workloads workloads/upload-5GiB-1x.run.json
```

This creates the 5 GiB random file under `~/files/upload/5GiB-1x/`.

## Building

First, checkout branch https://github.com/aws/aws-sdk-js-v3/tree/kuhe/experiment/tm and pack `lib-tm-alt`:

```sh
cd aws-sdk-js-v3
git checkout kuhe/experiment/tm
cd lib/lib-tm-alt
yarn && yarn build
npm pack --pack-destination ~/repo/aws-crt-s3-benchmarks/runners/s3-benchrunner-alt-js/
cd ~/repo/aws-crt-s3-benchmarks/runners/s3-benchrunner-alt-js
mv aws-sdk-lib-tm-alt-*.tgz aws-sdk-lib-tm-alt.tgz
```

Before installing, make sure `package.json` points to the correct tarball filename:

```json
{
  "dependencies": {
    "@aws-sdk/client-s3": "^3.1090.0",
    "@aws-sdk/lib-tm-alt": "file:./aws-sdk-lib-tm-alt.tgz"
  }
}
```

Then install dependencies:

```sh
npm install
```

## Running

```sh
node main.js S3_CLIENT WORKLOAD BUCKET REGION FILES_DIR [MAX_CONCURRENCY] [TARGET_PART_SIZE_MB]
```

### S3_CLIENT options

- `sdk-js-tm-alt` — Uses `@aws-sdk/lib-tm-alt` S3UploadManager (worker-thread file-based uploads)

### Example

```sh
node main.js sdk-js-tm-alt \
  ../../workloads/upload-5GiB-1x.run.json \
  s3-newinstance-benchmarks \
  us-west-2 \
  ~/files \
  256 \
  64
```

- `MAX_CONCURRENCY` — total in-flight requests (default: 256, distributed across 64 workers)
- `TARGET_PART_SIZE_MB` — multipart part size in MiB (default: 64)

Results are written to `results/` in this directory.
