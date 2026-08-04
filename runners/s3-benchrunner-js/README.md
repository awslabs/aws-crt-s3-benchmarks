# s3-benchrunner-js

s3-benchrunner for [@aws-sdk/lib-transfer-manager](https://github.com/aws/aws-sdk-js-v3/tree/main/lib/lib-transfer-manager) — the current JS SDK transfer manager.

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

This creates the 5 GiB random files under `~/files/upload/5GiB-1x/`.

## Building

First, checkout and pack `lib-transfer-manager` from the [`kuhe/experiment/tm`](https://github.com/aws/aws-sdk-js-v3/tree/kuhe/experiment/tm) branch:

```sh
cd aws-sdk-js-v3
git checkout kuhe/experiment/tm
cd lib/lib-transfer-manager
yarn && yarn build
npm pack --pack-destination ~/repo/aws-crt-s3-benchmarks/runners/s3-benchrunner-js/
cd ~/repo/aws-crt-s3-benchmarks/runners/s3-benchrunner-js
mv aws-sdk-lib-transfer-manager-*.tgz aws-sdk-lib-transfer-manager.tgz
```

Before installing, make sure `package.json` points to the correct tarball filename:

```json
{
  "dependencies": {
    "@aws-sdk/client-s3": "^3.1092.0",
    "@aws-sdk/lib-transfer-manager": "file:./aws-sdk-lib-transfer-manager.tgz"
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

- `sdk-js-tm` — Uses `@aws-sdk/lib-transfer-manager` for uploads

### Example

```sh
node main.js sdk-js-tm \
  ../../workloads/upload-5GiB-10x.run.json \
  s3-newinstance-benchmarks \
  us-west-2 \
  ~/files \
  256 \
  64
```

- `MAX_CONCURRENCY` — total in-flight requests (default: SDK default)
- `TARGET_PART_SIZE_MB` — multipart part size in MiB (default: SDK default)

Results are written to `results/` in this directory.
