# s3-benchrunner-c

s3-benchrunner for [aws-c-s3](https://github.com/awslabs/aws-c-s3)

This runner is actually built with c++, to keep the code short and simple.

## Building

First, follow the build/install directions for [aws-c-s3](https://github.com/awslabs/aws-c-s3#building).

Build with `-DCMAKE_BUILD_TYPE=Release` for performance testing or `Debug` for debugging.

If you didn't install to a system directory, you'll need to set
`CMAKE_PREFIX_PATH=<AWS_C_S3_INSTALL_DIR>` in the following steps:

Then build the runner:
```sh
cd aws-crt-s3-benchmarks/runners/s3-benchrunner-c
cmake -S . -B build -DCMAKE_PREFIX_PATH=<AWS_C_S3_INSTALL_DIR> -DCMAKE_BUILD_TYPE={Release,RelWithDebInfo,Debug}
cmake --build build
```

## Running

See [instructions here](../../README.md#run-a-benchmark)
