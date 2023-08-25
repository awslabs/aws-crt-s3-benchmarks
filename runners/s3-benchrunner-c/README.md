# s3-benchrunner-c

s3-benchrunner for [aws-c-s3](https://github.com/awslabs/aws-c-s3)

## Building

First, follow the build/install directions for [aws-c-s3](https://github.com/awslabs/aws-c-s3#building).

Build with `-DCMAKE_BUILD_TYPE=Release` for performance testing or `Debug` for debugging.

If you didn't install to a system directory, you'll need to set
`CMAKE_PREFIX_PATH=<s3-install-dir>` in the following steps:

```sh
cd aws-crt-s3-benchmarks/runners/s3-benchrunner-c
cmake -S . -B build -DCMAKE_PREFIX_PATH={aws-c-s3-install-dir} -DCMAKE_BUILD_TYPE={Release,RelWithDebInfo,Debug}
cmake --build build
```

## Running

See [instructions here](../README.md)
