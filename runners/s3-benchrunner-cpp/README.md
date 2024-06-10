# s3-benchrunner-cpp

s3-benchrunner for [aws-sdk-cpp](https://github.com/awslabs/aws-sdk-cpp)

## Building

First, get the C++ SDK and install it somewhere (SDK_INSTALL_DIR)
```
git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp
cmake -S aws-sdk-cpp -B aws-sdk-cpp/build \
    -DCMAKE_INSTALL_PREFIX=<SDK_INSTALL_DIR> \
    -DCMAKE_BUILD_TYPE={Release,Debug} \
    -DBUILD_ONLY="s3-crt;transfer" \
    -DBUILD_SHARED_LIBS=OFF \
    -DENABLE_TESTING=OFF
cmake --build aws-sdk-cpp/build --target install
```

Build with `-DCMAKE_BUILD_TYPE=Release` for performance testing or `Debug` for debugging.

If you didn't install to a system directory, you'll need to set
`CMAKE_PREFIX_PATH=<SDK_INSTALL_DIR>` in the following steps:

Then build the runner:
```sh
cd aws-crt-s3-benchmarks/runners/s3-benchrunner-cpp
cmake -S . -B build -DCMAKE_PREFIX_PATH=<SDK_INSTALL_DIR> -DCMAKE_BUILD_TYPE={Release,Debug}
cmake --build build
```

## Running

See [instructions here](../../README.md#run-a-benchmark)
