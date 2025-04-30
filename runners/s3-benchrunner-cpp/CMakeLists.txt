cmake_minimum_required(VERSION 3.22)
cmake_policy(VERSION 3.22...3.24)
project(s3-benchrunner-cpp CXX)

add_executable(${PROJECT_NAME})

# It's hacky, but share code with s3-benchrunner-c by adding it as an
# include directory, and compiling shared cpp files
set(SHARED_CODE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../s3-benchrunner-c)

target_include_directories(${PROJECT_NAME} PRIVATE ${SHARED_CODE_DIR})

target_sources(${PROJECT_NAME} PRIVATE
    ${SHARED_CODE_DIR}/BenchmarkRunner.cpp
    SdkClient.cpp
    TransferManager.cpp
    main.cpp)

install(TARGETS ${PROJECT_NAME}
    DESTINATION ${CMAKE_INSTALL_BINDIR})

set_target_properties(${PROJECT_NAME} PROPERTIES
    CXX_STANDARD 20)

if(NOT MSVC)
    target_compile_options(${PROJECT_NAME} PRIVATE "-fno-exceptions;-fno-rtti")
endif()

# dependencies
include(FetchContent)

# using this JSON library because it's simple, and they documented how to integrate with CMake
FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz)
FetchContent_MakeAvailable(json)

# CLI parser. Using a commit because latest tagged release doesn't have CMake<3.5 depreciation fix.
FetchContent_Declare(argh_parser URL https://github.com/adishavit/argh/archive/c3f0d8c8a6dacb00df626b409248a34e3bcd15f5.zip)
FetchContent_MakeAvailable(argh_parser)

# aws-sdk-cpp fails to link on MacOS unless we manually add zlib here:
# https://github.com/aws/aws-sdk-cpp/issues/2635#issuecomment-1708483628
find_package(ZLIB REQUIRED)

find_package(AWSSDK REQUIRED COMPONENTS s3-crt transfer)

target_link_libraries(${PROJECT_NAME} nlohmann_json argh aws-cpp-sdk-s3-crt aws-cpp-sdk-transfer)
