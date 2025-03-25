cmake_minimum_required(VERSION 3.22)
cmake_policy(VERSION 3.22...3.24)
project(s3-benchrunner-c CXX)

add_executable(${PROJECT_NAME}
    BenchmarkRunner.cpp
    CRunner.cpp)

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
FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.11.2/json.tar.xz)
FetchContent_MakeAvailable(json)

# CLI parser
FetchContent_Declare(
    argh_parser
    GIT_REPOSITORY https://github.com/adishavit/argh
    GIT_TAG v1.3.2
)
FetchContent_MakeAvailable(argh_parser)

find_package(aws-c-s3 REQUIRED)

target_link_libraries(${PROJECT_NAME} PRIVATE AWS::aws-c-s3 nlohmann_json argh)
