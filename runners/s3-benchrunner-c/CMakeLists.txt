cmake_minimum_required(VERSION 3.22)
cmake_policy(VERSION 3.22...3.24)
project(s3-benchrunner-c CXX)

message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "C++ compiler flags: ${CMAKE_CXX_FLAGS}")

add_executable(${PROJECT_NAME}
    BenchmarkRunner.cpp
    CRunner.cpp)

install(TARGETS ${PROJECT_NAME}
    DESTINATION ${CMAKE_INSTALL_BINDIR})

set_target_properties(${PROJECT_NAME} PROPERTIES
    CXX_STANDARD 20)

if(NOT MSVC)
    target_compile_options(${PROJECT_NAME} PRIVATE "-fno-rtti")
endif()

# dependencies
include(FetchContent)

# using this JSON library because it's simple, and they documented how to integrate with CMake
FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.11.2/json.tar.xz)
FetchContent_MakeAvailable(json)

# using CLI11 for CLI argument parsing


FetchContent_Declare(
    cli11_proj
    QUIET
    GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
    GIT_TAG v2.5.0
)
FetchContent_MakeAvailable(cli11_proj)

find_package(aws-c-s3 REQUIRED)

target_link_libraries(${PROJECT_NAME} PRIVATE AWS::aws-c-s3 nlohmann_json)
target_link_libraries(${PROJECT_NAME} PRIVATE CLI11::CLI11)
