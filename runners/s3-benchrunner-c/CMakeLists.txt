cmake_minimum_required(VERSION 3.22)
cmake_policy(VERSION 3.22...3.24)
project(s3-benchrunner-c-utils CXX)

option(CPP_SDK_RUNNER "If enabled, build the cpp runner" OFF)

file(GLOB UTILS_HEADERS
    include/*.h
)
file(GLOB UTILS_SOURCE
    source/*.cpp
)

file(GLOB UTILS_SRC
    ${UTILS_HEADERS}
    ${UTILS_SOURCE}
)

add_library(${PROJECT_NAME} ${UTILS_SRC})

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

find_package(aws-c-s3)

target_include_directories(${PROJECT_NAME} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_link_libraries(${PROJECT_NAME} AWS::aws-c-s3 nlohmann_json)
add_subdirectory(cs3Runner)

if(CPP_SDK_RUNNER)
    add_subdirectory(SdkBenchRunner)
endif()
