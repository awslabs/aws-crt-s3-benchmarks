cmake_minimum_required(VERSION 3.22)
cmake_policy(VERSION 3.22...3.24)
project(s3-benchrunner-cpp CXX)

add_executable(${PROJECT_NAME} SdkBenchRunner.cpp)

install(TARGETS ${PROJECT_NAME}
    DESTINATION ${CMAKE_INSTALL_BINDIR})

set_target_properties(${PROJECT_NAME} PROPERTIES
    CXX_STANDARD 20
    INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib64;${CMAKE_INSTALL_PREFIX}/lib")

find_package(AWSSDK REQUIRED COMPONENTS s3-crt s3)
find_library(UTILS_LIB NAME s3-benchrunner-c-utils)

if(NOT MSVC)
    target_compile_options(${PROJECT_NAME} PRIVATE "-fno-exceptions;-fno-rtti")
endif()

target_link_libraries(${PROJECT_NAME} s3-benchrunner-c-utils ${AWSSDK_LINK_LIBRARIES})
