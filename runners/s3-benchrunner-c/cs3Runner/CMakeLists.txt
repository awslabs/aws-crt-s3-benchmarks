cmake_minimum_required(VERSION 3.22)
cmake_policy(VERSION 3.22...3.24)
project(s3-benchrunner-c CXX)

add_executable(${PROJECT_NAME} cs3Runner.cpp)

install(TARGETS ${PROJECT_NAME}
    DESTINATION ${CMAKE_INSTALL_BINDIR})

set_target_properties(${PROJECT_NAME} PROPERTIES
    CXX_STANDARD 20)

if(NOT MSVC)
    target_compile_options(${PROJECT_NAME} PRIVATE "-fno-exceptions;-fno-rtti")
endif()

find_library(UTILS_LIB NAME s3-benchrunner-c-utils)

target_link_libraries(${PROJECT_NAME} s3-benchrunner-c-utils)
