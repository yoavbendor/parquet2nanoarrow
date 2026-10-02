# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Yoav Bendor

cmake_minimum_required(VERSION 3.24)
project(parquet2nanoarrow LANGUAGES C CXX VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

option(P2N_BUILD_TESTS "Build parquet2nanoarrow tests, tools and benchmarks" ON)
option(P2N_WERROR "Treat warnings as errors" OFF)
option(P2N_PARQUET_TESTING "Fetch apache/parquet-testing (~8 MB) and check every file against pyarrow" ON)
set(P2N_SANITIZER "" CACHE STRING "Sanitizers for the whole tree, e.g. address,undefined")

if(NOT DEFINED FETCHCONTENT_BASE_DIR)
    set(FETCHCONTENT_BASE_DIR "${CMAKE_SOURCE_DIR}/.deps" CACHE PATH "Fetched dependency sources")
endif()
include(FetchContent)

if(P2N_SANITIZER)
    add_compile_options(-fsanitize=${P2N_SANITIZER} -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=${P2N_SANITIZER})
endif()

# --- nanom (header-only): the Thrift metadata model, page kernels and codecs ---
# Pinned to the nanom commit that carries tagged.hpp, columnar.hpp and codec.hpp. Point
# FETCHCONTENT_SOURCE_DIR_NANOM at a local checkout to develop both together.
if(NOT TARGET nanom::nanom)
    FetchContent_Declare(nanom
        GIT_REPOSITORY https://github.com/yoavbendor/nanom.git
        GIT_TAG ca68001a8107b980a49cd0b0c28e092fbb15250a
    )
    FetchContent_GetProperties(nanom)
    if(NOT nanom_POPULATED)
        FetchContent_Populate(nanom)  # headers only: do not pull nanom's tests/benches into this build
    endif()
    add_library(p2n_nanom INTERFACE)
    target_include_directories(p2n_nanom SYSTEM INTERFACE ${nanom_SOURCE_DIR}/include)
    add_library(nanom::nanom ALIAS p2n_nanom)
endif()

# --- nanoarrow (pinned to the same commit nanolance and nanoarrow2parquet use) ---
if(NOT TARGET nanoarrow_static AND NOT TARGET nanoarrow)
    set(NANOARROW_IPC OFF CACHE BOOL "" FORCE)
    set(NANOARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(NANOARROW_BUILD_APPS OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(nanoarrow
        GIT_REPOSITORY https://github.com/apache/arrow-nanoarrow.git
        GIT_TAG ffe61d3cd2d02da9e60cfc405cd6c50c512f64b9
    )
    FetchContent_MakeAvailable(nanoarrow)
endif()
if(TARGET nanoarrow_static)
    set(P2N_NANOARROW_TARGET nanoarrow_static)
else()
    set(P2N_NANOARROW_TARGET nanoarrow)
endif()

# --- zstd (ZSTD pages) ---
set(P2N_ZSTD_TARGET "")
find_package(zstd QUIET)
if(TARGET zstd::libzstd_static)
    set(P2N_ZSTD_TARGET zstd::libzstd_static)
elseif(TARGET zstd::libzstd_shared)
    set(P2N_ZSTD_TARGET zstd::libzstd_shared)
elseif(TARGET zstd::libzstd)
    set(P2N_ZSTD_TARGET zstd::libzstd)
endif()
if(NOT P2N_ZSTD_TARGET)
    set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)
    set(ZSTD_LEGACY_SUPPORT OFF CACHE BOOL "" FORCE)        # pre-1.0 zstd frames never appear in Parquet
    set(ZSTD_MULTITHREAD_SUPPORT OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(_p2n_zstd
        GIT_REPOSITORY https://github.com/facebook/zstd.git
        GIT_TAG v1.5.6
        SOURCE_SUBDIR build/cmake
    )
    FetchContent_MakeAvailable(_p2n_zstd)
    set(P2N_ZSTD_TARGET libzstd_static)
    target_include_directories(libzstd_static INTERFACE $<BUILD_INTERFACE:${_p2n_zstd_SOURCE_DIR}/lib>)
endif()

# --- zlib (GZIP pages), optional ---
find_package(ZLIB QUIET)

# --- library ---
add_library(parquet2nanoarrow STATIC
    src/mapped_file.cpp
    src/buffer_pool.cpp
    src/schema.cpp
    src/column_reader.cpp
    src/nested.cpp
    src/reader.cpp)
add_library(parquet2nanoarrow::parquet2nanoarrow ALIAS parquet2nanoarrow)
target_include_directories(parquet2nanoarrow PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
find_package(Threads REQUIRED)
target_link_libraries(parquet2nanoarrow PUBLIC nanom::nanom ${P2N_NANOARROW_TARGET}
                                        PRIVATE ${P2N_ZSTD_TARGET} Threads::Threads)
if(ZLIB_FOUND)
    target_link_libraries(parquet2nanoarrow PRIVATE ZLIB::ZLIB)
    target_compile_definitions(parquet2nanoarrow PRIVATE P2N_HAVE_ZLIB=1)
endif()
target_compile_options(parquet2nanoarrow PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${P2N_WERROR}>:-Werror>)

# Shared C-ABI library: the ArrowArrayStream entry point the Python differential test loads
# with ctypes (and any non-C++ consumer can use).
add_library(parquet2nanoarrow_c SHARED src/c_api.cpp)
target_link_libraries(parquet2nanoarrow_c PRIVATE parquet2nanoarrow)

if(P2N_BUILD_TESTS)
    enable_testing()
    function(p2n_exe name)
        add_executable(${name} ${ARGN})
        target_link_libraries(${name} PRIVATE parquet2nanoarrow)
        target_compile_options(${name} PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${P2N_WERROR}>:-Werror>)
    endfunction()
    p2n_exe(p2n_cat tools/p2n_cat.cpp)
    p2n_exe(p2n_bench bench/bench_read.cpp)
    p2n_exe(p2n_fuzz fuzz/fuzz_reader.cpp)
    p2n_exe(p2n_typed_read examples/typed_read.cpp)
    target_compile_definitions(p2n_fuzz PRIVATE P2N_FUZZ_STANDALONE=1)

    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_Interpreter_FOUND)
        add_test(NAME differential_pyarrow
                 COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/differential.py
                         $<TARGET_FILE:parquet2nanoarrow_c>)
        set_tests_properties(differential_pyarrow PROPERTIES SKIP_RETURN_CODE 77)
        add_test(NAME differential_pyarrow_threads
                 COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/differential.py
                         $<TARGET_FILE:parquet2nanoarrow_c>)
        set_tests_properties(differential_pyarrow_threads PROPERTIES SKIP_RETURN_CODE 77 ENVIRONMENT "P2N_THREADS=4")
        add_test(NAME typed_reader
                 COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/typed.py $<TARGET_FILE:p2n_typed_read>)
        set_tests_properties(typed_reader PROPERTIES SKIP_RETURN_CODE 77)
        add_test(NAME fuzz_corrupted_files
                 COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/make_fuzz_seeds.py
                         $<TARGET_FILE:p2n_fuzz>)
        set_tests_properties(fuzz_corrupted_files PROPERTIES SKIP_RETURN_CODE 77)
        if(P2N_PARQUET_TESTING)
            # Real-world files from parquet-mr, Spark, Impala, Arrow, … incl. legacy list/map layouts.
            FetchContent_Declare(parquet_testing
                GIT_REPOSITORY https://github.com/apache/parquet-testing.git
                GIT_TAG 56653c437c8092f704a092d0d1d4e600124cd49f
            )
            FetchContent_GetProperties(parquet_testing)
            if(NOT parquet_testing_POPULATED)
                FetchContent_Populate(parquet_testing)
            endif()
            add_test(NAME parquet_testing_corpus
                     COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/parquet_testing.py
                             $<TARGET_FILE:parquet2nanoarrow_c> ${parquet_testing_SOURCE_DIR})
            set_tests_properties(parquet_testing_corpus PROPERTIES SKIP_RETURN_CODE 77)
        endif()
    endif()
endif()
