cmake_minimum_required(VERSION 3.16)
project(msgpack_blob VERSION 1.6.0 LANGUAGES CXX)

# ── Standalone C++ MessagePack Blob library (no SQLite dependency) ─────────────
# A self-contained package — the C++ sibling of the python/, js/, rust/ and go/
# ports. Split into logical translation units (decode, encode, json, mutate,
# iterate) that share private internals via src/msgpack_blob_detail.hpp.
#
# Can be built standalone:
#     cmake -B build cpp && cmake --build build && ctest --test-dir build
# or pulled into the top-level build via add_subdirectory(cpp), which is how the
# repository root reuses the `msgpack_blob_static` target for its C++ ↔ SQLite
# interop tests.

option(MSGPACK_BUILD_TESTS "Build and register CTest tests" ON)
option(MSGPACK_BUILD_FUZZ  "Build the libFuzzer-based fuzz harness" OFF)

# Raw sources, exported to the parent scope so an instrumented fuzz build can
# recompile them with sanitizer flags.
set(MSGPACK_BLOB_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/msgpack_blob_decode.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/msgpack_blob_encode.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/msgpack_blob_json.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/msgpack_blob_mutate.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/msgpack_blob_iterate.cpp
)
if(NOT CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(MSGPACK_BLOB_SOURCES ${MSGPACK_BLOB_SOURCES} PARENT_SCOPE)
endif()

add_library(msgpack_blob_static STATIC ${MSGPACK_BLOB_SOURCES})
target_include_directories(msgpack_blob_static PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
set_target_properties(msgpack_blob_static PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
if(NOT MSVC)
    target_compile_options(msgpack_blob_static PRIVATE -Wall -Wextra)
endif()

install(TARGETS msgpack_blob_static ARCHIVE DESTINATION lib)
install(FILES include/msgpack_blob.hpp DESTINATION include)

# ── Tests ─────────────────────────────────────────────────────────────────────
if(MSGPACK_BUILD_TESTS)
    enable_testing()

    # C++ Blob API unit tests (standalone, no SQLite dependency)
    add_executable(msgpack_blob_unit tests/test_msgpack_blob.cpp)
    target_link_libraries(msgpack_blob_unit PRIVATE msgpack_blob_static)
    set_target_properties(msgpack_blob_unit PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
    add_test(NAME msgpack_blob_unit COMMAND msgpack_blob_unit)
    set_tests_properties(msgpack_blob_unit PROPERTIES PASS_REGULAR_EXPRESSION "0 failed")

    # Cross-language test-vector generator (reference impl = C++ Blob library).
    # Run it to refresh tests/vectors/blob_vectors.json consumed by every port:
    #   cmake --build build --target blob_vectors_gen
    #   ./build/blob_vectors_gen > tests/vectors/blob_vectors.json
    add_executable(blob_vectors_gen tests/gen_blob_vectors.cpp)
    target_link_libraries(blob_vectors_gen PRIVATE msgpack_blob_static)
    set_target_properties(blob_vectors_gen PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)

    # C++ API corpus runner (no libFuzzer needed — runs the shared corpus files).
    add_executable(fuzz_blob_corpus_runner
        tests/fuzz_blob_corpus_runner.cpp tests/fuzz_msgpack_blob.cpp)
    target_link_libraries(fuzz_blob_corpus_runner PRIVATE msgpack_blob_static)
    set_target_properties(fuzz_blob_corpus_runner PROPERTIES CXX_STANDARD 17)
    if(NOT MSVC)
        target_compile_options(fuzz_blob_corpus_runner PRIVATE -w)
    endif()
    add_test(NAME fuzz_blob_corpus
        COMMAND fuzz_blob_corpus_runner ${CMAKE_CURRENT_SOURCE_DIR}/../tests/fuzz_corpus)
    set_tests_properties(fuzz_blob_corpus PROPERTIES PASS_REGULAR_EXPRESSION "0 failed")
endif()

# ── Fuzz testing (requires Clang with libFuzzer support) ──────────────────────
if(MSGPACK_BUILD_FUZZ)
    # Compile the library sources directly so sanitizer/coverage instrumentation
    # reaches the Blob code, not just the harness.
    add_executable(fuzz_msgpack_blob
        tests/fuzz_msgpack_blob.cpp ${MSGPACK_BLOB_SOURCES})
    target_include_directories(fuzz_msgpack_blob PRIVATE include)
    set_target_properties(fuzz_msgpack_blob PROPERTIES CXX_STANDARD 17)
    target_compile_options(fuzz_msgpack_blob PRIVATE -fsanitize=fuzzer,address -g)
    target_link_options(fuzz_msgpack_blob PRIVATE -fsanitize=fuzzer,address)
endif()
