cmake_minimum_required(VERSION 3.16)
project(sqlite_msgpack VERSION 1.0.0 LANGUAGES C)

# ── Options ──────────────────────────────────────────────────────────────────
option(BUILD_SHARED_LIBS "Build msgpack as a loadable extension (.so/.dylib/.dll)" ON)
option(MSGPACK_BUILD_TESTS "Build and register CTest tests" ON)

# ── Compiler flags ────────────────────────────────────────────────────────────
# Suppress MSVC warnings about POSIX names and use of standard C library functions
if(MSVC)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS _CRT_NONSTDC_NO_WARNINGS)
    add_compile_options(/W3)
else()
    # Apply only basic warnings globally; -Wpedantic is intentionally omitted
    # because the bundled SQLite amalgamation and shell trigger pedantic warnings
    # (string-length limits, extra semicolons) that are not ours to fix.
    add_compile_options(-Wall -Wextra)
endif()

# ── Platform-specific link libraries helper ───────────────────────────────────
# On Linux the dynamic loader (dl) and math (m) libraries must be explicit.
# On Windows they are implicit. On macOS neither is needed.
function(target_platform_libs target scope)
    if(UNIX AND NOT APPLE)
        target_link_libraries(${target} ${scope} dl pthread m)
    elseif(WIN32)
        # Nothing extra needed; shlwapi provides path helpers used by the shell
        target_link_libraries(${target} ${scope} shlwapi)
    endif()
endfunction()

# ── SQLite amalgamation (compiled once, reused by all targets) ─────────────────
add_library(sqlite3_amalg STATIC src/sqlite3.c)
target_include_directories(sqlite3_amalg PUBLIC include)
target_compile_definitions(sqlite3_amalg PUBLIC
    SQLITE_THREADSAFE=1
    SQLITE_ENABLE_JSON1
    SQLITE_ENABLE_LOAD_EXTENSION=1
)
# Suppress warnings from third-party SQLite source we don't control
if(NOT MSVC)
    target_compile_options(sqlite3_amalg PRIVATE -w)
endif()
set_target_properties(sqlite3_amalg PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_platform_libs(sqlite3_amalg PUBLIC)

# ── Loadable extension ────────────────────────────────────────────────────────
if(BUILD_SHARED_LIBS)
    add_library(msgpack MODULE src/msgpack.c)
    target_include_directories(msgpack PRIVATE include)

    # macOS: resolve SQLite symbols at load time (not at link time)
    if(APPLE)
        target_link_options(msgpack PRIVATE -undefined dynamic_lookup)
    endif()

    # Windows: the extension DLL must import sqlite3 symbols from the host process.
    # The __declspec(dllexport) on the entry point is handled by SQLITE_API in
    # sqlite3ext.h when SQLITE_CORE is NOT defined, so no extra step is needed.
    if(WIN32)
        # Ensure the entry-point symbol is exported from the DLL
        set_target_properties(msgpack PROPERTIES
            WINDOWS_EXPORT_ALL_SYMBOLS ON
        )
    endif()

    set_target_properties(msgpack PROPERTIES
        PREFIX ""                      # msgpack.so / msgpack.dylib / msgpack.dll
        C_VISIBILITY_PRESET hidden
    )
    install(TARGETS msgpack
        LIBRARY  DESTINATION lib       # .so / .dylib
        RUNTIME  DESTINATION bin       # .dll on Windows
    )
endif()

# ── Static library (for embedding in test runners) ────────────────────────────
add_library(msgpack_static STATIC src/msgpack.c)
target_include_directories(msgpack_static PRIVATE include)
target_compile_definitions(msgpack_static PRIVATE SQLITE_CORE)

# ── SQLite3 CLI shell ─────────────────────────────────────────────────────────
add_executable(sqlite3_cli src/shell.c src/sqlite3.c)
target_include_directories(sqlite3_cli PRIVATE include)
target_compile_definitions(sqlite3_cli PRIVATE
    SQLITE_THREADSAFE=1
    SQLITE_ENABLE_JSON1
    SQLITE_ENABLE_LOAD_EXTENSION=1
)
# Suppress warnings from third-party SQLite source we don't control
if(NOT MSVC)
    target_compile_options(sqlite3_cli PRIVATE -w)
endif()
target_platform_libs(sqlite3_cli PRIVATE)

# ── Tests ─────────────────────────────────────────────────────────────────────
if(MSGPACK_BUILD_TESTS)
    enable_testing()

    # Helper macro: build a static-linked C test binary and register with CTest
    macro(add_msgpack_test target src)
        add_executable(${target} tests/${src})
        target_include_directories(${target} PRIVATE include)
        target_compile_definitions(${target} PRIVATE SQLITE_CORE)
        target_link_libraries(${target} PRIVATE sqlite3_amalg msgpack_static)
        target_platform_libs(${target} PRIVATE)
        add_test(NAME ${target} COMMAND ${target})
        set_tests_properties(${target} PROPERTIES PASS_REGULAR_EXPRESSION "0 failed")
    endmacro()

    # Original C unit tests
    add_msgpack_test(msgpack_unit test_msgpack.c)

    # SQL integration test via CLI (loadable extension required)
    if(BUILD_SHARED_LIBS)
        add_test(NAME msgpack_sql
            COMMAND ${CMAKE_COMMAND}
                -DSQLITE3_EXE=$<TARGET_FILE:sqlite3_cli>
                -DEXT=$<TARGET_FILE:msgpack>
                -DTEST_SQL=${CMAKE_SOURCE_DIR}/tests/msgpack01.test
                -P ${CMAKE_SOURCE_DIR}/cmake/run_sql_test.cmake
        )
        set_tests_properties(msgpack_sql PROPERTIES
            PASS_REGULAR_EXPRESSION "All tests complete[.] 0 failures"
        )
    endif()

    # Spec-compliance phases (p1–p7)
    add_msgpack_test(msgpack_spec_p1 test_spec_p1_ints.c)
    add_msgpack_test(msgpack_spec_p2 test_spec_p2_scalars.c)
    add_msgpack_test(msgpack_spec_p3 test_spec_p3_containers.c)
    add_msgpack_test(msgpack_spec_p4 test_spec_p4_extract.c)
    add_msgpack_test(msgpack_spec_p5 test_spec_p5_mutation.c)
    add_msgpack_test(msgpack_spec_p6 test_spec_p6_json.c)
    add_msgpack_test(msgpack_spec_p7 test_spec_p7_agg_vtab.c)

endif()

# ── Install headers ───────────────────────────────────────────────────────────
install(FILES include/sqlite3.h include/sqlite3ext.h DESTINATION include)

# ── Performance benchmarks (optional, default OFF) ────────────────────────────
option(MSGPACK_BUILD_BENCH "Build the msgpack vs JSON vs JSONB benchmark" OFF)

if(MSGPACK_BUILD_BENCH)
    add_executable(bench_msgpack_vs_json tests/bench_msgpack_vs_json.c)
    target_include_directories(bench_msgpack_vs_json PRIVATE include)
    target_compile_definitions(bench_msgpack_vs_json PRIVATE SQLITE_CORE)
    target_link_libraries(bench_msgpack_vs_json PRIVATE sqlite3_amalg msgpack_static)
    target_platform_libs(bench_msgpack_vs_json PRIVATE)

    # run_bench — build and run, print table to stdout
    add_custom_target(run_bench
        COMMAND bench_msgpack_vs_json
        DEPENDS bench_msgpack_vs_json
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running msgpack vs JSON vs JSONB benchmarks..."
        USES_TERMINAL
    )

    # run_bench_readme — run benchmark, update README.md, then regenerate graph
    find_program(PYTHON3 NAMES python3 python)

    if(PYTHON3)
        add_custom_target(run_bench_readme
            # Step 1: run the C benchmark and rewrite the README bench section
            COMMAND ${CMAKE_COMMAND}
                -DBENCH_EXE=$<TARGET_FILE:bench_msgpack_vs_json>
                -DREADME=${CMAKE_SOURCE_DIR}/README.md
                -P ${CMAKE_SOURCE_DIR}/cmake/run_bench_readme.cmake
            # Step 2: regenerate the PNG from the freshly updated README table
            COMMAND ${PYTHON3}
                ${CMAKE_SOURCE_DIR}/tools/gen_bench_graph.py
                --readme ${CMAKE_SOURCE_DIR}/README.md
                --out    ${CMAKE_SOURCE_DIR}/docs/bench.png
            DEPENDS bench_msgpack_vs_json
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Running benchmarks, updating README.md, regenerating docs/bench.png..."
            USES_TERMINAL
        )

        # bench_graph — standalone: regenerate PNG from existing README table only
        add_custom_target(bench_graph
            COMMAND ${PYTHON3}
                ${CMAKE_SOURCE_DIR}/tools/gen_bench_graph.py
                --readme ${CMAKE_SOURCE_DIR}/README.md
                --out    ${CMAKE_SOURCE_DIR}/docs/bench.png
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Generating docs/bench.png from README benchmark table..."
            USES_TERMINAL
        )
    else()
        # No Python: run_bench_readme just updates the README text
        add_custom_target(run_bench_readme
            COMMAND ${CMAKE_COMMAND}
                -DBENCH_EXE=$<TARGET_FILE:bench_msgpack_vs_json>
                -DREADME=${CMAKE_SOURCE_DIR}/README.md
                -P ${CMAKE_SOURCE_DIR}/cmake/run_bench_readme.cmake
            DEPENDS bench_msgpack_vs_json
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
            COMMENT "Running benchmarks and updating README.md (no Python — graph not updated)..."
            USES_TERMINAL
        )
    endif()
endif()

# ── Size comparison (built alongside bench when MSGPACK_BUILD_BENCH=ON) ───────
if(MSGPACK_BUILD_BENCH)
    add_executable(bench_size_comparison tests/bench_size_comparison.c)
    target_include_directories(bench_size_comparison PRIVATE include)
    target_compile_definitions(bench_size_comparison PRIVATE SQLITE_CORE)
    target_link_libraries(bench_size_comparison PRIVATE sqlite3_amalg msgpack_static)
    target_platform_libs(bench_size_comparison PRIVATE)

    if(PYTHON3)
        add_custom_target(run_size_readme
            COMMAND ${CMAKE_COMMAND}
                -DSIZE_EXE=$<TARGET_FILE:bench_size_comparison>
                -DREADME=${CMAKE_SOURCE_DIR}/README.md
                -P ${CMAKE_SOURCE_DIR}/cmake/run_size_readme.cmake
            COMMAND ${PYTHON3}
                ${CMAKE_SOURCE_DIR}/tools/gen_size_graph.py
                --readme ${CMAKE_SOURCE_DIR}/README.md
                --out    ${CMAKE_SOURCE_DIR}/docs/size_comparison.png
            DEPENDS bench_size_comparison
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Running size comparison, updating README.md, generating docs/size_comparison.png..."
            USES_TERMINAL
        )

        add_custom_target(size_graph
            COMMAND ${PYTHON3}
                ${CMAKE_SOURCE_DIR}/tools/gen_size_graph.py
                --readme ${CMAKE_SOURCE_DIR}/README.md
                --out    ${CMAKE_SOURCE_DIR}/docs/size_comparison.png
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Generating docs/size_comparison.png from README size table..."
            USES_TERMINAL
        )
    else()
        add_custom_target(run_size_readme
            COMMAND ${CMAKE_COMMAND}
                -DSIZE_EXE=$<TARGET_FILE:bench_size_comparison>
                -DREADME=${CMAKE_SOURCE_DIR}/README.md
                -P ${CMAKE_SOURCE_DIR}/cmake/run_size_readme.cmake
            DEPENDS bench_size_comparison
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
            COMMENT "Running size comparison and updating README.md..."
            USES_TERMINAL
        )
    endif()
endif()
