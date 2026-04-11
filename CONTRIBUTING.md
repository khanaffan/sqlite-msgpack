# Contributing to sqlite-msgpack

Thanks for your interest in contributing! This document covers how to build, test,
and submit changes.

## Prerequisites

- **CMake** ≥ 3.16
- A **C11** and **C++17** compiler (Clang, GCC, or MSVC)
- **Git**

## Building

```bash
git clone https://github.com/<owner>/sqlite-msgpack.git
cd sqlite-msgpack
cmake -B build -DMSGPACK_BUILD_TESTS=ON
cmake --build build
```

### Build options

| Option | Default | Description |
|---|---|---|
| `BUILD_SHARED_LIBS` | `ON` | Build the loadable SQLite extension |
| `MSGPACK_BUILD_TESTS` | `ON` | Build and register CTest targets |
| `MSGPACK_BUILD_BENCH` | `OFF` | Build performance benchmarks |
| `MSGPACK_BUILD_FUZZ` | `OFF` | Build libFuzzer harnesses (requires Clang) |

## Running tests

```bash
cd build && ctest --output-on-failure
```

The full suite includes:

| Target | Description |
|---|---|
| `msgpack_unit` | C unit tests for the SQLite extension |
| `msgpack_sql` | SQL integration tests via CLI |
| `msgpack_spec_p1`–`p10` | Per-section msgpack spec compliance |
| `msgpack_blob_unit` | Standalone C++ API tests (289 assertions) |
| `msgpack_interop` | C++ ↔ SQLite interop tests (197 assertions) |
| `fuzz_corpus` | Fuzz corpus against the SQL extension |
| `fuzz_blob_corpus` | Fuzz corpus against the C++ API |

All tests must pass before submitting a pull request.

## Fuzz testing

Without libFuzzer, the corpus runners exercise the same code paths using seed
files in `tests/fuzz_corpus/`. These run as part of the normal CTest suite.

With libFuzzer (Clang required):

```bash
cmake -B build-fuzz -DMSGPACK_BUILD_FUZZ=ON \
  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
cmake --build build-fuzz
./build-fuzz/fuzz_msgpack tests/fuzz_corpus -max_total_time=300
./build-fuzz/fuzz_msgpack_blob tests/fuzz_corpus -max_total_time=300
```

If you find a crash, add the minimized input to `tests/fuzz_corpus/` alongside
your fix.

## Code style

- **C code** (`src/msgpack.c`): follows the existing SQLite-adjacent style — 2-space
  indent, `camelCase` locals, `snake_case` functions.
- **C++ code** (`src/msgpack_blob.cpp`, `include/msgpack_blob.hpp`): `snake_case`
  for methods and variables, `PascalCase` for types/enums, 4-space indent.
- Compiler warnings are the primary lint mechanism. The C++ library compiles
  cleanly with `-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion`.
- Comment only where the intent isn't obvious from the code.

## Project layout

```
include/
  msgpack_blob.hpp   C++ public header
  sqlite3.h          SQLite amalgamation header
src/
  msgpack.c          SQLite extension implementation
  msgpack_blob.cpp   Standalone C++ library
  sqlite3.c          SQLite amalgamation
tests/
  test_msgpack.c           C unit tests
  test_msgpack_blob.cpp    C++ API unit tests
  test_interop.cpp         C++ ↔ SQLite integration tests
  fuzz_msgpack.c           libFuzzer harness (SQL extension)
  fuzz_msgpack_blob.cpp    libFuzzer harness (C++ API)
  fuzz_corpus/             Seed corpus files
docs/
  cpp-api.md         C++ API reference
```

## Submitting changes

1. Fork the repository and create a feature branch.
2. Make your changes — keep commits focused and well-described.
3. Ensure all tests pass: `cd build && ctest --output-on-failure`
4. If you add new functionality, add corresponding tests.
5. If you change the public API, update `docs/cpp-api.md` and/or `README.md`.
6. Open a pull request with a clear description of what and why.

## Byte-identical encoding

A key invariant of this project: the C++ library and the SQLite extension must
produce **byte-identical** msgpack for the same logical data. The `msgpack_interop`
test verifies this with side-by-side hex comparisons. If you change encoding logic
in either API, make sure both stay in sync.

## License

By contributing, you agree that your contributions will be licensed under the
[MIT License](LICENSE).
