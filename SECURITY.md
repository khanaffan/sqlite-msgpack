# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| Latest release | ✅ |
| Older releases | ❌ |

Only the latest release receives security fixes. We recommend always using the
most recent version.

## Reporting a vulnerability

If you discover a security vulnerability, **please do not open a public issue.**

Instead, report it privately by emailing the maintainers or using GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
feature on this repository.

Please include:

- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept (minimized test input preferred)
- The version or commit SHA where the issue was found

We aim to acknowledge reports within **48 hours** and provide a fix or mitigation
within **7 days** for confirmed vulnerabilities.

## Scope

The following are in scope for security reports:

- **Buffer overflows / out-of-bounds reads** in the SQLite extension (`src/msgpack.c`)
  or the C++ library (`src/msgpack_blob.cpp`) when processing untrusted msgpack blobs
- **Memory leaks** triggered by crafted input
- **Denial of service** via pathological input (excessive CPU or memory consumption)
- **Integer overflows** leading to incorrect behaviour or memory corruption
- **Path traversal** bugs in the `$`-rooted path parser

The following are **out of scope**:

- Bugs in the bundled SQLite amalgamation (`src/sqlite3.c`) — report those to the
  [SQLite project](https://www.sqlite.org/src/wiki?name=Bug+Reports) directly
- Issues requiring a compromised build environment or physical access
- Performance concerns that do not constitute denial of service

## Hardening measures

- **Input size limits**: blobs are capped at 64 MB (`kMaxOutput`), nesting depth
  at 200 levels (`kMaxDepth`), and the fuzz harness rejects inputs over 4 KB.
- **Fuzz testing**: both the SQL extension and C++ API have dedicated libFuzzer
  harnesses with AddressSanitizer, exercised against 94+ corpus files on every
  test run. See [CONTRIBUTING.md](CONTRIBUTING.md#fuzz-testing) for details.
- **Compiler warnings**: the C++ library compiles cleanly with
  `-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion`.
- **Copy-on-write mutation**: all mutation functions return new blobs; they never
  modify input buffers in place.
- **No dynamic memory from C API**: the SQLite extension uses SQLite's allocator
  (`sqlite3_malloc`/`sqlite3_free`) exclusively, benefiting from SQLite's own
  memory safety infrastructure.
