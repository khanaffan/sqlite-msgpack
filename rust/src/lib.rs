//! A pure-Rust MessagePack **Blob** library.
//!
//! A zero-dependency port of the standalone C++ `msgpack` Blob API from
//! [sqlite-msgpack](https://github.com/khanaffan/sqlite-msgpack). It creates,
//! queries, mutates and iterates MessagePack binary blobs and produces
//! **byte-identical** output to the C++ library and the `sqlite-msgpack`
//! extension, so blobs are fully interchangeable across all of them.
//!
//! # Example
//! ```
//! use msgpack_blob::{Blob, Builder, Value, Iterator};
//!
//! let blob = Blob::from_json(r#"{"name":"Alice","scores":[95,87,91]}"#);
//! assert_eq!(blob.extract("$.name").as_string(), "Alice");
//! assert_eq!(blob.to_json(), r#"{"name":"Alice","scores":[95,87,91]}"#);
//!
//! let updated = blob.set("$.age", &Value::integer(30));
//! assert_eq!(updated.to_json(), r#"{"name":"Alice","scores":[95,87,91],"age":30}"#);
//!
//! for row in Iterator::new(&blob, "$", true) {
//!     let _ = (&row.fullkey, row.ty);
//! }
//! ```

// The internal modules are deliberately a close, line-for-line port of the C++
// reference (cpp/src/msgpack_blob_*.cpp) to guarantee byte-identical behaviour.
// Several Clippy lints flag idioms that intentionally mirror the C source
// (verbatim `i + 1 <= n` bounds checks, faithful multi-argument recursive
// helpers, ternary-style `if`s); silencing them keeps the port a faithful copy.
#![allow(
    clippy::too_many_arguments,
    clippy::collapsible_match,
    clippy::collapsible_else_if,
    clippy::if_same_then_else,
    clippy::int_plus_one,
    clippy::implicit_saturating_sub,
    clippy::manual_range_contains
)]

mod decode;
mod encode;
mod format;
mod json;
mod mutate;

mod blob;
mod builder;
mod iterate;
mod iterator;
mod value;

pub use blob::Blob;
pub use builder::Builder;
pub use iterate::EachRow;
pub use iterator::Iterator;
pub use value::{type_str, IntWidth, Type, Value};

/// Maximum container nesting depth (matches the C++ library / SQLite extension).
pub const MAX_DEPTH: i32 = format::MAX_DEPTH;
/// Maximum output buffer size (64 MiB).
pub const MAX_OUTPUT: usize = format::MAX_OUTPUT;

/// Crate version.
pub const VERSION: &str = "1.7.0";
