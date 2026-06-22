//! `Iterator` — a cursor over container children (flat `each` / recursive `tree`).

use crate::blob::Blob;
use crate::decode as d;
use crate::iterate::{each_iter, tree_walk, EachRow};

/// Iterate over a container's children.
///
/// Supports flat (`each`) and recursive (`tree`) modes, mirroring the SQLite
/// extension's `msgpack_each` / `msgpack_tree` table-valued functions. Use the
/// C++-style [`Iterator::next`] / [`Iterator::current`] cursor, collect
/// [`Iterator::rows`], or drive it as a standard Rust iterator.
pub struct Iterator<'a> {
    blob: &'a Blob,
    base: String,
    recursive: bool,
    rows: Vec<EachRow>,
    cursor: i64,
    populated: bool,
}

impl<'a> Iterator<'a> {
    pub fn new(blob: &'a Blob, path: &str, recursive: bool) -> Iterator<'a> {
        Iterator {
            blob,
            base: if path.is_empty() {
                "$".to_string()
            } else {
                path.to_string()
            },
            recursive,
            rows: Vec::new(),
            cursor: -1,
            populated: false,
        }
    }

    fn populate(&mut self) {
        if self.populated {
            return;
        }
        self.populated = true;
        self.rows.clear();
        let a = self.blob.data();
        let n = a.len();
        if n == 0 {
            return;
        }

        let mut iroot = 0;
        if self.base != "$" {
            let (rc, istart, _) = d::lookup(a, n, 0, &self.base);
            if rc != d::RC_OK {
                return;
            }
            iroot = istart;
        }

        if self.recursive {
            let base = self.base.clone();
            tree_walk(a, n, iroot, &base, &base, 0, &mut self.rows);
        } else {
            self.rows = each_iter(a, n, iroot, &self.base);
        }
    }

    // ── C++-style cursor protocol ─────────────────────────────────────
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        self.populate();
        self.cursor += 1;
        (self.cursor as usize) < self.rows.len()
    }
    pub fn current(&self) -> &EachRow {
        &self.rows[self.cursor as usize]
    }
    pub fn reset(&mut self) {
        self.cursor = -1;
    }

    /// Collect all rows.
    pub fn rows(mut self) -> Vec<EachRow> {
        self.populate();
        self.rows
    }
}

impl<'a> IntoIterator for Iterator<'a> {
    type Item = EachRow;
    type IntoIter = std::vec::IntoIter<EachRow>;
    fn into_iter(mut self) -> Self::IntoIter {
        self.populate();
        self.rows.into_iter()
    }
}
