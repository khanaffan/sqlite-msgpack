/* Iterator — a cursor over container children (flat each / recursive tree). */

import * as D from "./decode.ts";
import { EachRow, eachIter, treeWalk } from "./iterate.ts";
import { Blob } from "./blob.ts";

export { EachRow };

export class Iterator {
  _blob: Blob;
  _base: string;
  _recursive: boolean;
  _rows: EachRow[];
  _cursor: number;
  _populated: boolean;

  constructor(blob: Blob, path: string = "$", recursive: boolean = false) {
    this._blob = blob;
    this._base = path ? path : "$";
    this._recursive = recursive;
    this._rows = [];
    this._cursor = -1;
    this._populated = false;
  }

  _populate(): void {
    if (this._populated) return;
    this._populated = true;
    this._rows = [];
    const a = this._blob.data();
    const n = a.length;
    if (n === 0) return;

    let iroot = 0;
    if (this._base !== "$") {
      const r = D.lookup(a, n, 0, this._base);
      if (r.rc !== D.RC_OK) return;
      iroot = r.iStart;
    }

    if (this._recursive) treeWalk(a, n, iroot, this._base, this._base, 0, this._rows);
    else this._rows = eachIter(a, n, iroot, this._base);
  }

  // ── C++-style cursor protocol ─────────────────────────────────────
  next(): boolean {
    this._populate();
    this._cursor += 1;
    return this._cursor < this._rows.length;
  }
  current(): EachRow {
    return this._rows[this._cursor];
  }
  reset(): void {
    this._cursor = -1;
  }

  // ── iterable protocol ─────────────────────────────────────────────
  rows(): EachRow[] {
    this._populate();
    return this._rows.slice();
  }
  [Symbol.iterator](): IterableIterator<EachRow> {
    this._populate();
    return this._rows.slice()[Symbol.iterator]();
  }
}
