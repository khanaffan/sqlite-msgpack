/* Blob — an owning byte buffer wrapping a msgpack-encoded value. */

import { Buf } from "./format.ts";
import * as D from "./decode.ts";
import * as E from "./encode.ts";
import * as J from "./json.ts";
import * as M from "./mutate.ts";
import { Type, Value } from "./value.ts";

export class Blob {
  _data: Uint8Array;

  constructor(data?: Uint8Array | number[] | null) {
    if (data === null || data === undefined) this._data = new Uint8Array(0);
    else if (data instanceof Uint8Array) this._data = data;
    else this._data = Uint8Array.from(data);
  }

  // ── raw access ────────────────────────────────────────────────────
  data(): Uint8Array {
    return this._data;
  }
  size(): number {
    return this._data.length;
  }
  empty(): boolean {
    return this._data.length === 0;
  }
  hex(): string {
    let s = "";
    for (let i = 0; i < this._data.length; i++) s += this._data[i].toString(16).padStart(2, "0");
    return s;
  }
  equals(other: Blob): boolean {
    if (this._data.length !== other._data.length) return false;
    for (let i = 0; i < this._data.length; i++) if (this._data[i] !== other._data[i]) return false;
    return true;
  }

  // ── validation ────────────────────────────────────────────────────
  valid(): boolean {
    return D.isValid(this._data, this._data.length);
  }
  errorPosition(): number {
    return D.errorPosition(this._data, this._data.length);
  }

  // ── type inspection ───────────────────────────────────────────────
  type(path?: string): Type {
    const n = this._data.length;
    if (path === undefined) return n === 0 ? Type.Nil : D.getType(this._data, n, 0);
    const r = D.lookup(this._data, n, 0, path);
    if (r.rc !== D.RC_OK) return Type.Nil;
    return D.getType(this._data, n, r.iStart);
  }
  typeStr(path?: string): string {
    return this.type(path);
  }

  // ── extraction ────────────────────────────────────────────────────
  extract(path: string): Value {
    const n = this._data.length;
    const r = D.lookup(this._data, n, 0, path);
    if (r.rc !== D.RC_OK) return Value.nil();
    return D.decodeElement(this._data, n, r.iStart, r.iEnd);
  }

  arrayLength(path?: string): number {
    const n = this._data.length;
    if (path === undefined) return n === 0 ? -1 : D.getContainerCount(this._data, n, 0);
    const r = D.lookup(this._data, n, 0, path);
    if (r.rc !== D.RC_OK) return -1;
    return D.getContainerCount(this._data, n, r.iStart);
  }

  // ── mutation (copy-on-write) ──────────────────────────────────────
  _apply(path: string, value: Value, mode: number): Blob {
    const nb = new Buf();
    E.encodeValue(nb, value);
    const r = M.applyEdit(this._data, this._data.length, path, nb.toBytes(), mode);
    return r.rc === M.RC_OK ? new Blob(r.out) : this;
  }

  set(path: string, value: Value | Blob): Blob {
    if (value instanceof Blob) {
      const r = M.applyEdit(this._data, this._data.length, path, value._data, M.EDIT_SET);
      return r.rc === M.RC_OK ? new Blob(r.out) : this;
    }
    return this._apply(path, value, M.EDIT_SET);
  }
  insert(path: string, value: Value): Blob {
    return this._apply(path, value, M.EDIT_INSERT);
  }
  replace(path: string, value: Value): Blob {
    return this._apply(path, value, M.EDIT_REPLACE);
  }
  arrayInsert(path: string, value: Value): Blob {
    return this._apply(path, value, M.EDIT_ARRAY_INS);
  }
  remove(path: string): Blob {
    const r = M.applyEdit(this._data, this._data.length, path, new Uint8Array(0), M.EDIT_REMOVE);
    return r.rc === M.RC_OK ? new Blob(r.out) : this;
  }
  patch(mergePatch: Blob): Blob {
    const r = M.mergePatch(this._data, this._data.length, 0, mergePatch._data, mergePatch._data.length, 0, 0);
    return r.rc === M.RC_OK ? new Blob(r.out) : this;
  }

  // ── JSON conversion ───────────────────────────────────────────────
  toJson(): string {
    if (this._data.length === 0) return "null";
    return J.toJson(this._data, this._data.length, false, 0);
  }
  toJsonPretty(indent: number = 2): string {
    if (this._data.length === 0) return "null";
    indent = Math.max(0, Math.min(8, indent));
    return J.toJson(this._data, this._data.length, true, indent);
  }

  static fromJson(json: string | Uint8Array | null | undefined): Blob {
    return new Blob(J.fromJson(json));
  }
}
