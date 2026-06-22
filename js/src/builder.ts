/* Builder — a streaming encoder that produces a Blob. */

import { Buf } from "./format.ts";
import { utf8Encode } from "./format.ts";
import * as E from "./encode.ts";
import { Blob } from "./blob.ts";
import { Value } from "./value.ts";

export class Builder {
  _buf: Buf;

  constructor() {
    this._buf = new Buf();
  }

  // ── scalars ───────────────────────────────────────────────────────
  nil(): Builder {
    E.encNil(this._buf);
    return this;
  }
  boolean(v: boolean): Builder {
    E.encBool(this._buf, v);
    return this;
  }
  integer(x: number | bigint): Builder {
    E.encInteger(this._buf, BigInt(x));
    return this;
  }
  unsignedInteger(x: number | bigint): Builder {
    E.encUnsigned(this._buf, BigInt(x));
    return this;
  }
  real(d: number): Builder {
    E.encReal(this._buf, d);
    return this;
  }
  real32(f: number): Builder {
    E.encReal32(this._buf, f);
    return this;
  }
  string(s: string | Uint8Array): Builder {
    E.encString(this._buf, typeof s === "string" ? utf8Encode(s) : s);
    return this;
  }
  binary(data: Uint8Array): Builder {
    E.encBinary(this._buf, data);
    return this;
  }
  ext(typeCode: number, data: Uint8Array): Builder {
    E.encExt(this._buf, typeCode, data);
    return this;
  }

  // ── fixed-width integers ──────────────────────────────────────────
  int8(x: number | bigint): Builder {
    E.encInt8(this._buf, BigInt(x));
    return this;
  }
  int16(x: number | bigint): Builder {
    E.encInt16(this._buf, BigInt(x));
    return this;
  }
  int32(x: number | bigint): Builder {
    E.encInt32(this._buf, BigInt(x));
    return this;
  }
  int64(x: number | bigint): Builder {
    E.encInt64(this._buf, BigInt(x));
    return this;
  }
  uint8(x: number | bigint): Builder {
    E.encUint8(this._buf, BigInt(x));
    return this;
  }
  uint16(x: number | bigint): Builder {
    E.encUint16(this._buf, BigInt(x));
    return this;
  }
  uint32(x: number | bigint): Builder {
    E.encUint32(this._buf, BigInt(x));
    return this;
  }
  uint64(x: number | bigint): Builder {
    E.encUint64(this._buf, BigInt(x));
    return this;
  }

  // ── containers ────────────────────────────────────────────────────
  arrayHeader(count: number): Builder {
    E.encArrayHeader(this._buf, count);
    return this;
  }
  mapHeader(count: number): Builder {
    E.encMapHeader(this._buf, count);
    return this;
  }

  // ── embedding & timestamp ─────────────────────────────────────────
  raw(data: Uint8Array | Blob): Builder {
    this._buf.pushBytes(data instanceof Blob ? data.data() : data);
    return this;
  }
  value(v: Value): Builder {
    E.encodeValue(this._buf, v);
    return this;
  }
  timestamp(sec: number | bigint, nsec: number = 0): Builder {
    E.encTimestamp(this._buf, BigInt(sec), nsec);
    return this;
  }

  // ── finalize ──────────────────────────────────────────────────────
  build(): Blob {
    return new Blob(this._buf.toBytes());
  }

  get length(): number {
    return this._buf.length;
  }

  static quote(v: Value): Blob {
    return new Builder().value(v).build();
  }
}
