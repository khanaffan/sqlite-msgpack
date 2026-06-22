/*
 * msgpack-blob — a pure-TypeScript MessagePack Blob library.
 *
 * A zero-dependency port of the standalone C++ `msgpack` Blob API. It creates,
 * queries, mutates and iterates MessagePack binary blobs and produces
 * byte-identical output to the C++ library and the `sqlite-msgpack` extension,
 * so blobs are fully interchangeable across all three.
 *
 * @example
 * import { Blob, Builder, Value, Iterator } from "msgpack-blob";
 *
 * const blob = Blob.fromJson('{"name":"Alice","scores":[95,87,91]}');
 * blob.extract("$.name").asString();        // 'Alice'
 * blob.toJson();                             // '{"name":"Alice","scores":[95,87,91]}'
 *
 * const updated = blob.set("$.age", Value.integer(30));
 * for (const row of new Iterator(blob, "$", true)) {
 *   console.log(row.fullkey, row.type);
 * }
 */

export { Type, IntWidth, Value, typeStr } from "./value.ts";
export { Blob } from "./blob.ts";
export { Builder } from "./builder.ts";
export { Iterator, EachRow } from "./iterator.ts";
export { MAX_DEPTH, MAX_OUTPUT } from "./format.ts";

export const VERSION = "1.6.0";
