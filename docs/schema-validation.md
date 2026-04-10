# Schema Validation Guide

`msgpack_schema_validate()` validates MessagePack BLOBs against declarative
schemas expressed as JSON text or msgpack BLOBs. It brings data-contract
enforcement into SQL — via `CHECK` constraints, triggers, or ad-hoc queries.

For the full function API, see the
[README — Schema validation](../README.md#schema-validation) section.

---

## Table of contents

1. [Quick start](#quick-start)
2. [Schema language overview](#schema-language-overview)
3. [Type system mapping](#type-system-mapping)
4. [Keyword reference](#keyword-reference)
   - [type](#type)
   - [const](#const)
   - [enum](#enum)
   - [minimum / maximum](#minimum--maximum)
   - [exclusiveMinimum / exclusiveMaximum](#exclusiveminimum--exclusivemaximum)
   - [minLength / maxLength](#minlength--maxlength)
   - [items](#items)
   - [minItems / maxItems](#minitems--maxitems)
   - [properties](#properties)
   - [required](#required)
   - [additionalProperties](#additionalproperties)
5. [Boolean schemas](#boolean-schemas)
6. [Composing schemas](#composing-schemas)
7. [Schema input formats](#schema-input-formats)
8. [Performance and caching](#performance-and-caching)
9. [Common patterns](#common-patterns)
   - [CHECK constraints](#check-constraints)
   - [Trigger-based validation](#trigger-based-validation)
   - [Schema registry table](#schema-registry-table)
   - [Auditing invalid rows](#auditing-invalid-rows)
10. [Comparison with JSON Schema](#comparison-with-json-schema)
11. [Limitations and future work](#limitations-and-future-work)

---

## Quick start

```sql
-- Load the extension
.load ./msgpack

-- Validate a simple integer
SELECT msgpack_schema_validate(
  msgpack_quote(42),
  '{"type":"integer","minimum":0}'
);
-- 1

-- Validate a user object
SELECT msgpack_schema_validate(
  msgpack_object('name', 'Alice', 'age', 30),
  '{
    "type": "map",
    "required": ["name", "age"],
    "properties": {
      "name": {"type": "text", "minLength": 1},
      "age":  {"type": "integer", "minimum": 0, "maximum": 150}
    }
  }'
);
-- 1

-- Use in a CHECK constraint
CREATE TABLE users (
  id      INTEGER PRIMARY KEY,
  profile BLOB NOT NULL
    CHECK (msgpack_schema_validate(profile, '{
      "type": "map",
      "required": ["name"],
      "properties": {
        "name": {"type": "text", "minLength": 1}
      }
    }'))
);
```

---

## Schema language overview

Schemas are JSON objects (or msgpack maps) whose keys are **constraint
keywords**. The validator checks each keyword against the data and returns `1`
only if every constraint passes.

Design principles:

1. **Small surface area** — only keywords needed for MessagePack's type system.
2. **Composable** — schemas nest naturally via `properties` and `items`.
3. **Familiar** — names and semantics mirror JSON Schema where possible.

An empty schema `{}` or boolean `true` accepts everything. Boolean `false`
rejects everything.

---

## Type system mapping

MessagePack types map to schema type names as follows:

| Schema type name | MessagePack formats | `msgpack_type()` returns |
|------------------|---------------------|--------------------------|
| `"null"` | nil (`0xc0`) | `"null"` |
| `"bool"` | true (`0xc3`), false (`0xc2`) | `"true"` or `"false"` |
| `"integer"` | positive fixint, negative fixint, uint8–64, int8–64 | `"integer"` |
| `"real"` | float32, float64 | `"real"` |
| `"text"` | fixstr, str8, str16, str32 | `"text"` |
| `"blob"` | bin8, bin16, bin32 | `"blob"` |
| `"array"` | fixarray, array16, array32 | `"array"` |
| `"map"` | fixmap, map16, map32 | `"map"` |
| `"any"` | *(all of the above)* | *(any)* |

> **Note on booleans:** `msgpack_type()` returns `"true"` or `"false"` for
> boolean values — not `"bool"`. The schema type `"bool"` is a convenience
> alias that matches both true and false.

> **Note on `"any"`:** The type `"any"` is a wildcard that matches every
> MessagePack type. It is useful in `additionalProperties` to allow any value
> type while still validating known properties.

---

## Keyword reference

### `type`

Restricts the allowed MessagePack type of the value.

**Value:** A single type name (text) or an array of type names (union).

```sql
-- Single type
SELECT msgpack_schema_validate(msgpack_quote(42), '{"type":"integer"}');    -- 1
SELECT msgpack_schema_validate(msgpack_quote('hi'), '{"type":"integer"}');  -- 0

-- Union type: accept integer or null
SELECT msgpack_schema_validate(msgpack_quote(NULL), '{"type":["integer","null"]}');  -- 1
SELECT msgpack_schema_validate(msgpack_quote('x'), '{"type":["integer","null"]}');   -- 0
```

When `type` is an array, the value must match **at least one** of the listed
types.

---

### `const`

Requires the value to be byte-identical to the specified constant. The
comparison is performed on the raw msgpack encoding.

**Value:** Any JSON/msgpack value.

```sql
SELECT msgpack_schema_validate(msgpack_quote(42), '{"const":42}');    -- 1
SELECT msgpack_schema_validate(msgpack_quote(99), '{"const":42}');    -- 0
SELECT msgpack_schema_validate(msgpack_quote('ok'), '{"const":"ok"}'); -- 1
```

> `const` compares msgpack bytes, so `42` (integer) and `42.0` (real) are
> distinct values because they have different encodings.

---

### `enum`

Lists the allowed values. The data must exactly match one of them.

**Value:** An array of allowed values.

```sql
SELECT msgpack_schema_validate(
  msgpack_quote('active'),
  '{"enum":["active","inactive","pending"]}'
);  -- 1

-- Mixed types in enum
SELECT msgpack_schema_validate(
  msgpack_quote(NULL),
  '{"enum":[1, 2, 3, "text", null, true, false]}'
);  -- 1
```

> Like `const`, comparison is byte-level on the msgpack encoding.

---

### `minimum` / `maximum`

Constrain the numeric value with inclusive bounds.

**Applies to:** `integer` and `real` types only. Silently ignored for other
types.

**Value:** A number.

```sql
-- value ≥ 0 AND value ≤ 150
SELECT msgpack_schema_validate(
  msgpack_quote(25),
  '{"type":"integer","minimum":0,"maximum":150}'
);  -- 1

SELECT msgpack_schema_validate(
  msgpack_quote(-1),
  '{"type":"integer","minimum":0}'
);  -- 0
```

---

### `exclusiveMinimum` / `exclusiveMaximum`

Constrain the numeric value with exclusive (strict) bounds.

**Applies to:** `integer` and `real` types only.

**Value:** A number.

```sql
-- value > 0 (exclusive: 0 itself fails)
SELECT msgpack_schema_validate(msgpack_quote(0), '{"exclusiveMinimum":0}');  -- 0
SELECT msgpack_schema_validate(msgpack_quote(1), '{"exclusiveMinimum":0}');  -- 1

-- value < 100
SELECT msgpack_schema_validate(msgpack_quote(100), '{"exclusiveMaximum":100}');  -- 0
SELECT msgpack_schema_validate(msgpack_quote(99), '{"exclusiveMaximum":100}');   -- 1
```

All four numeric keywords can be combined:

```sql
-- 0 < value < 100 (open interval)
'{"type":"integer","exclusiveMinimum":0,"exclusiveMaximum":100}'

-- 0 ≤ value < 100 (half-open interval)
'{"type":"integer","minimum":0,"exclusiveMaximum":100}'
```

---

### `minLength` / `maxLength`

Constrain text (string) length in **UTF-8 codepoints** (not bytes).

**Applies to:** `text` type only. Silently ignored for other types.

**Value:** A non-negative integer.

```sql
SELECT msgpack_schema_validate(
  msgpack_quote('hello'),
  '{"type":"text","minLength":1,"maxLength":100}'
);  -- 1

-- Empty string fails minLength:1
SELECT msgpack_schema_validate(
  msgpack_quote(''),
  '{"type":"text","minLength":1}'
);  -- 0
```

Multi-byte UTF-8 characters count as single codepoints:

```sql
-- "café" is 4 codepoints (the é is one codepoint, two UTF-8 bytes)
SELECT msgpack_schema_validate(
  msgpack_quote('café'),
  '{"type":"text","maxLength":4}'
);  -- 1
```

---

### `items`

Specifies a schema that every element of an array must satisfy.

**Applies to:** `array` type only.

**Value:** A schema object, `true`, or `false`.

| Value | Meaning |
|-------|---------|
| `{...}` | Every element must satisfy this schema |
| `true` | No constraint on elements (same as omitting) |
| `false` | Array must be empty |

```sql
-- Every element must be an integer
SELECT msgpack_schema_validate(
  msgpack_array(1, 2, 3),
  '{"type":"array","items":{"type":"integer"}}'
);  -- 1

-- Mixed types fail
SELECT msgpack_schema_validate(
  msgpack_array(1, 'oops'),
  '{"type":"array","items":{"type":"integer"}}'
);  -- 0

-- items:false requires empty array
SELECT msgpack_schema_validate(
  msgpack_array(),
  '{"type":"array","items":false}'
);  -- 1
```

---

### `minItems` / `maxItems`

Constrain the number of elements in an array.

**Applies to:** `array` type only.

**Value:** A non-negative integer.

```sql
-- Between 1 and 10 elements
SELECT msgpack_schema_validate(
  msgpack_array(1, 2, 3),
  '{"type":"array","minItems":1,"maxItems":10}'
);  -- 1

-- Empty array fails minItems:1
SELECT msgpack_schema_validate(
  msgpack_array(),
  '{"type":"array","minItems":1}'
);  -- 0
```

---

### `properties`

Defines schemas for known keys in a map. Each key maps to a sub-schema that is
applied to the corresponding value when that key is present.

**Applies to:** `map` type only.

**Value:** An object where keys are property names and values are schemas.

Properties that are not present in the data are **not** validated (use
`required` to enforce presence).

```sql
SELECT msgpack_schema_validate(
  msgpack_object('name', 'Alice', 'age', 30),
  '{
    "type": "map",
    "properties": {
      "name": {"type": "text"},
      "age":  {"type": "integer", "minimum": 0}
    }
  }'
);  -- 1

-- 'age' is present but has wrong type → fails
SELECT msgpack_schema_validate(
  msgpack_object('name', 'Alice', 'age', 'thirty'),
  '{
    "type": "map",
    "properties": {
      "name": {"type": "text"},
      "age":  {"type": "integer"}
    }
  }'
);  -- 0
```

---

### `required`

Lists key names that must be present in the map. The value of each required key
is not validated by `required` itself — use `properties` for that.

**Applies to:** `map` type only.

**Value:** An array of text strings.

```sql
SELECT msgpack_schema_validate(
  msgpack_object('name', 'Alice'),
  '{"type":"map","required":["name","age"]}'
);  -- 0  (missing 'age')

SELECT msgpack_schema_validate(
  msgpack_object('name', 'Alice', 'age', 30),
  '{"type":"map","required":["name","age"]}'
);  -- 1
```

---

### `additionalProperties`

Controls what happens with keys that are **not** listed in `properties`.

**Applies to:** `map` type only.

**Value:** `true` (default), `false`, or a schema.

| Value | Meaning |
|-------|---------|
| `true` | Unknown keys are allowed with any value (default) |
| `false` | Unknown keys are rejected |
| `{...}` | Unknown keys must have values matching this schema |

```sql
-- Reject unknown keys
SELECT msgpack_schema_validate(
  msgpack_object('name', 'Alice', 'extra', 1),
  '{
    "type": "map",
    "properties": {"name": {"type": "text"}},
    "additionalProperties": false
  }'
);  -- 0

-- Unknown keys must be text
SELECT msgpack_schema_validate(
  msgpack_object('name', 'Alice', 'tag', 'vip'),
  '{
    "type": "map",
    "properties": {"name": {"type": "text"}},
    "additionalProperties": {"type": "text"}
  }'
);  -- 1
```

---

## Boolean schemas

The simplest schemas are literal JSON booleans:

- `true` — accepts any value (equivalent to `{}`)
- `false` — rejects every value

These are useful inside `items` and `additionalProperties`:

```sql
-- No array elements allowed
'{"type":"array","items":false}'

-- No unknown map keys allowed
'{"type":"map","properties":{...},"additionalProperties":false}'
```

---

## Composing schemas

Keywords within a single schema object are **conjunctive** (AND): every keyword
must pass for the schema to pass. This is the composition mechanism:

```sql
-- This schema requires ALL of: type=integer AND value ≥ 0 AND value ≤ 100
'{"type":"integer","minimum":0,"maximum":100}'
```

Schemas also compose through nesting. The `items`, `properties`, and
`additionalProperties` keywords all accept full sub-schemas:

```sql
-- Array of maps, each map has typed properties
'{
  "type": "array",
  "items": {
    "type": "map",
    "required": ["id", "name"],
    "properties": {
      "id":   {"type": "integer", "minimum": 1},
      "name": {"type": "text", "minLength": 1, "maxLength": 255},
      "tags": {"type": "array", "items": {"type": "text"}, "maxItems": 10}
    }
  }
}'
```

Nesting is limited to 200 levels of recursion to prevent stack overflow on
maliciously crafted schemas.

---

## Schema input formats

The `schema` parameter accepts two formats:

### JSON text (most common)

Pass the schema as a SQL text string containing JSON:

```sql
SELECT msgpack_schema_validate(data, '{"type":"integer"}');
```

The JSON is parsed to msgpack internally. When the schema argument is a
constant expression (the common case in `CHECK` constraints), the parsed result
is cached and reused for every row.

### Msgpack BLOB

Pass a pre-encoded msgpack BLOB as the schema:

```sql
SELECT msgpack_schema_validate(
  data,
  msgpack_from_json('{"type":"integer","minimum":0}')
);
```

This can be useful when schemas are stored as msgpack BLOBs in a table,
avoiding a JSON parse step entirely.

---

## Performance and caching

### Automatic caching

When the `schema` argument is a **constant expression** — which is the case for
inline string literals and `CHECK` constraints — the validator caches the
parsed msgpack schema using SQLite's `sqlite3_set_auxdata` API. On subsequent
invocations with the same constant schema:

- JSON parsing is **skipped** (zero overhead)
- Validation cost is proportional to the **data size**, not the schema size

This makes `msgpack_schema_validate` suitable for high-throughput `CHECK`
constraints without measurable overhead beyond the validation logic itself.

### Validation cost

The validator performs a single-pass traversal of both the data and the schema.
For each data element, it looks up matching schema keywords and applies
constraints. The traversal is:

- **O(n)** in data size for flat structures
- **O(n × m)** in the worst case where `n` = data elements and `m` = schema
  map key lookups (linear scan of map keys)

For typical schemas with small `properties` maps, validation is effectively
linear in data size.

---

## Common patterns

### CHECK constraints

The most common use case — enforce a data contract at the storage boundary:

```sql
CREATE TABLE events (
  id    INTEGER PRIMARY KEY,
  data  BLOB NOT NULL
    CHECK (msgpack_schema_validate(data, '{
      "type": "map",
      "required": ["event", "ts"],
      "properties": {
        "event": {"type": "text", "minLength": 1},
        "ts":    {"type": "integer", "minimum": 0},
        "meta":  {"type": "map"}
      },
      "additionalProperties": true
    }'))
);
```

### Trigger-based validation

When you need dynamic schemas or better error messages:

```sql
CREATE TRIGGER validate_events BEFORE INSERT ON events
BEGIN
  SELECT RAISE(ABORT, 'event data does not match schema')
  WHERE NOT msgpack_schema_validate(NEW.data, '{
    "type": "map",
    "required": ["event", "ts"],
    "properties": {
      "event": {"type": "text"},
      "ts":    {"type": "integer", "minimum": 0}
    }
  }');
END;
```

### Schema registry table

Store schemas centrally and reference them by name:

```sql
CREATE TABLE schemas (
  name    TEXT PRIMARY KEY,
  version INTEGER NOT NULL DEFAULT 1,
  schema  TEXT NOT NULL
);

INSERT INTO schemas VALUES ('event_v1', 1, '{
  "type": "map",
  "required": ["event", "ts"],
  "properties": {
    "event": {"type": "text"},
    "ts":    {"type": "integer", "minimum": 0}
  }
}');

-- Validate using stored schema
SELECT msgpack_schema_validate(
  data,
  (SELECT schema FROM schemas WHERE name = 'event_v1')
)
FROM events;
```

### Auditing invalid rows

Find rows that don't conform to a schema:

```sql
SELECT rowid, msgpack_to_json(data)
FROM events
WHERE NOT msgpack_schema_validate(data, '{
  "type": "map",
  "required": ["event", "ts"]
}');
```

### Migrating schemas

Validate data against a new schema version before committing to a migration:

```sql
-- Count rows that would fail the new schema
SELECT count(*)
FROM events
WHERE NOT msgpack_schema_validate(data, '{
  "type": "map",
  "required": ["event", "ts", "source"],
  "properties": {
    "event":  {"type": "text"},
    "ts":     {"type": "integer", "minimum": 0},
    "source": {"type": "text", "minLength": 1}
  }
}');
```

---

## Comparison with JSON Schema

The schema language is inspired by [JSON Schema](https://json-schema.org/) but
tailored to MessagePack. Key differences:

| Feature | JSON Schema | msgpack schema |
|---------|-------------|----------------|
| Type names | `"string"`, `"number"`, `"object"`, `"array"` | `"text"`, `"integer"`/`"real"`, `"map"`, `"array"` |
| Boolean type | `"boolean"` | `"bool"` |
| Binary data | Not native | `"blob"` type |
| Wildcard type | Not available | `"any"` |
| `pattern` | ECMA-262 regex | Not yet (Phase 2) |
| `$ref` / `$defs` | Supported | Not yet (Phase 3) |
| `anyOf`/`allOf`/`oneOf`/`not` | Supported | Not yet (Phase 2) |
| `prefixItems` | Supported | Not yet (Phase 2) |
| `uniqueItems` | Supported | Not yet (Phase 2) |
| `minProperties`/`maxProperties` | Supported | Not yet (Phase 3) |
| Error reporting | N/A (up to validator) | Not yet (Phase 2: `msgpack_schema_errors()`) |

---

## Limitations and future work

### Current limitations (Phase 1)

- **No `pattern` keyword** — regex matching for text fields is not yet
  supported.
- **No combinators** — `anyOf`, `allOf`, `oneOf`, `not` are not yet available.
  Use union types (`"type":["integer","null"]`) as a partial workaround.
- **No tuple validation** — `prefixItems` for positional array elements is not
  yet supported.
- **No `uniqueItems`** — duplicate checking within arrays is not available.
- **No `$ref`/`$defs`** — schema references for DRY schemas are not yet
  supported.
- **No error reporting** — `msgpack_schema_errors()` is planned for Phase 2.
  Currently the function only returns `0` (invalid) or `1` (valid) with no
  detail about which constraint failed.
- **No `minProperties`/`maxProperties`** — map size constraints are planned for
  Phase 3.

### Planned for Phase 2

- `pattern` keyword (regex for text)
- Schema combinators: `anyOf`, `allOf`, `oneOf`, `not`
- Tuple validation: `prefixItems`
- `uniqueItems` for arrays
- `msgpack_schema_errors()` — returns a JSON array of validation error objects

### Planned for Phase 3

- `$ref` / `$defs` for reusable schema definitions
- `minProperties` / `maxProperties`
- Performance benchmarks
