# Proposal: `msgpack_schema_validate` — Schema Validation for sqlite-msgpack

**Date:** 2026-04-10  
**Status:** Draft  
**Author:** Affan Khan

---

## Summary

Add a `msgpack_schema_validate()` SQL function to **sqlite-msgpack** that validates a MessagePack BLOB against a user-defined schema. The schema itself is expressed as a msgpack (or JSON) map using a compact, SQLite-friendly dialect inspired by JSON Schema but tailored to MessagePack's type system.

This closes the gap between "is this well-formed msgpack?" (`msgpack_valid`) and "does this msgpack conform to my application's data contract?"

---

## Motivation

Today the extension provides structural validation via `msgpack_valid()`, which answers only one question: *is the byte stream legal MessagePack?* Applications that store heterogeneous msgpack BLOBs in a column have no in-database way to enforce shape constraints such as:

- "the root must be a map with keys `name` (text) and `age` (integer)"
- "the `tags` field must be an array of text values with at most 10 elements"
- "the `config` map is optional, but when present it must contain a `timeout` integer"

Without schema validation, these rules must be enforced in application code, which is:

1. **Error-prone** — every writer must reimplement the same checks.
2. **Late-detected** — bad data is only caught at read time, not at insert/update time.
3. **Invisible** — constraints aren't expressed in the database definition, making the schema implicit.

A built-in schema validator enables `CHECK` constraints, triggers, and ad-hoc queries that keep data correct at the storage boundary.

---

## Proposed API

### Core function

```sql
msgpack_schema_validate(mp, schema)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `mp` | BLOB | MessagePack value to validate |
| `schema` | TEXT or BLOB | Schema definition — either a JSON text string or a msgpack BLOB |

**Returns:** `1` if `mp` conforms to `schema`, `0` otherwise.

### Error detail function

```sql
msgpack_schema_errors(mp, schema)
```

Returns a JSON text string describing all validation errors, or `NULL` if valid. Format:

```json
[
  {"path":"$.age","error":"expected type 'integer', got 'text'"},
  {"path":"$.tags[3]","error":"array length 15 exceeds maxItems 10"}
]
```

---

## Schema Language

The schema language is a minimal, MessagePack-native dialect. Schemas are ordinary msgpack maps (or JSON objects that get converted internally). The design follows three principles:

1. **Small surface area** — only the keywords needed for MessagePack's type system.
2. **Composable** — schemas nest naturally via `properties`, `items`, and combinators.
3. **Familiar** — names and semantics mirror JSON Schema (draft 2020-12) where possible.

### Type keywords

| Keyword | Value | Meaning |
|---------|-------|---------|
| `type` | text or array of text | Required type(s). Values: `"null"`, `"bool"`, `"integer"`, `"real"`, `"text"`, `"blob"`, `"array"`, `"map"`, `"any"` |

```sql
-- Schema: root must be an integer
SELECT msgpack_schema_validate(
  msgpack_quote(42),
  '{"type":"integer"}'
);  -- 1

SELECT msgpack_schema_validate(
  msgpack_quote('hello'),
  '{"type":"integer"}'
);  -- 0
```

Multiple types (union):

```sql
-- Accept integer or null
SELECT msgpack_schema_validate(
  msgpack_quote(NULL),
  '{"type":["integer","null"]}'
);  -- 1
```

### Numeric constraints

| Keyword | Applies to | Meaning |
|---------|------------|---------|
| `minimum` | integer, real | value ≥ minimum |
| `maximum` | integer, real | value ≤ maximum |
| `exclusiveMinimum` | integer, real | value > exclusiveMinimum |
| `exclusiveMaximum` | integer, real | value < exclusiveMaximum |

```sql
-- Age must be 0–150
SELECT msgpack_schema_validate(
  msgpack_quote(25),
  '{"type":"integer","minimum":0,"maximum":150}'
);  -- 1
```

### Text constraints

| Keyword | Meaning |
|---------|---------|
| `minLength` | codepoint count ≥ value |
| `maxLength` | codepoint count ≤ value |
| `pattern` | POSIX-ish regex (uses `sqlite3_strlike` or a small regex engine) |

```sql
SELECT msgpack_schema_validate(
  msgpack_quote('hello'),
  '{"type":"text","minLength":1,"maxLength":100}'
);  -- 1
```

### Array constraints

| Keyword | Meaning |
|---------|---------|
| `items` | Schema that every element must satisfy |
| `minItems` | array length ≥ value |
| `maxItems` | array length ≤ value |
| `uniqueItems` | if `true`, all elements must be byte-distinct |

```sql
-- Array of integers, 1–10 elements
SELECT msgpack_schema_validate(
  msgpack_array(1, 2, 3),
  '{"type":"array","items":{"type":"integer"},"minItems":1,"maxItems":10}'
);  -- 1
```

### Tuple validation (positional arrays)

| Keyword | Meaning |
|---------|---------|
| `prefixItems` | Array of schemas, one per positional element |
| `items` | Schema for any additional elements beyond `prefixItems` |

```sql
-- Tuple: [text, integer]
SELECT msgpack_schema_validate(
  msgpack_array('Alice', 30),
  '{"type":"array","prefixItems":[{"type":"text"},{"type":"integer"}],"items":false}'
);  -- 1
```

When `"items": false`, no additional elements are allowed beyond the prefix.

### Map constraints

| Keyword | Meaning |
|---------|---------|
| `properties` | Map of `key → schema` for known keys |
| `required` | Array of key names that must be present |
| `additionalProperties` | `true` (default), `false`, or a schema for unknown keys |
| `minProperties` | key count ≥ value |
| `maxProperties` | key count ≤ value |

```sql
-- User object schema
SELECT msgpack_schema_validate(
  msgpack_object('name', 'Alice', 'age', 30),
  '{
    "type": "map",
    "required": ["name", "age"],
    "properties": {
      "name": {"type": "text", "minLength": 1},
      "age":  {"type": "integer", "minimum": 0}
    },
    "additionalProperties": false
  }'
);  -- 1
```

### Schema combinators

| Keyword | Meaning |
|---------|---------|
| `anyOf` | Valid if at least one sub-schema matches |
| `allOf` | Valid if all sub-schemas match |
| `oneOf` | Valid if exactly one sub-schema matches |
| `not` | Valid if the sub-schema does **not** match |

```sql
-- Accept integer or text
SELECT msgpack_schema_validate(
  msgpack_quote('hello'),
  '{"anyOf":[{"type":"integer"},{"type":"text"}]}'
);  -- 1
```

### Const and enum

| Keyword | Meaning |
|---------|---------|
| `const` | Value must equal this exactly |
| `enum` | Value must be one of these |

```sql
-- Status must be one of three values
SELECT msgpack_schema_validate(
  msgpack_quote('active'),
  '{"enum":["active","inactive","pending"]}'
);  -- 1
```

### Schema references (optional / phase 2)

| Keyword | Meaning |
|---------|---------|
| `$ref` | Path-based reference to a named sub-schema in `$defs` |
| `$defs` | Map of reusable schema definitions |

```sql
SELECT msgpack_schema_validate(
  msgpack_object('home', msgpack_object('street','Main','zip','12345'),
                 'work', msgpack_object('street','Elm','zip','67890')),
  '{
    "$defs": {
      "address": {
        "type": "map",
        "required": ["street", "zip"],
        "properties": {
          "street": {"type": "text"},
          "zip": {"type": "text", "minLength": 5, "maxLength": 5}
        }
      }
    },
    "type": "map",
    "properties": {
      "home": {"$ref": "#/$defs/address"},
      "work": {"$ref": "#/$defs/address"}
    }
  }'
);  -- 1
```

---

## Usage Patterns

### CHECK constraints on table columns

```sql
CREATE TABLE events (
  id    INTEGER PRIMARY KEY,
  data  BLOB NOT NULL
    CHECK (msgpack_schema_validate(data, '{
      "type": "map",
      "required": ["event", "ts"],
      "properties": {
        "event": {"type": "text"},
        "ts":    {"type": "integer", "minimum": 0},
        "meta":  {"type": "map"}
      }
    }'))
);

-- Succeeds
INSERT INTO events(data) VALUES (
  msgpack_object('event', 'click', 'ts', 1712345678)
);

-- Fails CHECK constraint — missing 'ts'
INSERT INTO events(data) VALUES (
  msgpack_object('event', 'click')
);
```

### Stored schemas in a lookup table

```sql
CREATE TABLE schemas (
  name   TEXT PRIMARY KEY,
  schema TEXT NOT NULL
);

INSERT INTO schemas VALUES ('user_v1', '{
  "type": "map",
  "required": ["name", "email"],
  "properties": {
    "name":  {"type": "text", "minLength": 1},
    "email": {"type": "text"},
    "age":   {"type": "integer", "minimum": 0, "maximum": 150}
  }
}');

-- Validate on insert via trigger
CREATE TRIGGER validate_users BEFORE INSERT ON users
BEGIN
  SELECT RAISE(ABORT, 'schema validation failed')
  WHERE NOT msgpack_schema_validate(
    NEW.profile,
    (SELECT schema FROM schemas WHERE name = 'user_v1')
  );
END;
```

### Debugging invalid data

```sql
SELECT msgpack_schema_errors(data, '{"type":"map","required":["name"]}')
FROM users
WHERE NOT msgpack_schema_validate(data, '{"type":"map","required":["name"]}');

-- Returns:
-- [{"path":"$","error":"missing required key 'name'"}]
```

---

## Implementation Plan

### Phase 1 — Core Validator (MVP)

| Component | Details |
|-----------|---------|
| Schema parser | Accept JSON text or msgpack BLOB, build an internal schema tree |
| Type checker | `type`, `const`, `enum` |
| Numeric constraints | `minimum`, `maximum`, `exclusiveMinimum`, `exclusiveMaximum` |
| Text constraints | `minLength`, `maxLength` |
| Array constraints | `items`, `minItems`, `maxItems` |
| Map constraints | `properties`, `required`, `additionalProperties` |
| SQL functions | `msgpack_schema_validate(mp, schema)` returning 0/1 |
| Tests | Extend `test_msgpack.c` with schema validation test suite |

### Phase 2 — Extended Validation

| Component | Details |
|-----------|---------|
| `pattern` | Regex support for text fields |
| Combinators | `anyOf`, `allOf`, `oneOf`, `not` |
| Tuple validation | `prefixItems` |
| `uniqueItems` | Byte-level dedup check for arrays |
| `msgpack_schema_errors()` | Detailed error reporting function |

### Phase 3 — Reuse & Performance

| Component | Details |
|-----------|---------|
| `$ref` / `$defs` | Schema references for DRY schemas |
| Schema caching | Cache parsed schema trees across calls with same schema text (using `sqlite3_get_auxdata` / `sqlite3_set_auxdata`) |
| `minProperties` / `maxProperties` | Map size constraints |
| Benchmarks | Validate overhead vs. raw `msgpack_valid()` |

---

## Design Decisions

### Why not reuse JSON Schema directly?

JSON Schema is large (50+ keywords) and assumes a JSON type system. MessagePack has types that JSON lacks (`bin`, `ext`) and lacks JSON's implicit string-only keys. A purpose-built dialect keeps the code small and the semantics precise, while reusing familiar naming from JSON Schema so the learning curve is minimal.

### Why accept schemas as JSON text?

Schemas are typically static constants in SQL statements. JSON text is readable and editable inline. Internally, JSON text schemas are parsed once and can be cached via `sqlite3_set_auxdata` when the schema argument is a constant expression—so there is no per-row parsing overhead.

### Why return 0/1 instead of raising an error?

Returning a boolean makes the function composable with `CHECK`, `WHERE`, `CASE`, and `IIF`. For error details, `msgpack_schema_errors()` provides structured diagnostics without forcing error-path control flow.

### Schema caching strategy

When `msgpack_schema_validate(mp, schema)` is called with a constant `schema` argument (which is the common case in `CHECK` constraints and triggers), the parsed schema tree is cached using SQLite's auxiliary data API. Subsequent rows reuse the cached tree, making validation cost proportional to the data size, not the schema size.

---

## Compatibility

- **SQLite version:** No new SQLite APIs required beyond what the extension already uses.
- **Existing functions:** No changes to any existing `msgpack_*` function.
- **Build system:** New source files added to the existing CMake build; no new dependencies.
- **Extension loading:** The new functions register alongside existing ones in the same `sqlite3_extension_init` entry point.

---

## Open Questions

1. **`pattern` engine:** Should we use SQLite's built-in `LIKE`/`GLOB`, a minimal regex engine, or PCRE2? Trade-off: footprint vs. expressiveness.
2. **Recursive schemas:** Should `$ref` support recursive definitions (e.g., tree-shaped data)? If so, we need a depth limit to prevent stack overflow.
3. **`ext` type validation:** MessagePack's `ext` type carries a type code (`0–127`). Should schemas be able to constrain the ext type code?
4. **Error output format:** Is JSON-array-of-objects the right format for `msgpack_schema_errors`, or should it return a msgpack BLOB for consistency?
5. **Naming:** `msgpack_schema_validate` vs. `msgpack_validate_schema` vs. `msgpack_conforms` — preferences?
