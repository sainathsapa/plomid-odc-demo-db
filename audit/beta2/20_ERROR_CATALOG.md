# Beta 2 — 20 Error Catalog (Phase 20)

Date (UTC): 2026-10-09. Commit: `7acf160` + working tree.

## Catalog

Public reference: `docs/v0.1.0/reference/error-catalog.mdx` →
`docs/v0.1.0/reference/errors.mdx` (extended this phase with the full
`ErrorKind` → `PL-*` → SQLSTATE table: 15 kinds, primary codes, keyword
refinements). Stable contract:

```text
ErrorKind → PL-* (core/error.rs) → SQLSTATE (sqlstate_for) → wire error
```

## Exhaustive mapping test

`network/src/server/handler.rs::sqlstate_tests`:

- `every_error_kind_maps_to_a_documented_sqlstate` — every kind with a
  keyword-free message maps to its documented primary (pre-existing).
- `no_unexpected_internal_sqlstate` (**added this phase**) — with a neutral
  message, only `Internal` may yield `XX000`; any new kind landing in the
  `_ => "XX000"` arm fails until mapped. 7/7 sqlstate tests green
  (`cargo test -p plomid-network --lib sqlstate`).

## INTERNAL discipline

`XX000` is reserved for genuinely unexpected failures. Stable user-facing
situations (constraint violations, missing objects, privilege denials,
version skew, corruption with a known shape) all carry their own kind and
never escape as `INTERNAL`. Error semantics are frozen: remapping a code
requires updating the table + both tests.

## Status

GREEN (catalog published, exhaustive + INTERNAL-guard tests green).
