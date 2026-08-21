# Changelog

All notable changes to `sparquet-cola` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-08-21

### Added

- **Row-level failure codes.** Every check now answers `code()`: the `code` declared on
  the rule (`{"type": "range", "column": "age", "min": 1, "max": 99, "code": "AGE_RANGE"}`)
  or, when it is omitted, the validation expression itself, rendered compactly and
  **deterministically** — `not_null(email)`, `unique(id,dt)`, `range(age,1,99)`
  (`*` marks an open bound: `range(valor,0,*)`), `regex(email,^.+@.+$)`,
  `missing_percent(cpf)`. The string lands *in the data*, so the same rule always
  renders the same code. Overridden by the row-level checks (`NotNullCheck`,
  `UniqueCheck`, `RangeCheck`, `RegexCheck`, `MetricCheck`); the aggregate ones fall
  back to their `type`, since they never label a row.
- `Cola.split(df, rules, annotate=None, only=None)`:
  - `annotate` names an `array<string>` column added **to `invalid` only**, listing the
    codes of the rules that row violated, in rule-declaration order. It is built from
    the same per-rule `violation()` expressions the split already computes, so it costs
    no extra pass over the data — and it is added after the filter, so `valid` keeps the
    input schema exactly. When no row-level rule feeds the split, the (empty) `invalid`
    still carries the column, so a destination never changes shape.
  - `only` restricts the split *and* the annotation to the rules whose code is in the
    list; omitted means every row-level rule, the previous behaviour.
- `Cola.codes(rules)` — the code of each rule, in order. Lets a caller discover what it
  can scope or filter a quarantine by before running the split.

- `tests/test_split_spark.py`: integration tests that run `split` against a local
  SparkSession (the regression above cannot be caught without one). They skip cleanly
  when no working Java/Spark is available, and are wired into both GitHub Actions
  workflows.

### Unchanged

- Split semantics: a row is invalid when it violates any row-level check,
  `coalesce(cond, false)` keeps a NULL comparison from invalidating a row,
  aggregate-only rule lists leave everything valid, and the internal flag column never
  leaks into either output.

### Fixed

> Shipped as part of this release: 0.1.1 was tagged in the changelog but never
> published to PyPI, so its fix reaches users here.

- `Cola.split(df, rules)` no longer crashes when the rule list contains a `unique`
  check. `UniqueCheck.violation()` returns a window expression
  (`count(1) over (partition by ...) > 1`) and Spark forbids window functions inside a
  `WHERE` clause, so filtering by the combined predicate raised
  `[WINDOW_FUNCTION_NOT_ALLOWED_IN_CLAUSE]`. `split` now materialises the predicate as
  an internal column, filters on that column and drops it from both returned
  DataFrames — the `valid`/`invalid` schemas are identical to the input, as before.
  Row-level semantics are unchanged (a row is invalid when it violates any row-level
  check; a `NULL` comparison does not invalidate a row).


## [0.1.0] - 2026-08-16

### Added

- Initial release: the `Cola` engine (`run`, `split`, `register`, `available`) with the
  built-in checks `not_null`, `unique`, `range`, `regex`, `row_count`, `sql`, `check`
  (SODA-style metric + threshold) and `schema`, the threshold DSL (`> 0`,
  `between 10 and 20`, `< 5%`, `< 1d`), validity config (`valid_values`, `valid_format`,
  `valid_regex`, `valid_min`/`valid_max`, `valid_length`) and the valid/invalid
  quarantine split. Depends only on `pyspark`.

[0.2.0]: https://github.com/VictorPasqualini/sparquet-cola/releases/tag/v0.2.0
[0.1.1]: https://github.com/VictorPasqualini/sparquet-cola/releases/tag/v0.1.1
[0.1.0]: https://github.com/VictorPasqualini/sparquet-cola/releases/tag/v0.1.0
