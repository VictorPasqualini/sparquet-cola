# Changelog

All notable changes to `sparquet-cola` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-08-21

### Changed

- **Every metric is a rule `type` now.** `{"type": "missing_percent", "column": "cpf",
  "must_be": "< 1%"}` — the `check` wrapper is **gone**, not deprecated. Its only job
  was to carry a `metric` field that is now the `type` itself: a level of indirection
  that decided nothing. `rule_type` on the result carries the metric too, so a report
  row says `missing_percent`, not `check`.

  Migrating is mechanical: drop `"type": "check"` and promote `metric` to `type`.

  Registered metric types: `row_count`, `distinct_count`, `missing_count`,
  `missing_percent`, `duplicate_count`, `duplicate_percent`, `invalid_count`,
  `invalid_percent`, `min`, `max`, `avg`, `mean`, `sum`, `stddev`, `freshness`.

- **`not_null`, `unique`, `range`, `regex`, `sql` and `schema` stay first-class**, and
  that is a decision backed by their semantics rather than a hesitation:
  - `regex` counts NULL as a violation (`~rlike(pattern) | isNull`), while `invalid_*`
    treats NULL as *missing* — a different metric. Folding one into the other would
    quietly stop flagging NULLs.
  - `range` labels the ROW outside the interval; the `min`/`max` metrics describe the
    column and cannot point at a row, so the quarantine would lose those rows.
  - `not_null` and `unique` do map exactly onto `missing_count = 0` and
    `duplicate_count = 0`, but they read better and report per-column counts, and are
    the names every DQ tool uses.

### Added

- **`targets`: one rule entry, many independent rules.** A rule may declare several
  targets and each becomes a rule of its own — its own `CheckResult`, its own code, its
  own contribution to the quarantine:

  ```json
  { "type": "regex", "targets": [
      { "column": "document",  "pattern": "^[0-9]{11}$" },
      { "column": "document2", "pattern": "^[0-9]{12}$" } ] }
  ```

  Keys outside `targets` are shared defaults. Independence is the whole point: one
  aggregated verdict would not say which column broke.

  Exposed as `Cola.expand(rules)` / `expand_targets(rules)` because the expansion must
  run **before** anything pairs rules with results by position — the framework's
  validation report does exactly that, and two divergent implementations would
  desynchronise it. So there is one, here.

  Every ambiguous form is refused at parse time rather than silently degraded: an empty
  target list (it would erase the validation), a `code` on the parent (every expanded
  rule would inherit the same one, making the quarantine annotation ambiguous), an
  empty target, a nested `targets`, a `type` inside a target.

- `METRIC_TYPES` is public, so a caller can enumerate the metrics.

### Fixed

- `not_null` e `unique` aceitam `column` (singular), não só `columns`. Com `targets`, a
  forma natural de declarar um alvo é `{"column": "id"}` — é o que `range` e `regex`
  usam — e o código derivado já renderizava `not_null(id)` a partir dela; só a execução
  lia `params["columns"]` cru, então a regra morria com um `KeyError: 'columns'` que não
  dizia qual regra era nem o que declarar. Passa a existir **um** leitor de colunas na
  lib (`_columns_of`), usado também pelas métricas, com erro que nomeia a regra.

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
