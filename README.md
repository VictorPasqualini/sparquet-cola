# sparquet-cola

**Spark data-quality library** — SODA-style metric checks, free-form SQL rules, schema
assertions and a valid/invalid split (quarantine), all on top of PySpark.

*Cola* ("glue" in Portuguese) is the layer that **glues** quality onto your DataFrames.
It depends **only on `pyspark`** — drop it into any Spark job, notebook or Airflow task,
with or without the [Sparquet](https://github.com/VictorPasqualini/sparquet) framework.
Inside Sparquet it is the engine behind the `validations` block; the rule `type` strings
are identical, so what you learn here transfers directly to the pipeline JSON.

> 🌍 **Docs:** [English](README.md) · [Português](README.pt-BR.md) · [Español](README.es.md)

The import name is `sparquet_cola` (underscore, Python convention); the PyPI package is
`sparquet-cola` (hyphen).

## Install

```bash
pip install sparquet-cola
```

```python
from sparquet_cola import Cola
```

`pyspark>=3.4.0` comes as a dependency. The public names are `Cola`, `ColaSplit`,
`CheckResult`, and the individual check classes.

## Quickstart

One class does everything — a registry of check types with four members: `run`, `split`,
`register` and `available`.

```python
from sparquet_cola import Cola

cola = Cola()

# 1) Run checks and read the results (never raises on a failed check)
for r in cola.run(df, [
    {"type": "row_count", "min": 1},
    {"type": "not_null", "columns": ["id"]},
    {"type": "check", "metric": "missing_percent", "column": "cpf", "must_be": "< 5%", "warn": "= 0"},
    {"type": "sql", "failed_rows": "SELECT * FROM _validation_df WHERE amount < 0"},
]):
    print(r)   # [FAIL] check: missing_percent(cpf) = 8 violates must_be (< 5%)

# 2) Split valid from invalid (quarantine)
split = cola.split(df, [
    {"type": "not_null", "columns": ["id"]},
    {"type": "check", "metric": "invalid_count", "column": "email",
     "valid_format": "email", "must_be": "= 0"},
])
split.valid.write.format("delta").save(".../silver_ok")
split.invalid.write.format("delta").save(".../silver_quarantine")
```

| Member | Signature | Returns |
|---|---|---|
| `run` | `run(df, rules)` | a `CheckResult` per rule, in order |
| `split` | `split(df, rules)` | a `ColaSplit(valid, invalid)` of two DataFrames |
| `register` | `register(name, cls)` | registers a custom check under a `type` |
| `available` | property | sorted list of registered check types |

A **rule** is a plain dict with a `type` key plus that check's parameters. A `CheckResult`
carries `rule_type`, `passed`, `severity` (`pass`/`warn`/`fail`), `message`, `failed_count`,
`metric_value`, `check_name` and `failed_rows` (a DataFrame, for `sql` failed-rows checks).

`run` only raises for a genuinely malformed rule (unknown `type`, a missing required
parameter, an invalid threshold or metric name) — a *failed* check is a returned result,
not an exception.

## The checks

| Type | What it does | Row-level? |
|---|---|---|
| `not_null` | fails when a listed column contains NULL | yes |
| `unique` | fails when the tuple of columns is not unique | yes |
| `range` | numeric/date column outside inclusive `[min, max]` | yes |
| `regex` | string column not matching a pattern (`rlike`) | yes |
| `row_count` | guard on the DataFrame size (`min`/`max`) | no |
| `sql` | free-form SQL over the temp view `_validation_df` — invariant (`query`) or `failed_rows` mode | no |
| `check` | a **metric** compared to a **threshold**, SODA-style, with `warn`/`fail` levels | for `missing_*`/`invalid_*` |
| `schema` | required/forbidden columns and expected types (a basic data contract) | no |

*Row-level* checks feed the valid/invalid split; aggregate checks don't.

### `check` metrics and threshold DSL

`check` measures one metric and compares it to a threshold:

```python
{"type": "check", "name": "cpf completeness",
 "metric": "missing_percent", "column": "cpf", "must_be": "< 1%", "warn": "= 0"}
```

Metrics: `row_count`, `distinct_count`, `missing_count`/`missing_percent`,
`duplicate_count`/`duplicate_percent`, `invalid_count`/`invalid_percent`,
`min`/`max`/`avg`/`sum`/`stddev`, `freshness`.

Threshold DSL (used in `must_be` and the softer `warn`):

| Form | Example |
|---|---|
| comparison | `> 0`, `< 5`, `>= 100`, `= 0`, `!= 0` |
| range | `between 10 and 20`, `not between 1 and 2` |
| percent suffix | `< 5%` (the `%` is cosmetic) |
| duration suffix | `< 1d`, `<= 2h`, `> 30m` (for `freshness`; units `s`/`m`/`h`/`d`/`w`) |

For `invalid_*`, validity is configured with `valid_values` / `invalid_values` /
`valid_format` / `valid_regex` / `valid_min` / `valid_max` / `valid_length` (and
`min/max_length`). Named `valid_format` values include `email`, `uuid`, `cpf`, `cnpj`,
`date`, `url`, `ip`, and more. A `warn` breach is logged and reported but is **not** a
failure.

## Custom checks

Subclass `BaseCheck`, implement `run(df) -> CheckResult`, and optionally `violation(df)`
(a boolean Spark `Column`, `True` for offending rows) to join the split. Register it under
a `type`:

```python
from pyspark.sql import functions as F
from sparquet_cola import Cola
from sparquet_cola.checks import BaseCheck, CheckResult

class NoFutureDateCheck(BaseCheck):
    def run(self, df):
        column = self.params["column"]
        failed = df.filter(F.col(column) > F.current_date()).count()
        if failed:
            return CheckResult("no_future_date", False, f"{failed} future dates", failed)
        return CheckResult("no_future_date", True)

    def violation(self, df):
        return F.col(self.params["column"]) > F.current_date()

cola = Cola()
cola.register("no_future_date", NoFutureDateCheck)
cola.run(df, [{"type": "no_future_date", "column": "ordered_at"}])
```

## Inside Sparquet

The `validations` block of a [Sparquet](https://github.com/VictorPasqualini/sparquet)
pipeline JSON runs on exactly this engine — the same rule `type` strings, threshold DSL and
validity config. The framework adds report persistence, the `on_failure` policy and
row-level quarantine via `validations.outputs`.

## Development

```bash
pip install -e .
PYTHONPATH=. python tests/test_cola_lib.py    # pure unit tests, no Java needed
```

Releasing to PyPI is automated via GitHub Actions — see [docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
