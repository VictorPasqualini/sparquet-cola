# sparquet-cola

**Librería de calidad de datos para Spark** — checks de métrica al estilo SODA, reglas SQL
libres, verificación de schema y el split válidas/inválidas (cuarentena), todo sobre PySpark.

*Cola* ("pegamento" en portugués) es la capa que **pega** calidad a tus DataFrames. Depende
**solo de `pyspark`** — úsala en cualquier job Spark, notebook o task de Airflow, con o sin
el framework [Sparquet](https://github.com/VictorPasqualini/sparquet). Dentro de Sparquet es
el motor detrás del bloque `validations`; los `type` de las reglas son idénticos, así que lo
que aprendes aquí se traslada directo al JSON del pipeline.

> 🌍 **Docs:** [English](README.md) · [Português](README.pt-BR.md) · [Español](README.es.md)

El nombre de import es `sparquet_cola` (guion bajo, convención Python); el paquete en PyPI es
`sparquet-cola` (guion).

## Instalación

```bash
pip install sparquet-cola
```

```python
from sparquet_cola import Cola
```

`pyspark>=3.4.0` viene como dependencia. Los nombres públicos son `Cola`, `ColaSplit`,
`CheckResult` y las clases de check individuales.

## Inicio rápido

Una clase hace todo — un registry de tipos de check con cuatro miembros: `run`, `split`,
`register` y `available`.

```python
from sparquet_cola import Cola

cola = Cola()

# 1) Correr checks y leer los resultados (nunca lanza en un check reprobado)
for r in cola.run(df, [
    {"type": "row_count", "min": 1},
    {"type": "not_null", "columns": ["id"]},
    {"type": "check", "metric": "missing_percent", "column": "cpf", "must_be": "< 5%", "warn": "= 0"},
    {"type": "sql", "failed_rows": "SELECT * FROM _validation_df WHERE amount < 0"},
]):
    print(r)   # [FAIL] check: missing_percent(cpf) = 8 viola must_be (< 5%)

# 2) Separar válidas de inválidas (cuarentena)
split = cola.split(df, [
    {"type": "not_null", "columns": ["id"]},
    {"type": "check", "metric": "invalid_count", "column": "email",
     "valid_format": "email", "must_be": "= 0"},
])
split.valid.write.format("delta").save(".../silver_ok")
split.invalid.write.format("delta").save(".../silver_cuarentena")
```

| Miembro | Firma | Devuelve |
|---|---|---|
| `run` | `run(df, rules)` | un `CheckResult` por regla, en orden |
| `split` | `split(df, rules)` | un `ColaSplit(valid, invalid)` de dos DataFrames |
| `register` | `register(name, cls)` | registra un check personalizado bajo un `type` |
| `available` | propiedad | lista ordenada de los tipos de check registrados |

Una **regla** es un dict simple con la clave `type` más los parámetros del check. Un
`CheckResult` lleva `rule_type`, `passed`, `severity` (`pass`/`warn`/`fail`), `message`,
`failed_count`, `metric_value`, `check_name` y `failed_rows` (un DataFrame, para checks `sql`
en modo failed-rows).

`run` solo lanza para una regla genuinamente malformada (`type` desconocido, un parámetro
obligatorio ausente, un threshold o métrica inválidos) — un check *reprobado* es un
resultado devuelto, no una excepción.

## Los checks

| Tipo | Qué hace | ¿Row-level? |
|---|---|---|
| `not_null` | falla cuando una columna listada tiene NULL | sí |
| `unique` | falla cuando la tupla de columnas no es única | sí |
| `range` | columna numérica/fecha fuera del intervalo inclusivo `[min, max]` | sí |
| `regex` | columna string que no coincide con un patrón (`rlike`) | sí |
| `row_count` | guarda sobre el tamaño del DataFrame (`min`/`max`) | no |
| `sql` | SQL libre sobre la temp view `_validation_df` — modo invariante (`query`) o `failed_rows` | no |
| `check` | una **métrica** comparada con un **threshold**, estilo SODA, con niveles `warn`/`fail` | para `missing_*`/`invalid_*` |
| `schema` | columnas obligatorias/prohibidas y tipos esperados (data contract básico) | no |

Los checks *row-level* alimentan el split válidas/inválidas; los agregados no.

### Métricas del `check` y el DSL de threshold

El `check` mide una métrica y la compara con un threshold:

```python
{"type": "check", "name": "completitud del cpf",
 "metric": "missing_percent", "column": "cpf", "must_be": "< 1%", "warn": "= 0"}
```

Métricas: `row_count`, `distinct_count`, `missing_count`/`missing_percent`,
`duplicate_count`/`duplicate_percent`, `invalid_count`/`invalid_percent`,
`min`/`max`/`avg`/`sum`/`stddev`, `freshness`.

DSL de threshold (usado en `must_be` y en el `warn`, más suave):

| Forma | Ejemplo |
|---|---|
| comparación | `> 0`, `< 5`, `>= 100`, `= 0`, `!= 0` |
| intervalo | `between 10 and 20`, `not between 1 and 2` |
| sufijo porcentual | `< 5%` (el `%` es cosmético) |
| sufijo de duración | `< 1d`, `<= 2h`, `> 30m` (para `freshness`; unidades `s`/`m`/`h`/`d`/`w`) |

Para `invalid_*`, la validez se configura con `valid_values` / `invalid_values` /
`valid_format` / `valid_regex` / `valid_min` / `valid_max` / `valid_length` (y
`min/max_length`). Valores nombrados de `valid_format` incluyen `email`, `uuid`, `cpf`,
`cnpj`, `date`, `url`, `ip` y más. Una violación de `warn` se loguea y reporta, pero **no**
es una falla.

## Checks personalizados

Hereda de `BaseCheck`, implementa `run(df) -> CheckResult` y, opcionalmente, `violation(df)`
(una `Column` booleana de Spark, `True` para filas ofensoras) para entrar al split.
Regístralo bajo un `type`:

```python
from pyspark.sql import functions as F
from sparquet_cola import Cola
from sparquet_cola.checks import BaseCheck, CheckResult

class NoFutureDateCheck(BaseCheck):
    def run(self, df):
        column = self.params["column"]
        failed = df.filter(F.col(column) > F.current_date()).count()
        if failed:
            return CheckResult("no_future_date", False, f"{failed} fechas futuras", failed)
        return CheckResult("no_future_date", True)

    def violation(self, df):
        return F.col(self.params["column"]) > F.current_date()

cola = Cola()
cola.register("no_future_date", NoFutureDateCheck)
cola.run(df, [{"type": "no_future_date", "column": "ordered_at"}])
```

## Dentro de Sparquet

El bloque `validations` de un JSON de pipeline de
[Sparquet](https://github.com/VictorPasqualini/sparquet) corre exactamente en este motor —
los mismos `type` de regla, el mismo DSL de threshold y config de validez. El framework
añade persistencia de reporte, la política `on_failure` y la cuarentena row-level vía
`validations.outputs`.

## Desarrollo

```bash
pip install -e .
PYTHONPATH=. python tests/test_cola_lib.py    # tests unitarios puros, sin Java
```

La publicación en PyPI está automatizada vía GitHub Actions — ver
[docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md).

## Licencia

Apache License 2.0 — ver [LICENSE](LICENSE) y [NOTICE](NOTICE).
