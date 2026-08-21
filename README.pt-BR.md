# sparquet-cola

**Biblioteca de qualidade de dados para Spark** — checks de métrica no estilo SODA, regras
SQL livres, verificação de schema e o split válidas/inválidas (quarentena), tudo em cima do
PySpark.

*Cola* é a camada que **gruda** qualidade nos seus DataFrames. Depende **apenas de
`pyspark`** — dá para usar em qualquer job Spark, notebook ou task do Airflow, com ou sem o
framework [Sparquet](https://github.com/VictorPasqualini/sparquet). Dentro do Sparquet, é o
motor por trás do bloco `validations`; o `type` das regras é idêntico, então o que você
aprende aqui vale direto no JSON do pipeline.

> 🌍 **Docs:** [English](README.md) · [Português](README.pt-BR.md) · [Español](README.es.md)

O nome de import é `sparquet_cola` (underscore, convenção Python); o pacote no PyPI é
`sparquet-cola` (hífen).

## Instalação

```bash
pip install sparquet-cola
```

```python
from sparquet_cola import Cola
```

`pyspark>=3.4.0` vem como dependência. Os nomes públicos são `Cola`, `ColaSplit`,
`CheckResult` e as classes de check individuais.

## Início rápido

Uma classe faz tudo — um registry de tipos de check com quatro membros: `run`, `split`,
`register` e `available`.

```python
from sparquet_cola import Cola

cola = Cola()

# 1) Rodar checks e ler os resultados (nunca lança em check reprovado)
for r in cola.run(df, [
    {"type": "row_count", "min": 1},
    {"type": "not_null", "columns": ["id"]},
    {"type": "missing_percent", "column": "cpf", "must_be": "< 5%", "warn": "= 0"},
    {"type": "sql", "failed_rows": "SELECT * FROM _validation_df WHERE valor < 0"},
]):
    print(r)   # [FAIL] check: missing_percent(cpf) = 8 viola must_be (< 5%)

# 2) Separar válidas de inválidas (quarentena)
split = cola.split(df, [
    {"type": "not_null", "columns": ["id"]},
    {"type": "invalid_count", "column": "email",
     "valid_format": "email", "must_be": "= 0"},
])
split.valid.write.format("delta").save(".../silver_ok")
split.invalid.write.format("delta").save(".../silver_quarentena")
```

| Membro | Assinatura | Retorna |
|---|---|---|
| `run` | `run(df, rules)` | um `CheckResult` por regra, em ordem |
| `split` | `split(df, rules, annotate=None, only=None)` | um `ColaSplit(valid, invalid)` de dois DataFrames |
| `codes` | `codes(rules)` | o código de cada regra, em ordem |
| `register` | `register(name, cls)` | registra um check customizado sob um `type` |
| `available` | propriedade | lista ordenada dos tipos de check registrados |

Uma **regra** é um dict simples com a chave `type` mais os parâmetros do check. Um
`CheckResult` carrega `rule_type`, `passed`, `severity` (`pass`/`warn`/`fail`), `message`,
`failed_count`, `metric_value`, `check_name` e `failed_rows` (um DataFrame, para checks `sql`
em modo failed-rows).

`run` só lança para uma regra genuinamente malformada (`type` desconhecido, parâmetro
obrigatório ausente, threshold ou métrica inválidos) — um check *reprovado* é um resultado
retornado, não uma exceção.

## Os checks

| Tipo | O que faz | Row-level? |
|---|---|---|
| `not_null` | reprova quando uma coluna listada tem NULL | sim |
| `unique` | reprova quando a tupla de colunas não é única | sim |
| `range` | coluna numérica/data fora do intervalo inclusivo `[min, max]` | sim |
| `regex` | coluna string que não casa com um padrão (`rlike`) | sim |
| `row_count` | guarda no tamanho do DataFrame (`min`/`max`) | não |
| `sql` | SQL livre sobre a temp view `_validation_df` — modo invariante (`query`) ou `failed_rows` | não |
| *qualquer métrica* | `missing_percent`, `duplicate_count`, `avg`, `freshness`… — a métrica É o tipo, comparada a um **threshold** com níveis `warn`/`fail` | para `missing_*`/`invalid_*` |
| `schema` | colunas obrigatórias/proibidas e tipos esperados (data contract básico) | não |

Checks *row-level* alimentam o split válidas/inválidas; checks agregados não.

### Códigos de falha por linha

Uma tabela de quarentena que não diz **qual regra** rejeitou cada linha não permite
agir. Por isso toda regra tem um **código**: o que você declara ou — quando você omite —
a própria expressão da validação, renderizada de forma compacta e determinística (a
mesma regra sempre gera a mesma string, porque ela vai para dentro dos seus dados).

| Regra | Código |
|---|---|
| `{"type": "range", "column": "idade", "min": 1, "max": 99, "code": "AGE_RANGE"}` | `AGE_RANGE` |
| `{"type": "not_null", "columns": ["email"]}` | `not_null(email)` |
| `{"type": "unique", "columns": ["id", "dt"]}` | `unique(id,dt)` |
| `{"type": "range", "column": "idade", "min": 1, "max": 99}` | `range(idade,1,99)` |
| `{"type": "range", "column": "valor", "min": 0}` | `range(valor,0,*)` (`*` = sem limite) |
| `{"type": "regex", "column": "email", "pattern": "^.+@.+$"}` | `regex(email,^.+@.+$)` |
| `{"type": "missing_percent", "column": "cpf", ...}` | `missing_percent(cpf)` |

O `split` então grava esses códigos ao lado das linhas rejeitadas, e pode ser restrito a
um subconjunto das regras:

```python
split = cola.split(df, rules, annotate="dq_codes", only=["AGE_RANGE", "not_null(email)"])
split.invalid.select("id", "dq_codes").show(truncate=False)
```

`annotate` acrescenta uma coluna `array<string>` **somente ao `invalid`** — no `valid`
ela seria vazia por definição — montada a partir dos mesmos predicados que o split já
calcula, então não custa uma passada extra. `only` restringe o split e a anotação às
regras cujo código está na lista; omitido, todas as regras row-level participam.

### Uma regra, vários alvos

Uma regra pode declarar vários alvos, e cada um vira uma regra própria — com seu
resultado, seu código e sua contribuição à quarentena:

```python
{"type": "regex", "targets": [
    {"column": "document",  "pattern": "^[0-9]{11}$"},
    {"column": "document2", "pattern": "^[0-9]{12}$"}]}
```

Chaves fora de `targets` são defaults compartilhados: `{"type": "range", "min": 0,
"targets": [{"column": "a"}, {"column": "b", "max": 9}]}` limita as duas colunas por
baixo e só uma por cima.

A independência é o ponto: um veredito agregado não diria qual coluna quebrou. Toda
forma ambígua é recusada no parse em vez de degradada em silêncio — lista de alvos
vazia, `code` no nível do pai (todas as regras expandidas herdariam o mesmo e a
anotação da quarentena deixaria de ser decidível), `targets` aninhado.

### Métricas e o DSL de threshold

O `check` mede uma métrica e compara com um threshold:

```python
{"type": "missing_percent", "name": "completude do cpf", "column": "cpf", "must_be": "< 1%", "warn": "= 0"}
```

Métricas: `row_count`, `distinct_count`, `missing_count`/`missing_percent`,
`duplicate_count`/`duplicate_percent`, `invalid_count`/`invalid_percent`,
`min`/`max`/`avg`/`sum`/`stddev`, `freshness`.

DSL de threshold (usado em `must_be` e no `warn`, mais brando):

| Forma | Exemplo |
|---|---|
| comparação | `> 0`, `< 5`, `>= 100`, `= 0`, `!= 0` |
| intervalo | `between 10 and 20`, `not between 1 and 2` |
| sufixo percentual | `< 5%` (o `%` é cosmético) |
| sufixo de duração | `< 1d`, `<= 2h`, `> 30m` (para `freshness`; unidades `s`/`m`/`h`/`d`/`w`) |

Para `invalid_*`, a validade é configurada com `valid_values` / `invalid_values` /
`valid_format` / `valid_regex` / `valid_min` / `valid_max` / `valid_length` (e
`min/max_length`). Valores nomeados de `valid_format` incluem `email`, `uuid`, `cpf`,
`cnpj`, `date`, `url`, `ip` e outros. Uma violação de `warn` é logada e reportada, mas
**não** é uma falha.

## Checks customizados

Herde de `BaseCheck`, implemente `run(df) -> CheckResult` e, opcionalmente, `violation(df)`
(uma `Column` booleana do Spark, `True` para linhas ofensoras) para entrar no split.
Registre sob um `type`:

```python
from pyspark.sql import functions as F
from sparquet_cola import Cola
from sparquet_cola.checks import BaseCheck, CheckResult

class NoFutureDateCheck(BaseCheck):
    def run(self, df):
        column = self.params["column"]
        failed = df.filter(F.col(column) > F.current_date()).count()
        if failed:
            return CheckResult("no_future_date", False, f"{failed} datas futuras", failed)
        return CheckResult("no_future_date", True)

    def violation(self, df):
        return F.col(self.params["column"]) > F.current_date()

cola = Cola()
cola.register("no_future_date", NoFutureDateCheck)
cola.run(df, [{"type": "no_future_date", "column": "ordered_at"}])
```

## Dentro do Sparquet

O bloco `validations` de um JSON de pipeline do
[Sparquet](https://github.com/VictorPasqualini/sparquet) roda exatamente neste motor — os
mesmos `type` de regra, o mesmo DSL de threshold e config de validade. O framework adiciona
persistência de relatório, a política `on_failure` e a quarentena row-level via
`validations.outputs`.

## Desenvolvimento

```bash
pip install -e .
PYTHONPATH=. python tests/test_cola_lib.py    # testes unitários puros, sem Java
PYTHONPATH=. python tests/test_split_spark.py # integração do split; pula sem Java
```

A publicação no PyPI é automatizada via GitHub Actions — ver
[docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md).
As mudanças de cada versão estão em [CHANGELOG.md](CHANGELOG.md).

## Licença

Apache License 2.0 — ver [LICENSE](LICENSE) e [NOTICE](NOTICE).
