"""Testes de integração do split valid/invalid (quarentena) com SparkSession real.

Diferente de `test_cola_lib.py` (puro, sem Spark), este arquivo executa `Cola.split`
de ponta a ponta — é o único jeito de cobrir o caminho do `unique`, cuja violação é
uma **window function** (`count() over (partition by ...)`). Filtrar direto por esse
predicado é proibido pelo Spark ([WINDOW_FUNCTION_NOT_ALLOWED_IN_CLAUSE]); o split
materializa a flag como coluna antes de filtrar, e é exatamente isso que os testes
abaixo travam (regressão do bug do split com regra `unique`).

Cobre também a quarentena ROTULADA: `split(..., annotate=...)` acrescenta um
`array<string>` com o código das regras que cada linha violou (só no lado inválido) e
`only=[...]` restringe o split a um subconjunto de regras. Só um Spark real prova que
a coluna sai com os códigos certos, que ela não aparece no `valid` e que a coluna
auxiliar do split continua não vazando.

Roda com `python tests/test_split_spark.py` (ou pytest). Se não houver pyspark ou um
Java/Spark funcional (CI sem JDK, por exemplo), a classe é **pulada** — nunca falha.
"""
from __future__ import annotations

import unittest

try:  # pyspark pode não estar instalado no ambiente de testes puros
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - ambiente sem pyspark
    SparkSession = None  # type: ignore[assignment]

from sparquet_cola import Cola


class SparkTestCase(unittest.TestCase):
    """SparkSession local compartilhada — e o skip limpo quando não há Java."""

    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        if SparkSession is None:
            raise unittest.SkipTest("pyspark não instalado")
        try:
            cls.spark = (
                SparkSession.builder
                .master("local[1]")
                .appName("sparquet-cola-split-tests")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
            # força subir a JVM: sem Java o erro aparece aqui, e viramos skip.
            cls.spark.createDataFrame([(1,)], "probe int").count()
        except Exception as exc:  # pragma: no cover - ambiente sem Java/Spark
            cls.spark = None
            raise unittest.SkipTest(f"Spark/Java indisponível: {exc}")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.spark is not None:
            cls.spark.stop()
            cls.spark = None


class SplitWithSparkTest(SparkTestCase):
    """Split real: contagens por regra e schema de saída sem coluna auxiliar."""

    def _df(self):
        # id "A" duplicado (2 linhas), "B" único, uma linha com id nulo.
        return self.spark.createDataFrame(
            [("A", 10), ("A", 20), ("B", 30), (None, 40)],
            "id string, valor int",
        )

    def _assert_schema_preserved(self, df, split):
        """Nenhuma coluna auxiliar pode vazar para as saídas (vão direto pro destino)."""
        self.assertEqual(split.valid.columns, df.columns)
        self.assertEqual(split.invalid.columns, df.columns)

    def test_split_with_unique_rule(self):
        """Regressão: window function no predicado não pode quebrar o filtro."""
        df = self._df()
        split = Cola().split(df, [{"type": "unique", "columns": ["id"]}])

        # as 2 linhas de "A" são duplicatas; "B" e o id nulo são partições de 1 linha.
        self.assertEqual(split.invalid.count(), 2)
        self.assertEqual(split.valid.count(), 2)
        self.assertEqual(
            sorted(r["valor"] for r in split.invalid.collect()), [10, 20]
        )
        self._assert_schema_preserved(df, split)

    def test_split_unique_combined_with_not_null(self):
        """OR de um predicado com window (unique) e um sem (not_null)."""
        df = self._df()
        split = Cola().split(df, [
            {"type": "unique", "columns": ["id"]},
            {"type": "not_null", "columns": ["id"]},
        ])

        # inválidas: as 2 duplicatas de "A" + a linha de id nulo.
        self.assertEqual(split.invalid.count(), 3)
        self.assertEqual(split.valid.count(), 1)
        self.assertEqual(split.valid.collect()[0]["id"], "B")
        self._assert_schema_preserved(df, split)

    def test_split_unique_with_multiple_columns(self):
        """Chave composta: a partição da window usa as duas colunas."""
        df = self._df()
        split = Cola().split(df, [{"type": "unique", "columns": ["id", "valor"]}])

        # (A,10) e (A,20) são distintos como par → nada duplicado.
        self.assertEqual(split.invalid.count(), 0)
        self.assertEqual(split.valid.count(), 4)
        self._assert_schema_preserved(df, split)

    def test_null_comparison_does_not_invalidate(self):
        """coalesce(false): comparação nula não invalida a linha."""
        df = self.spark.createDataFrame(
            [("A", 10), ("B", None)], "id string, valor int"
        )
        split = Cola().split(df, [{"type": "range", "column": "valor", "min": 5}])

        self.assertEqual(split.invalid.count(), 0)
        self.assertEqual(split.valid.count(), 2)
        self._assert_schema_preserved(df, split)

    def test_no_row_level_rules_keeps_everything_valid(self):
        """Só checks agregados → nada vai para a quarentena."""
        df = self._df()
        split = Cola().split(df, [{"type": "row_count", "min": 1}])

        self.assertEqual(split.valid.count(), 4)
        self.assertEqual(split.invalid.count(), 0)
        self._assert_schema_preserved(df, split)


class AnnotatedSplitTest(SparkTestCase):
    """Códigos de falha por linha: `annotate` + `only`."""

    def _people(self):
        # ana: ok | bruno: email inválido | carla: idade fora de faixa | dan: os dois
        return self.spark.createDataFrame(
            [
                ("ana", "ana@example.com", 34),
                ("bruno", "bruno#example", 28),
                ("carla", "carla@example.com", 200),
                ("dan", "dan#example", 300),
            ],
            "nome string, email string, idade int",
        )

    EMAIL = r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"

    def _rules(self):
        return [
            {"type": "regex", "column": "email", "pattern": self.EMAIL, "code": "BAD_EMAIL"},
            {"type": "range", "column": "idade", "min": 0, "max": 120},
            {"type": "row_count", "min": 1},
        ]

    def _codes_by_name(self, df, column="dq_codes"):
        return {row["nome"]: row[column] for row in df.collect()}

    def test_annotate_lists_the_violated_codes(self):
        split = Cola().split(self._people(), self._rules(), annotate="dq_codes")

        codes = self._codes_by_name(split.invalid)
        self.assertEqual(codes["bruno"], ["BAD_EMAIL"])
        # `code` omitido → a própria expressão da regra.
        self.assertEqual(codes["carla"], ["range(idade,0,120)"])
        # Ordem das regras declaradas, não ordem de descoberta.
        self.assertEqual(codes["dan"], ["BAD_EMAIL", "range(idade,0,120)"])
        self.assertNotIn("ana", codes)

    def test_annotate_column_is_an_array_of_strings(self):
        split = Cola().split(self._people(), self._rules(), annotate="dq_codes")
        self.assertEqual(dict(split.invalid.dtypes)["dq_codes"], "array<string>")

    def test_annotate_only_touches_the_invalid_side(self):
        df = self._people()
        split = Cola().split(df, self._rules(), annotate="dq_codes")

        self.assertEqual(split.valid.columns, df.columns)
        self.assertEqual(split.invalid.columns, df.columns + ["dq_codes"])
        # E nada da coluna auxiliar do split em nenhum dos lados.
        self.assertNotIn("__cola_is_invalid__", split.invalid.columns)
        self.assertNotIn("__cola_is_invalid__", split.valid.columns)

    def test_only_scopes_the_split_to_the_listed_codes(self):
        split = Cola().split(self._people(), self._rules(), annotate="dq_codes",
                             only=["BAD_EMAIL"])

        codes = self._codes_by_name(split.invalid)
        # carla só viola a faixa, que ficou fora do escopo → é válida aqui.
        self.assertEqual(sorted(codes), ["bruno", "dan"])
        self.assertEqual(codes["dan"], ["BAD_EMAIL"])
        self.assertEqual(sorted(r["nome"] for r in split.valid.collect()), ["ana", "carla"])

    def test_only_accepts_a_derived_code(self):
        split = Cola().split(self._people(), self._rules(), only=["range(idade,0,120)"])
        self.assertEqual(sorted(r["nome"] for r in split.invalid.collect()), ["carla", "dan"])

    def test_only_with_no_matching_code_keeps_everything_valid(self):
        split = Cola().split(self._people(), self._rules(), annotate="dq_codes",
                             only=["NAO_EXISTE"])

        self.assertEqual(split.valid.count(), 4)
        self.assertEqual(split.invalid.count(), 0)
        # Schema estável: o destino da quarentena não muda de forma quando o escopo
        # não casa com nenhuma regra.
        self.assertEqual(dict(split.invalid.dtypes)["dq_codes"], "array<string>")

    def test_annotate_with_only_aggregate_rules_keeps_the_column(self):
        split = Cola().split(self._people(), [{"type": "row_count", "min": 1}],
                             annotate="dq_codes")

        self.assertEqual(split.valid.count(), 4)
        self.assertEqual(split.invalid.count(), 0)
        self.assertEqual(dict(split.invalid.dtypes)["dq_codes"], "array<string>")
        self.assertNotIn("dq_codes", split.valid.columns)

    def test_annotate_with_a_window_predicate(self):
        """`unique` é window function: o código tem de sair no array igual aos outros."""
        df = self.spark.createDataFrame(
            [("A", 10), ("A", 20), ("B", 30)], "id string, valor int"
        )
        split = Cola().split(df, [{"type": "unique", "columns": ["id"]}], annotate="dq_codes")

        rows = split.invalid.collect()
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertEqual(row["dq_codes"], ["unique(id)"])

    def test_null_comparison_does_not_produce_a_code(self):
        """coalesce(false) por regra: comparação nula não invalida nem rotula."""
        df = self.spark.createDataFrame(
            [("A", 10), ("B", None)], "id string, valor int"
        )
        split = Cola().split(df, [{"type": "range", "column": "valor", "min": 5}],
                             annotate="dq_codes")

        self.assertEqual(split.invalid.count(), 0)
        self.assertEqual(split.valid.count(), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
