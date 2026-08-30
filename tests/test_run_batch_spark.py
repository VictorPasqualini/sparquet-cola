"""Testes de integração da passada agregada de `Cola.run` com Spark real.

Os checks que sabem se exprimir como agregação (`BaseCheck.aggregations`) são
medidos **juntos**, num único `df.agg(...)`, em vez de uma action por regra. O que
este arquivo trava é a equivalência: rodar as regras em bloco tem de dar exatamente
o mesmo `CheckResult` de rodar cada uma sozinha — mesmo veredito, mesma mensagem,
mesma contagem, mesmo `metric_value`. É o caminho fácil de quebrar em silêncio,
porque um valor lido da fatia errada continua sendo um número plausível.

Trava também o que a agregação **não** pode mudar de semântica: `unique` conta a
combinação com NULL como um valor (é o que `distinct().count()` sempre fez, e o que
`count_distinct` sobre colunas soltas descartaria), e `sql`/`schema` continuam fora
da passada, rodando cada um a sua.

Roda com `python tests/test_run_batch_spark.py` (ou pytest). Sem pyspark ou sem um
Java/Spark funcional, a classe é **pulada** — nunca falha.
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
                .appName("sparquet-cola-batch-tests")
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


#: Um bloco com todas as famílias de regra agregável, contra o df dos testes. Fica no
#: nível do módulo porque os dois testes centrais leem a MESMA lista: um compara o
#: bloco com regra a regra, o outro confere os valores absolutos.
REGRAS = [
    {"type": "not_null", "columns": ["id", "email"]},
    {"type": "unique", "columns": ["id"]},
    {"type": "range", "column": "idade", "min": 0, "max": 60},
    {"type": "regex", "column": "email", "pattern": ".*@.*"},
    {"type": "row_count", "min": 1},
    {"type": "missing_count", "column": "email", "must_be": "< 10"},
    {"type": "missing_percent", "column": "email", "must_be": "< 50%"},
    {"type": "invalid_count", "column": "uf", "valid_values": ["SP", "RJ"],
     "must_be": "< 10"},
    {"type": "duplicate_count", "columns": ["id"], "must_be": "< 10"},
    {"type": "distinct_count", "columns": ["uf"], "must_be": "> 0"},
    {"type": "avg", "column": "idade", "must_be": "> 0"},
    {"type": "max", "column": "idade", "must_be": "< 200"},
]


class BatchEqualsIndividualTest(SparkTestCase):
    """A passada conjunta responde o mesmo que uma action por regra."""

    def _df(self):
        # id "A" duplicado, um id nulo, um email nulo, uma idade fora da faixa,
        # um email sem arroba e uma uf fora dos valores válidos.
        return self.spark.createDataFrame(
            [
                ("A", "a@x.com", 30, "SP"),
                ("A", "a2@x.com", 61, "RJ"),
                ("B", None, 20, "MG"),
                (None, "sem-arroba", 40, "SP"),
            ],
            "id string, email string, idade int, uf string",
        )

    def _campos(self, resultado):
        return (
            resultado.rule_type, resultado.passed, resultado.message,
            resultado.failed_count, resultado.severity, resultado.metric_value,
        )

    def test_o_bloco_da_o_mesmo_que_regra_a_regra(self):
        df = self._df()
        cola = Cola()
        bloco = cola.run(df, REGRAS)
        uma_a_uma = [cola.run(df, [regra])[0] for regra in REGRAS]
        self.assertEqual(len(bloco), len(REGRAS))
        for regra, agregado, sozinho in zip(REGRAS, bloco, uma_a_uma):
            with self.subTest(regra=regra["type"]):
                self.assertEqual(self._campos(agregado), self._campos(sozinho))

    def test_os_valores_sao_os_do_dataframe(self):
        """Igual ao caminho antigo não basta: os números têm de estar certos."""
        por_tipo = {
            resultado.rule_type: resultado
            for resultado in Cola().run(self._df(), REGRAS)
        }
        # 1 id nulo + 1 email nulo
        self.assertEqual(por_tipo["not_null"].failed_count, 2)
        # "A" duplicado: 4 linhas, 3 combinações distintas (NULL conta como uma)
        self.assertEqual(por_tipo["unique"].failed_count, 1)
        self.assertEqual(por_tipo["duplicate_count"].metric_value, 1.0)
        # idade 61 fora de [0, 60]
        self.assertEqual(por_tipo["range"].failed_count, 1)
        # email nulo e "sem-arroba" — o regex conta NULL como violação
        self.assertEqual(por_tipo["regex"].failed_count, 2)
        self.assertEqual(por_tipo["row_count"].metric_value, 4.0)
        self.assertEqual(por_tipo["missing_count"].metric_value, 1.0)
        self.assertEqual(por_tipo["missing_percent"].metric_value, 25.0)
        # "MG" fora de valid_values; o email nulo é missing, não inválido
        self.assertEqual(por_tipo["invalid_count"].metric_value, 1.0)
        self.assertEqual(por_tipo["distinct_count"].metric_value, 3.0)
        self.assertEqual(por_tipo["avg"].metric_value, 37.75)
        self.assertEqual(por_tipo["max"].metric_value, 61.0)

    def test_unique_conta_a_combinacao_nula_como_um_valor(self):
        """`count_distinct` solto descartaria a linha nula e inventaria um duplicado."""
        df = self.spark.createDataFrame(
            [("A", 1), ("B", 2), (None, 3)], "id string, valor int"
        )
        # Duas regras para forçar a passada conjunta (com uma só, o motor não a monta).
        resultados = Cola().run(
            df,
            [{"type": "unique", "columns": ["id"]}, {"type": "row_count", "min": 1}],
        )
        self.assertTrue(resultados[0].passed)
        self.assertEqual(resultados[0].failed_count, 0)

    def test_sql_e_schema_ficam_fora_da_passada(self):
        """Nem todo check é agregável — os que não são continuam rodando sozinhos."""
        df = self._df()
        resultados = Cola().run(
            df,
            [
                {"type": "row_count", "min": 1},
                {"type": "sql", "query": "SELECT count(*) > 0 FROM _validation_df"},
                {"type": "schema", "required_columns": ["id", "email"]},
                {"type": "not_null", "columns": ["uf"]},
            ],
        )
        self.assertEqual([r.passed for r in resultados], [True, True, True, True])
        self.assertEqual(
            [r.rule_type for r in resultados],
            ["row_count", "sql", "schema", "not_null"],
        )

    def test_regras_repetidas_nao_colidem_na_agregacao(self):
        """Duas regras iguais medem a mesma coluna — o alias tem de ser posicional."""
        df = self._df()
        resultados = Cola().run(
            df,
            [
                {"type": "not_null", "columns": ["id"]},
                {"type": "not_null", "columns": ["id"]},
                {"type": "row_count", "min": 1},
            ],
        )
        self.assertEqual([r.failed_count for r in resultados[:2]], [1, 1])

    def test_range_sem_limites_nao_entra_na_agregacao(self):
        """Sem `min`/`max` a regra não mede nada: passa sem pedir coluna nenhuma."""
        resultados = Cola().run(
            self._df(),
            [{"type": "range", "column": "idade"}, {"type": "row_count", "min": 1}],
        )
        self.assertTrue(resultados[0].passed)
        self.assertEqual(resultados[0].failed_count, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
