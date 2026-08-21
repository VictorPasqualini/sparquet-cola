"""Testes da biblioteca sparquet_cola (partes puras — sem SparkSession).

Constrói checks a partir de dicts, verifica o registry, o contrato de check
customizado e quais checks sabem apontar linhas (violation) para o split
valid/invalid. Colunas do Spark são construídas de forma lazy, então não é preciso
uma SparkSession ativa.
"""
from __future__ import annotations

import unittest

from sparquet_cola import Cola, CheckResult
from sparquet_cola.checks import (
    BaseCheck,
    MetricCheck,
    NotNullCheck,
    RangeCheck,
    RegexCheck,
    RowCountCheck,
    SchemaCheck,
    SqlCheck,
    UniqueCheck,
)


class TestRegistry(unittest.TestCase):
    def test_default_checks(self):
        self.assertEqual(
            Cola().available,
            ["check", "not_null", "range", "regex", "row_count", "schema", "sql", "unique"],
        )

    def test_build_from_dict(self):
        chk = Cola().build({"type": "not_null", "columns": ["id"]})
        self.assertIsInstance(chk, NotNullCheck)
        self.assertEqual(chk.params["columns"], ["id"])

    def test_build_strips_type(self):
        chk = Cola().build({"type": "sql", "query": "SELECT true"})
        self.assertIsInstance(chk, SqlCheck)
        self.assertNotIn("type", chk.params)

    def test_unknown_check_raises(self):
        with self.assertRaises(ValueError):
            Cola().build({"type": "nao_existe"})

    def test_register_custom_check(self):
        class AlwaysOk(BaseCheck):
            def run(self, df):
                return CheckResult("always_ok", True)

        cola = Cola()
        cola.register("always_ok", AlwaysOk)
        self.assertIn("always_ok", cola.available)
        self.assertIsInstance(cola.build({"type": "always_ok"}), AlwaysOk)


class TestViolationContract(unittest.TestCase):
    """Contrato do split: quais checks sabem apontar linhas (violation).

    A construção das Columns de violação envolve literais e window functions que
    exigem uma SparkSession ativa, então aqui verificamos apenas o CONTRATO (qual
    check sobrescreve `violation`); a correção dos predicados é coberta em integração.
    """

    def test_row_level_checks_override_violation(self):
        for cls in (NotNullCheck, RangeCheck, RegexCheck, UniqueCheck, MetricCheck):
            self.assertIsNot(cls.violation, BaseCheck.violation, cls.__name__)

    def test_table_level_checks_use_base_violation(self):
        # row_count, sql e schema não atribuem violação a linhas → não entram no split.
        for cls in (RowCountCheck, SqlCheck, SchemaCheck):
            self.assertIs(cls.violation, BaseCheck.violation, cls.__name__)


class TestRuleCodes(unittest.TestCase):
    """`code()`: o `code` declarado, ou a expressão da regra renderizada.

    O código vai PARA DENTRO DOS DADOS (coluna de anotação da quarentena), então o
    que se cobra aqui é determinismo: a mesma regra sempre a mesma string. Puro —
    só formatação de string, sem SparkSession.
    """

    def code(self, rule):
        return Cola().build(rule).code()

    def test_declared_code_wins(self):
        self.assertEqual(
            self.code({"type": "range", "column": "age", "min": 1, "max": 99, "code": "AGE_RANGE"}),
            "AGE_RANGE",
        )

    def test_blank_declared_code_falls_back_to_the_expression(self):
        # `"code": ""` é ausência de código, não um código vazio gravado na linha.
        self.assertEqual(self.code({"type": "not_null", "columns": ["email"], "code": "  "}),
                         "not_null(email)")

    def test_declared_code_is_trimmed(self):
        self.assertEqual(self.code({"type": "unique", "columns": ["id"], "code": " PK \n"}), "PK")

    def test_derived_codes_per_check(self):
        cases = [
            ({"type": "not_null", "columns": ["email"]}, "not_null(email)"),
            ({"type": "not_null", "columns": ["id", "cpf"]}, "not_null(id,cpf)"),
            ({"type": "unique", "columns": ["id"]}, "unique(id)"),
            ({"type": "unique", "columns": ["id", "dt"]}, "unique(id,dt)"),
            ({"type": "range", "column": "age", "min": 1, "max": 99}, "range(age,1,99)"),
            ({"type": "range", "column": "valor", "min": 0}, "range(valor,0,*)"),
            ({"type": "range", "column": "valor", "max": 10}, "range(valor,*,10)"),
            ({"type": "regex", "column": "email", "pattern": "^.+@.+$"}, "regex(email,^.+@.+$)"),
            ({"type": "check", "metric": "missing_percent", "column": "cpf", "must_be": "< 1%"},
             "missing_percent(cpf)"),
            ({"type": "check", "metric": "invalid_count", "columns": ["email"], "must_be": "= 0"},
             "invalid_count(email)"),
            ({"type": "check", "metric": "row_count", "must_be": "> 0"}, "row_count"),
            # Checks agregados nunca rotulam uma linha, mas ainda respondem `code()`.
            ({"type": "row_count", "min": 1}, "row_count"),
            ({"type": "schema", "required_columns": ["id"]}, "schema"),
            ({"type": "sql", "query": "SELECT true"}, "sql"),
        ]
        for rule, expected in cases:
            with self.subTest(rule=rule):
                self.assertEqual(self.code(rule), expected)

    def test_same_rule_always_renders_the_same_code(self):
        rule = {"type": "range", "column": "age", "min": 1, "max": 99}
        self.assertEqual(self.code(rule), self.code(dict(rule)))

    def test_codes_lists_every_rule_in_order(self):
        codes = Cola().codes([
            {"type": "not_null", "columns": ["id"]},
            {"type": "range", "column": "age", "min": 1, "max": 99, "code": "AGE_RANGE"},
            {"type": "row_count", "min": 1},
        ])
        self.assertEqual(codes, ["not_null(id)", "AGE_RANGE", "row_count"])

    def test_custom_check_falls_back_to_its_type(self):
        class Weird(BaseCheck):
            check_type = "weird"

            def run(self, df):
                return CheckResult("weird", True)

        cola = Cola()
        cola.register("weird", Weird)
        self.assertEqual(cola.build({"type": "weird"}).code(), "weird")


class TestCheckResult(unittest.TestCase):
    def test_failed_rows_defaults_none_and_severity_derived(self):
        r = CheckResult("sql", passed=False, message="x", failed_count=3)
        self.assertIsNone(r.failed_rows)
        self.assertEqual(r.severity, "fail")
        self.assertEqual(CheckResult("x", passed=True).severity, "pass")


if __name__ == "__main__":
    unittest.main(verbosity=2)
