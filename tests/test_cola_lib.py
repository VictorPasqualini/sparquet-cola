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
    METRIC_TYPES,
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
        available = Cola().available
        # As regras com semântica própria — nenhuma delas expressável como métrica.
        for rule_type in ("not_null", "unique", "range", "regex", "row_count", "sql", "schema"):
            self.assertIn(rule_type, available)
        # Toda métrica é um tipo de regra, sem wrapper.
        for metric in METRIC_TYPES:
            self.assertIn(metric, available)
        # E o wrapper `check` deixou de existir: só carregava um campo `metric` que
        # agora é o próprio `type`.
        self.assertNotIn("check", available)

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
            ({"type": "missing_percent", "column": "cpf", "must_be": "< 1%"},
             "missing_percent(cpf)"),
            ({"type": "invalid_count", "columns": ["email"], "must_be": "= 0"},
             "invalid_count(email)"),
            ({"type": "row_count", "must_be": "> 0"}, "row_count"),
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


class TestColumnForms(unittest.TestCase):
    """`columns` (lista) e `column` (singular) valem em qualquer regra de coluna.

    Não é açúcar: com `targets`, a forma natural de declarar um alvo é
    `{"column": "id"}` — é o que `range` e `regex` já usavam —, e o código derivado
    (`not_null(id)`) sempre aceitou as duas formas. Só a execução do `not_null`/`unique`
    lia `params["columns"]` cru: o relatório saía certo e o pipeline morria com um
    `KeyError: 'columns'` sem dizer qual regra.
    """

    def test_singular_column_derives_the_same_code(self):
        cola = Cola()
        self.assertEqual(
            cola.codes([{"type": "not_null", "column": "id"}]),
            cola.codes([{"type": "not_null", "columns": ["id"]}]),
        )

    def test_singular_column_builds_a_runnable_check(self):
        # Sem Spark: basta provar que a leitura das colunas não levanta.
        from sparquet_cola.checks import _columns_of

        self.assertEqual(_columns_of({"column": "id"}, "not_null"), ["id"])
        self.assertEqual(_columns_of({"columns": ["id", "dt"]}, "unique"), ["id", "dt"])
        # Uma string em `columns` é o erro de digitação mais provável, e o significado
        # pretendido é óbvio — aceitar evita um crash por uma vírgula esquecida.
        self.assertEqual(_columns_of({"columns": "id"}, "not_null"), ["id"])

    def test_no_column_at_all_says_which_rule_and_what_to_declare(self):
        from sparquet_cola.checks import _columns_of

        with self.assertRaises(ValueError) as ctx:
            _columns_of({}, "not_null")
        self.assertIn("not_null", str(ctx.exception))
        self.assertIn("column", str(ctx.exception))

    def test_a_frame_wide_metric_may_have_no_column(self):
        from sparquet_cola.checks import _columns_of

        # row_count não mede coluna nenhuma — exigir uma seria inventar requisito.
        self.assertEqual(_columns_of({}, "row_count", required=False), [])


class TestTargets(unittest.TestCase):
    """`targets`: uma entrada de regra vira N regras independentes.

    Independentes é o ponto: cada alvo tem seu próprio resultado, seu próprio código e
    sua própria contribuição à quarentena. Um veredito agregado não diria qual coluna
    quebrou — que é justamente o que se quer saber.
    """

    def test_one_entry_becomes_one_rule_per_target(self):
        rules = Cola().expand([
            {"type": "regex",
             "targets": [{"column": "document", "pattern": "^[0-9]{11}$"},
                         {"column": "document2", "pattern": "^[0-9]{12}$"}]},
        ])
        self.assertEqual(rules, [
            {"type": "regex", "column": "document", "pattern": "^[0-9]{11}$"},
            {"type": "regex", "column": "document2", "pattern": "^[0-9]{12}$"},
        ])

    def test_parent_keys_are_shared_defaults(self):
        rules = Cola().expand([
            {"type": "range", "min": 0, "targets": [{"column": "a"}, {"column": "b", "max": 9}]},
        ])
        self.assertEqual(rules, [
            {"type": "range", "min": 0, "column": "a"},
            {"type": "range", "min": 0, "column": "b", "max": 9},
        ])

    def test_each_target_gets_its_own_code(self):
        codes = Cola().codes([
            {"type": "regex",
             "targets": [{"column": "cpf", "pattern": "^[0-9]{11}$"},
                         {"column": "cnpj", "pattern": "^[0-9]{14}$"}]},
        ])
        self.assertEqual(codes, ["regex(cpf,^[0-9]{11}$)", "regex(cnpj,^[0-9]{14}$)"])

    def test_expansion_is_idempotent(self):
        cola = Cola()
        once = cola.expand([{"type": "not_null", "targets": [{"columns": ["a"]}]}])
        self.assertEqual(cola.expand(once), once)

    def test_a_rule_without_targets_passes_through_untouched(self):
        rule = {"type": "row_count", "min": 1}
        self.assertIs(Cola().expand([rule])[0], rule)

    def test_ambiguous_forms_are_refused_instead_of_degraded(self):
        cases = {
            "lista vazia apagaria a validação": {"type": "regex", "targets": []},
            "code no pai duplicaria o código": {
                "type": "regex", "code": "X", "targets": [{"column": "a"}]},
            "target vazio duplicaria a regra pai": {"type": "regex", "targets": [{}]},
            "type dentro do target": {"type": "regex", "targets": [{"type": "range"}]},
            "targets aninhado": {"type": "regex", "targets": [{"targets": []}]},
            "targets não-lista": {"type": "regex", "targets": {"column": "a"}},
        }
        for why, rule in cases.items():
            with self.subTest(why=why):
                with self.assertRaises(ValueError):
                    Cola().expand([rule])


class TestCheckResult(unittest.TestCase):
    def test_failed_rows_defaults_none_and_severity_derived(self):
        r = CheckResult("sql", passed=False, message="x", failed_count=3)
        self.assertIsNone(r.failed_rows)
        self.assertEqual(r.severity, "fail")
        self.assertEqual(CheckResult("x", passed=True).severity, "pass")


if __name__ == "__main__":
    unittest.main(verbosity=2)
