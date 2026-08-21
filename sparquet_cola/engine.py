"""Motor do sparquet_cola: roda checks e faz o split valid/invalid (quarentena)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparquet_cola.checks import (
    BaseCheck,
    CheckResult,
    MetricCheck,
    NotNullCheck,
    RangeCheck,
    RegexCheck,
    RowCountCheck,
    SchemaCheck,
    SqlCheck,
    UniqueCheck,
)

# Registry padrão: nome no JSON → classe de check. O `type` do JSON continua o mesmo
# ("validations" → rules[].type); o branding sparquet_cola é só interno.
_DEFAULT_CHECKS: Dict[str, Type[BaseCheck]] = {
    "not_null": NotNullCheck,
    "unique": UniqueCheck,
    "range": RangeCheck,
    "regex": RegexCheck,
    "row_count": RowCountCheck,
    "sql": SqlCheck,
    "check": MetricCheck,
    "schema": SchemaCheck,
}


@dataclass
class ColaSplit:
    """Resultado do split de quarentena: linhas válidas e inválidas."""

    valid: DataFrame
    invalid: DataFrame


class Cola:
    """API pública do sparquet_cola — use como biblioteca independente.

        from sparquet_cola import Cola
        cola = Cola()

        results = cola.run(df, [
            {"type": "not_null", "columns": ["id"]},
            {"type": "check", "metric": "missing_percent", "column": "cpf", "must_be": "< 5%"},
        ])

        split = cola.split(df, [
            {"type": "not_null", "columns": ["id"]},
            {"type": "check", "metric": "invalid_percent", "column": "email",
             "valid_format": "email", "must_be": "< 100%"},
        ])
        split.valid.write...    split.invalid.write...

        # Quarentena rotulada: `dq_codes` diz QUAL regra rejeitou cada linha, e
        # `only` restringe o split às regras escolhidas (pelos códigos).
        split = cola.split(df, rules, annotate="dq_codes", only=["not_null(id)"])

    Cada regra é um dict com `type` + params (o mesmo shape do bloco `validations`
    do sparquet). Depende só de pyspark — pode virar um pacote separado.
    """

    def __init__(self) -> None:
        self._registry: Dict[str, Type[BaseCheck]] = dict(_DEFAULT_CHECKS)

    def register(self, name: str, cls: Type[BaseCheck]) -> None:
        """Registra um check customizado disponível por `type`."""
        self._registry[name] = cls

    @property
    def available(self) -> List[str]:
        return sorted(self._registry)

    def build(self, rule: Any) -> BaseCheck:
        """Instancia o check de uma regra (dict com `type`, ou um ValidationRule)."""
        rule_type = rule.type if hasattr(rule, "type") else rule["type"]
        cls = self._registry.get(rule_type)
        if cls is None:
            raise ValueError(f"Unknown check '{rule_type}'. Available: {self.available}")
        if hasattr(rule, "params"):
            return cls(rule)
        params = {k: v for k, v in rule.items() if k != "type"}
        return cls(params)

    def run(self, df: DataFrame, rules: List[Any]) -> List[CheckResult]:
        """Roda todos os checks e devolve um CheckResult por regra."""
        return [self.build(rule).validate(df) for rule in rules]

    def codes(self, rules: Iterable[Any]) -> List[str]:
        """O código de cada regra, na ordem — declarado (`code`) ou derivado.

        Use para descobrir com o que rotular/filtrar uma quarentena antes de rodar
        o split (é o mesmo valor que `annotate` grava na linha).
        """
        return [self.build(rule).code() for rule in rules]

    def split(
        self,
        df: DataFrame,
        rules: List[Any],
        annotate: Optional[str] = None,
        only: Optional[Iterable[str]] = None,
    ) -> ColaSplit:
        """Divide o df em válidas/inválidas usando os checks *row-level*.

        Uma linha é **inválida** quando viola QUALQUER check que sabe apontar linhas
        (not_null, range, regex, unique, e o `check` de missing/invalid). Checks
        agregados (row_count, avg, freshness, duplicate_count…) não entram no split.
        Se nenhum check for row-level, tudo é válido.

        `annotate` é o nome de uma coluna `array<string>` adicionada **só ao
        `invalid`** com os códigos (`BaseCheck.code()`) das regras que aquela linha
        violou — no `valid` ela seria vazia por definição, e nas saídas principais
        não tem sentido. É montada a partir dos mesmos predicados que decidem o
        split, então não custa uma passada extra nos dados.

        `only` restringe o split (e a anotação) às regras cujo código está na lista;
        omitido = todas as regras row-level, o comportamento histórico.
        """
        wanted = None if only is None else {str(code) for code in only}

        # (código, predicado) por regra row-level, na ordem declarada: é essa ordem
        # que aparece no array de códigos.
        violations: List[Tuple[str, Any]] = []
        for rule in rules:
            check = self.build(rule)
            if wanted is not None and check.code() not in wanted:
                continue
            violation = check.violation(df)
            if violation is not None:
                violations.append((check.code(), violation))

        if not violations:
            empty = df.limit(0)
            if annotate:
                # A quarentena vazia mantém o mesmo schema da anotada: o destino não
                # muda de forma só porque nenhuma regra row-level entrou no split.
                empty = empty.withColumn(annotate, self._empty_codes())
            return ColaSplit(valid=df, invalid=empty)

        cond = violations[0][1]
        for _, predicate in violations[1:]:
            cond = cond | predicate
        # coalesce(false): uma comparação nula (ex: col fora de range mas nula) não
        # deve nem invalidar nem sumir — vira válida.
        is_invalid = F.coalesce(cond, F.lit(False))

        # O predicado é MATERIALIZADO como coluna antes de filtrar: checks row-level
        # podem usar window functions (unique → count() over partitionBy), e o Spark
        # proíbe window function dentro de WHERE
        # ([WINDOW_FUNCTION_NOT_ALLOWED_IN_CLAUSE]). Com a flag calculada no SELECT, o
        # filtro passa a ser uma referência simples de coluna e funciona para qualquer
        # combinação de checks.
        flag = self._flag_column(df)
        flagged = df.withColumn(flag, is_invalid)
        invalid = flagged.filter(F.col(flag))
        if annotate:
            # Depois do filtro: a coluna só existe no lado inválido. Mesma lógica de
            # coalesce(false) por regra, para uma comparação nula não virar código.
            invalid = invalid.withColumn(annotate, self._codes_column(violations))
        # A coluna auxiliar é descartada nos dois lados: quem chama (o sparquet grava
        # direto no destino) precisa do schema original, sem vazamento na quarentena.
        return ColaSplit(
            valid=flagged.filter(~F.col(flag)).drop(flag),
            invalid=invalid.drop(flag),
        )

    @staticmethod
    def _empty_codes():
        """`array<string>` vazio — o tipo é fixado no cast, não inferido."""
        return F.array().cast("array<string>")

    @staticmethod
    def _codes_column(violations: List[Tuple[str, Any]]):
        """Array com os códigos das regras que a linha viola, na ordem declarada."""
        entries = [
            F.when(F.coalesce(predicate, F.lit(False)), F.lit(code))
            for code, predicate in violations
        ]
        # As regras que a linha NÃO viola entram como NULL e são removidas — sobra só
        # o motivo real da rejeição.
        return F.filter(F.array(*entries), lambda code: code.isNotNull()).cast("array<string>")

    @staticmethod
    def _flag_column(df: DataFrame) -> str:
        """Nome interno da flag de invalidez, garantido fora do schema do df."""
        name = "__cola_is_invalid__"
        existing = set(df.columns)
        while name in existing:
            name = f"_{name}"
        return name
