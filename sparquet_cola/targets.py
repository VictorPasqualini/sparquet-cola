"""Expansão de `targets` — uma entrada de regra vira N regras independentes.

Uma regra com `targets` é **açúcar de declaração**: tudo fora de `targets` é um
default compartilhado, cada target sobrescreve o que quiser, e o resultado é uma
lista achatada de regras comuns — cada uma com seu próprio `CheckResult`, sua
própria linha de relatório e seu próprio `code()`.

    {"type": "regex", "name": "documentos",
     "targets": [{"column": "cpf",  "pattern": "^[0-9]{11}$"},
                 {"column": "cnpj", "pattern": "^[0-9]{14}$"}]}

    →  [{"type": "regex", "name": "documentos", "column": "cpf",  "pattern": "^[0-9]{11}$"},
        {"type": "regex", "name": "documentos", "column": "cnpj", "pattern": "^[0-9]{14}$"}]

Por que uma função pura, exportada publicamente: a expansão é chamada em **dois**
lugares (`Cola.run/codes/split` aqui, e o parse da configuração no framework
sparquet). Os dois têm de calcular exatamente a mesma expansão, porque o framework
casa `rules[i]` com `results[i]` por posição — duas implementações divergentes
dessincronizariam o relatório. Então existe uma só, aqui.

Propriedades garantidas:

* **idempotente** — a saída nunca contém `targets`, logo `expand(expand(x)) == expand(x)`;
* **preserva a ordem** — `rules × targets` achatado na ordem de declaração;
* **passthrough por identidade** — uma regra sem `targets` sai como o *mesmo* objeto;
* **sem degradação silenciosa** — toda forma ambígua levanta `ValueError` no parse.

Depende só da stdlib: nada de pyspark, nada de I/O.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List

#: Nome da chave no JSON.
TARGETS_KEY = "targets"

#: Chaves que NÃO podem ficar no nível da regra junto com `targets`: cada uma
#: identifica ou endereça **uma** regra, e todas as regras expandidas herdariam o
#: mesmo valor — `code` duplicado torna a anotação da quarentena ambígua, e um
#: `output` compartilhado com `mode: overwrite` vira last-write-wins.
_PARENT_FORBIDDEN_WITH_TARGETS = ("code", "output")

#: Chaves proibidas DENTRO de um target: uma entrada de regra é UM tipo de regra
#: (é o que mantém "um nó do Studio ↔ um tipo de regra"), e `targets` aninhado tem
#: a mesma postura que `$include` aninhado — não suportado.
_TARGET_FORBIDDEN = ("type", TARGETS_KEY)


def _label(rule: Any, index: int) -> str:
    """Identifica a regra ofensora numa mensagem de erro: posição + `type`."""
    rule_type = rule.get("type") if isinstance(rule, dict) else getattr(rule, "type", None)
    return "rules[{}] (type={!r})".format(index, rule_type if rule_type else "?")


def expand_targets(rules: Iterable[Any]) -> List[Any]:
    """Achata `targets` em regras independentes. Pura, sem Spark, idempotente.

    Regras sem `targets` passam intactas (o mesmo objeto). Objetos que não são dict
    (o `ValidationRule` do framework) também passam — mas se ainda carregarem
    `targets` em `.params`, levantam: significa que a expansão no parse foi
    contornada, e seguir adiante produziria uma regra que mede a coluna errada.
    """
    expanded: List[Any] = []
    for index, rule in enumerate(rules or []):
        if not isinstance(rule, dict):
            params = getattr(rule, "params", None)
            if isinstance(params, dict) and TARGETS_KEY in params:
                raise ValueError(
                    f"{_label(rule, index)}: {TARGETS_KEY!r} chegou ao motor sem ter "
                    f"sido expandido. Chame expand_targets() no parse da configuração "
                    f"(sparquet: ValidationConfig.from_dict) antes de construir as regras."
                )
            expanded.append(rule)
            continue
        if TARGETS_KEY not in rule:
            expanded.append(rule)
            continue
        expanded.extend(_expand_one(rule, index))
    return expanded


def _expand_one(rule: Dict[str, Any], index: int) -> List[Dict[str, Any]]:
    label = _label(rule, index)
    targets = rule[TARGETS_KEY]

    if isinstance(targets, dict) or not isinstance(targets, (list, tuple)):
        raise ValueError(
            f"{label}: {TARGETS_KEY!r} deve ser uma LISTA de objetos; recebeu "
            f"{type(targets).__name__}."
        )
    if not targets:
        # Uma lista vazia apagaria a validação sem avisar — e ainda escreveria um
        # dataset `invalid` vazio, como se a regra tivesse passado.
        raise ValueError(
            f"{label}: {TARGETS_KEY!r} vazio apagaria a validação silenciosamente. "
            f"Remova a chave ou declare ao menos um target."
        )
    for key in _PARENT_FORBIDDEN_WITH_TARGETS:
        if key in rule:
            raise ValueError(
                f"{label}: {key!r} não pode ficar no nível da regra junto com "
                f"{TARGETS_KEY!r} — todas as regras expandidas herdariam o mesmo "
                f"valor. Declare {key!r} dentro de cada target."
            )

    shared = {k: v for k, v in rule.items() if k != TARGETS_KEY}
    out: List[Dict[str, Any]] = []
    for position, target in enumerate(targets):
        where = f"{label}: {TARGETS_KEY}[{position}]"
        if not isinstance(target, dict):
            raise ValueError(
                f"{where} deve ser um objeto; recebeu {type(target).__name__}. "
                f"Um valor solto não sabe qual chave preenche ('column'? 'columns'? "
                f"'query'?) — escreva {{\"column\": ...}} explicitamente."
            )
        if not target:
            raise ValueError(
                f"{where} está vazio — seria uma duplicata da regra pai, com o mesmo "
                f"código derivado, indistinguível na anotação da quarentena."
            )
        for key in _TARGET_FORBIDDEN:
            if key in target:
                raise ValueError(
                    f"{where}: {key!r} não é permitido dentro de um target."
                    + (" Uma entrada de regra é um único tipo de regra."
                       if key == "type" else
                       " targets aninhado não é suportado.")
                )
        out.append({**shared, **target})
    return out
