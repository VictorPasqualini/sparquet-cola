# Deploy no PyPI — publicar o `sparquet-cola` como biblioteca

Guia para empacotar e publicar o projeto no PyPI, de forma que possa ser instalado
com `pip install sparquet-cola` e usado como biblioteca (import `sparquet_cola`).

O empacotamento já está configurado em [`pyproject.toml`](../pyproject.toml):
- nome de distribuição: **`sparquet-cola`** (import: `sparquet_cola`);
- **versão é fonte única** em `sparquet_cola/__init__.py` (`__version__`), lida
  dinamicamente pelo setuptools (`[tool.setuptools.dynamic]`);
- só o pacote `sparquet_cola` entra no wheel/sdist (`packages.find` com `include`);
- dependência base: `pyspark>=3.4.0`.

---

## 1. Deploy automatizado (CI/CD) — o caminho recomendado

O fluxo está automatizado em GitHub Actions. Na prática, publicar uma release faz tudo
(testes → build → publish).

| Arquivo | Dispara em | O que faz |
|---|---|---|
| [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) | push / PR na `main` | roda os testes puros numa matriz Python (3.9 / 3.11 / 3.12) |
| [`.github/workflows/publish.yml`](../.github/workflows/publish.yml) | **Release publicado** | testes → build (`+ twine check`) → **publish no PyPI** |
| idem | **execução manual** (Actions → *Run workflow*) | mesma esteira, mas **publish no TestPyPI** (ensaio) |

Em `publish.yml` o `build`/`publish` só rodam se o job de `test` passar — o teste é o
portão do release.

### Trusted Publishing (OIDC) — sem token manual

A publicação usa o [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) do
PyPI: o GitHub Actions troca um token OIDC de curta duração, então **não há token/segredo
de API** guardado no repo (por isso `permissions: id-token: write` nos jobs de publish).

Configuração (uma vez, em cada índice):

1. Em **pypi.org** → *Account → Publishing* → **Add a pending publisher** (o projeto ainda
   não existe): nome do projeto `sparquet-cola`, owner `VictorPasqualini`, repositório
   `sparquet-cola`, workflow `publish.yml`, environment `pypi`.
2. Repita em **test.pypi.org** com environment `testpypi`.

> Alternativa por token: se preferir não usar OIDC, remova o bloco `permissions` e passe
> `password: ${{ secrets.PYPI_API_TOKEN }}` ao `pypa/gh-action-pypi-publish`.

### Publicar uma versão

```bash
# 1. bump da versão + commit (edite __version__ em sparquet_cola/__init__.py)
git add sparquet_cola/__init__.py && git commit -m "release: v0.1.1"

# 2. (opcional) ensaio no TestPyPI: Actions → "Publish to PyPI" → Run workflow

# 3. tag + release no GitHub → dispara o publish no PyPI real
git tag v0.1.1 && git push --tags
gh release create v0.1.1 --generate-notes
```

> A versão do pacote vem do `__version__` (não da tag). Mantenha a tag e o `__version__`
> em sincronia.

---

## 2. Deploy manual (fallback / máquina local)

Se precisar publicar sem o CI:

```bash
python -m pip install --upgrade build twine

# bump de versão em sparquet_cola/__init__.py, depois:
rm -rf dist build *.egg-info
python -m build                 # gera dist/*.whl e dist/*.tar.gz
python -m twine check dist/*

# ensaio no TestPyPI
python -m twine upload --repository testpypi dist/*

# produção
python -m twine upload dist/*
```

Use o token como senha (usuário `__token__`), gerado em *Account settings → API tokens* de
cada índice. Opcionalmente configure `~/.pypirc` para não digitar toda vez.

---

## 3. Usando como biblioteca (consumidor)

```python
from sparquet_cola import Cola

cola = Cola()
for r in cola.run(df, [{"type": "row_count", "min": 1}]):
    print(r)
```

---

## 4. Checklist de release

- [ ] `__version__` atualizado e commitado (SemVer).
- [ ] Testes verdes (`PYTHONPATH=. python tests/test_cola_lib.py`).
- [ ] (opcional) ensaio no **TestPyPI** via *Run workflow*.
- [ ] Tag `v<versão>` + GitHub Release → publica no **PyPI**.
