"""Extração e cache local das tabelas-base do Replicado (TURMAGR e
HISTESCOLARGR fatiada por ano).

Funções de *baixo nível* para popular/refresh o cache em ``cache_dir`` —
o mesmo diretório usado por :mod:`replicado.dataset_alocacao`
(``temp/cache_maquina_tempo`` por default). São a **fonte	canônica** das
operações de extração, parametrizáveis por ``cache_dir`` (para isolar
unidades/serviços) e por ``forcar`` (re-extrair mesmo com cache existente).

Antes estas funções viviam só em ``scripts/extrair_cache_replicado.py``; os
consumidores programáticos (ex.: a API Skuld) tinham que reimplementá-las só
para passar um ``cache_dir`` custom. Agora são parte do package — favorecendo
:func:`replicado.dataset_alocacao.montar_dataset` com ``atualizar_anos`` como
wrapper declarativo, mas disponíveis para uso direto quando se deseja controle
fino do cache.

Uso:

    from replicado.cache import extrair_turmagr, extrair_histescolar

    extrair_turmagr(cache_dir=Path("/srv/skuld/cache"), forcar=True)
    extrair_histescolar([2024, 2025], cache_dir=Path("/srv/skuld/cache"))
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from .connection import DB
from .dataset_alocacao import COLS_TURMAGR

# Diretório de cache default — mesmo do dataset_alocacao.
DEFAULT_CACHE_DIR = Path("temp/cache_maquina_tempo")

# Colunas da HISTESCOLARGR efetivamente usadas pelo pipeline (lista explícita
# para nunca depender de ``SELECT *``). É a **fonte da verdade** das fatias
# anuais; tanto este módulo quanto o script de extração consomem daqui.
COLS_HIST = """
    codpes, coddis, verdis, codtur, dtacrihst, stamtr, dtaultalt,
    rstfim, discrl, aplori
"""


def _resolve_cache_dir(cache_dir: Path | None) -> Path:
    """Normaliza ``cache_dir`` (default :data:`DEFAULT_CACHE_DIR`) e gara
que exista, devolvendo-o."""
    d = DEFAULT_CACHE_DIR if cache_dir is None else Path(cache_dir)
    d.mkdir(parents=True, exist_ok=True)
    return d


def extrair_turmagr(
    cache_dir: Path | None = None, forcar: bool = False
) -> pd.DataFrame:
    """Extrai a TURMAGR completa (``SELECT {COLS_TURMAGR}``) e salva em
    ``<cache_dir>/turmagr_full.pkl``.

    - ``forcar=False`` (default) e o pickle já existe → lê do cache, sem
      bater no banco.
    - ``forcar=True`` ou pickle ausente → re-extrai do Replicado e sobrescreve
      o cache.

    Retorna o DataFrame (com ``coddis``/``codtur`` saneados).
    """
    cache = _resolve_cache_dir(cache_dir) / "turmagr_full.pkl"
    if cache.exists() and not forcar:
        print(f"TURMAGR: cache existente ({cache}), pulando.")
        return pd.read_pickle(cache)
    t0 = time.time()
    rows = DB.fetch_all(f"SELECT {COLS_TURMAGR} FROM TURMAGR")
    df = pd.DataFrame(rows)
    if len(df):
        df["coddis"] = df["coddis"].astype(str).str.strip().str.upper()
        df["codtur"] = df["codtur"].astype(str).str.strip()
    df.to_pickle(cache)
    print(f"TURMAGR: {len(df)} turmas salvas em {cache} ({time.time() - t0:.0f}s)")
    return df


def extrair_fatia_histescolar(ano: int, cache_dir: Path | None = None) -> pd.DataFrame:
    """Extrai UMA fatia anual da HISTESCOLARGR (``codtur LIKE '<ano>%'``) e
    salva em ``<cache_dir>/histescolar_<ano>.pkl``, sobrescrevendo qualquer
    cache prévio. Sempre bate no banco (não consulta ``forcar``).

    É o bloco atômico reusado por :func:`extrair_histescolar` e pelo loop de
    refresh de :func:`replicado.dataset_alocacao.carregar_dados`
    (``atualizar_anos`` / ``forcar``). Retorna o DataFrame da fatia, com
    ``coddis``/``codtur`` saneados (``dtacrihst``/``dtaultalt`` ficam como
    string cru; a normalização para ``datetime`` ocorre em ``carregar_dados``).
    """
    base = _resolve_cache_dir(cache_dir)
    cache = base / f"histescolar_{ano}.pkl"
    t0 = time.time()
    rows = DB.fetch_all(
        f"SELECT {COLS_HIST} FROM HISTESCOLARGR WHERE codtur LIKE '{ano}%'"
    )
    df = pd.DataFrame(rows)
    if len(df):
        df["coddis"] = df["coddis"].astype(str).str.strip().str.upper()
        df["codtur"] = df["codtur"].astype(str).str.strip()
    df.to_pickle(cache)
    print(
        f"HISTESCOLARGR {ano}: {len(df)} registros salvos em {cache} "
        f"({time.time() - t0:.0f}s)"
    )
    return df


def extrair_histescolar(
    anos: Iterable[int],
    cache_dir: Path | None = None,
    forcar: bool = False,
) -> dict[int, pd.DataFrame]:
    """Extrai/cacheia fatias anuais da HISTESCOLARGR.

    Para cada ano em ``anos``:

    - ``forcar=False`` (default) e o pickle já existe → lê do cache.
    - ``forcar=True`` ou pickle ausente → re-extrai do Replicado (via
      :func:`extrair_fatia_histescolar`), sobrescrevendo o cache.

    Retorna ``{ano: DataFrame}`` das fatias processadas (extraídas OU lidas
    do cache). íltima para callers que precisam do histórico em memória.
    """
    base = _resolve_cache_dir(cache_dir)
    out: dict[int, pd.DataFrame] = {}
    for ano in anos:
        cache = base / f"histescolar_{ano}.pkl"
        if cache.exists() and not forcar:
            print(f"HISTESCOLARGR {ano}: cache existente, pulando.")
            out[ano] = pd.read_pickle(cache)
            continue
        out[ano] = extrair_fatia_histescolar(ano, base)
    return out


__all__ = [
    "DEFAULT_CACHE_DIR",
    "COLS_HIST",
    "extrair_turmagr",
    "extrair_fatia_histescolar",
    "extrair_histescolar",
]
