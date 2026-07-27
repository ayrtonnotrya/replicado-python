"""
Extrai e cacheia localmente as tabelas do Replicado usadas nas análises.

Gera em temp/cache_maquina_tempo/:
  - turmagr_full.pkl          : TURMAGR completa (colunas essenciais)
  - histescolar_<ANO>.pkl     : HISTESCOLARGR fatiada por ano da turma

Uso:
    poetry run python scripts/extrair_cache_replicado.py                # tudo
    poetry run python scripts/extrair_cache_replicado.py --anos 2022 2023
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from replicado.connection import DB  # noqa: E402

load_dotenv()

CACHE_DIR = Path("temp/cache_maquina_tempo")

_ANO_MIN = int(os.getenv("REPLICADO_ANO_MIN", "2010"))
_ANO_MAX_DEFAULT = int(os.getenv("REPLICADO_ANO_MAX", str(datetime.now().year)))

COLS_TURMAGR = """
    coddis, verdis, codtur, tiptur, tipmtr, dtainitur, dtafimtur, statur,
    dtacritur, numvagtur, numvagopt, numvagoptlre, numvagturcpl, numvagecr,
    numins, numinsopt, numinsoptlre, numinscpl, numinsecre = NULL,
    numpmtobg, numpmtopt, numpmtoptlre, numpmtcpl, numpmtecr,
    nummtr, nummtropt, nummtroptlre, nummtrturcpl, nummtrecr
""".replace("numinsecre = NULL", "numinsecr")

COLS_HIST = """
    codpes, coddis, verdis, codtur, dtacrihst, stamtr, dtaultalt,
    rstfim, discrl, aplori
"""


def extrair_turmagr(forcar: bool = False) -> pd.DataFrame:
    cache = CACHE_DIR / "turmagr_full.pkl"
    if cache.exists() and not forcar:
        print(f"TURMAGR: cache existente ({cache}), pulando.")
        return pd.read_pickle(cache)
    t0 = time.time()
    rows = DB.fetch_all(f"SELECT {COLS_TURMAGR} FROM TURMAGR")
    df = pd.DataFrame(rows)
    df["coddis"] = df["coddis"].str.strip().str.upper()
    df["codtur"] = df["codtur"].str.strip()
    df.to_pickle(cache)
    print(f"TURMAGR: {len(df)} turmas salvas em {cache} ({time.time()-t0:.0f}s)")
    return df


def extrair_histescolar(anos: list[int], forcar: bool = False) -> None:
    for ano in anos:
        cache = CACHE_DIR / f"histescolar_{ano}.pkl"
        if cache.exists() and not forcar:
            print(f"HISTESCOLARGR {ano}: cache existente, pulando.")
            continue
        t0 = time.time()
        rows = DB.fetch_all(
            f"SELECT {COLS_HIST} FROM HISTESCOLARGR WHERE codtur LIKE '{ano}%'"
        )
        df = pd.DataFrame(rows)
        if len(df):
            df["coddis"] = df["coddis"].str.strip().str.upper()
            df["codtur"] = df["codtur"].str.strip()
        df.to_pickle(cache)
        print(f"HISTESCOLARGR {ano}: {len(df)} registros salvos ({time.time()-t0:.0f}s)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        default=list(range(_ANO_MIN, _ANO_MAX_DEFAULT + 1)),
        help=(
            "Anos a extrair da HISTESCOLARGR (default: REPLICADO_ANO_MIN.."
            "REPLICADO_ANO_MAX do .env, fallback 2010..ano corrente)"
        ),
    )
    parser.add_argument("--forcar", action="store_true",
                        help="Reextrai mesmo com cache existente")
    args = parser.parse_args()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    extrair_turmagr(forcar=args.forcar)
    extrair_histescolar(args.anos, forcar=args.forcar)
    print("Concluído.")


if __name__ == "__main__":
    main()
