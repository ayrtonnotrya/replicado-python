"""
Extrai e cacheia localmente as tabelas-base do Replicado usadas nas análises.

Gera em temp/cache_maquina_tempo/:
  - turmagr_full.pkl          : TURMAGR completa (colunas essenciais)
  - histescolar_<ANO>.pkl     : HISTESCOLARGR fatiada por ano da turma

As funções de extração vivem em :mod:`replicado.cache` (parametrizáveis por
``cache_dir``); este CLI é só um wrapper de linha de comando sobre elas.

Uso:
    poetry run python scripts/extrair_cache_replicado.py                # tudo
    poetry run python scripts/extrair_cache_replicado.py --anos 2022 2023
    poetry run python scripts/extrair_cache_replicado.py --forcar
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime

from dotenv import load_dotenv

from replicado.cache import DEFAULT_CACHE_DIR, extrair_histescolar, extrair_turmagr

load_dotenv()

_ANO_MIN = int(os.getenv("REPLICADO_ANO_MIN", "2010"))
_ANO_MAX_DEFAULT = int(os.getenv("REPLICADO_ANO_MAX", str(datetime.now().year)))


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

    extrair_turmagr(cache_dir=DEFAULT_CACHE_DIR, forcar=args.forcar)
    extrair_histescolar(args.anos, cache_dir=DEFAULT_CACHE_DIR, forcar=args.forcar)
    print("Concluído.")


if __name__ == "__main__":
    main()
