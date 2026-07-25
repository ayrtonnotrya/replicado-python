"""CLI para o fornecedor de dataset de alocação de salas.

Delega toda a lógica para :mod:`replicado.dataset_alocacao`; existe só como
ponto de entrada prático.

Uso
---
    poetry run python scripts/build_dataset.py
    poetry run python scripts/build_dataset.py --codundclg 45 --prefixos MAC MAT
    poetry run python scripts/build_dataset.py --forcar-extracao --saida temp/dataset.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from replicado.dataset_alocacao import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
