"""CLI para o fornecedor de dataset aluno × turma (Micro Targeting Probabilístico).

Delega toda a lógica para :mod:`replicado.dataset_aluno`; existe só como
ponto de entrada prático.

Uso
---
    poetry run python scripts/build_dataset_aluno.py
    poetry run python scripts/build_dataset_aluno.py --codundclg 45 --prefixos MAC MAT
    poetry run python scripts/build_dataset_aluno.py --forcar-extracao --saida temp/dataset_aluno.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from replicado.dataset_aluno import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
