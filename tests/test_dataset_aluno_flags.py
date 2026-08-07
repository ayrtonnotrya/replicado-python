"""Testes das flags exploratórias do dataset aluno × turma.

Cobrem as decisões pré-implementação:
- ``excluir_fantasmas`` (sufixo ``_sf``): preserva y=1, remove negativos de
  alunos com zero matrículas no semestre.
- ``balancear_l`` (sufixo ``_bl``): amostra negativos L entre matriculados até
  a razão média O/E/C, sem duplicar ``pos``/``neg`` nem vazar disciplina de
  currículo.
- ``_saida_com_flags``: renomeação isolada por combinação de flags.

Roda **sem banco/túnel** (dados sintéticos / cache).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from replicado.dataset_aluno import (
    DatasetAlunoConfig,
    _negativos_l_balanceados,
    _saida_com_flags,
)


def _cfg(**kw) -> DatasetAlunoConfig:
    base = dict(
        codundclg=45,
        prefixos=("MAC",),
        sufixo_min=40,
        ano_min=2018,
        ano_max=2024,
    )
    base.update(kw)
    return DatasetAlunoConfig.from_env(**base)


def _sr(*vals) -> pd.Series:
    return pd.Series(list(vals), dtype=object)


def _fixture() -> dict[str, pd.DataFrame]:
    """Cenário sintético mínimo para ``_negativos_l_balanceados``.

    - matriculados: 101, 102 (têm ≥1 ``pos``)
    - currículo (necess): DISC1='O', DISC2='E/C' para ambos
    - oferta: DISC1, DISC2 (curriculares), DISC3, DISC4 (livres)
    - ``pos``: 101/DISC1 (O), 102/DISC2 (E/C), 101/DISC3 (L)
    - ``neg``: 102/DISC1 (O) — sem L-neg pré-existente
    """
    necess = pd.DataFrame(
        {
            "codpes": [101, 101, 102, 102],
            "coddis": _sr("DISC1", "DISC2", "DISC1", "DISC2"),
            "status_obrigatoriedade_otimista": _sr("O", "E/C", "O", "E/C"),
        }
    )
    tur = pd.DataFrame(
        {
            "coddis": _sr("DISC1", "DISC2", "DISC3", "DISC4"),
            "codtur": _sr("20181", "20181", "20181", "20181"),
            "sufixo": [41, 42, 43, 44],
            "ano_sem": [20181] * 4,
            "sem_tipo": _sr("1S", "1S", "1S", "1S"),
            "dtainitur": [pd.Timestamp("2018-02-20")] * 4,
        }
    )
    pos = pd.DataFrame(
        {
            "codpes": [101, 102, 101],
            "coddis": _sr("DISC1", "DISC2", "DISC3"),
            "codtur": _sr("20181", "20181", "20181"),
            "alvo_matriculado": [1, 1, 1],
        }
    )
    neg = pd.DataFrame(
        {
            "codpes": [102],
            "coddis": _sr("DISC1"),
            "codtur": _sr("20181"),
            "alvo_matriculado": [0],
        }
    )
    aprovados = pd.DataFrame({"codpes": pd.Series([], dtype=int), "coddis": _sr()})
    return dict(necess=necess, tur=tur, pos=pos, neg=neg, aprovados=aprovados)


# ---------------------------------------------------------------------------
# _saida_com_flags
# ---------------------------------------------------------------------------
def test_saida_com_flags_sem_flag_nao_altera_baseline() -> None:
    cfg = _cfg()
    assert _saida_com_flags(cfg) == Path("temp/dataset_aluno.csv")


def test_saida_com_flags_combinacoes() -> None:
    casos = [
        (False, False, "temp/dataset_aluno.csv"),
        (True, False, "temp/dataset_aluno_sf.csv"),
        (False, True, "temp/dataset_aluno_bl.csv"),
        (True, True, "temp/dataset_aluno_sf_bl.csv"),
    ]
    for ef, bl, esperado in casos:
        cfg = _cfg(excluir_fantasmas=ef, balancear_l=bl)
        assert str(_saida_com_flags(cfg)) == esperado


def test_saida_com_flags_aplica_sufixo_sobre_override() -> None:
    cfg = _cfg(saida=Path("temp/meu.csv"), excluir_fantasmas=True, balancear_l=True)
    assert _saida_com_flags(cfg) == Path("temp/meu_sf_bl.csv")


def test_config_defaults_preservam_baseline() -> None:
    cfg = _cfg()
    assert cfg.excluir_fantasmas is False
    assert cfg.balancear_l is False


# ---------------------------------------------------------------------------
# _negativos_l_balanceados
# ---------------------------------------------------------------------------
def test_l_balanceados_invariantes() -> None:
    f = _fixture()
    out = _negativos_l_balanceados(
        _cfg(), f["pos"], f["neg"], f["necess"], f["aprovados"], f["tur"], 20181
    )
    assert not out.empty
    assert (out["alvo_matriculado"] == 0).all()
    # nenhuma colisão com pos/neg
    pos_neg = pd.concat([f["pos"], f["neg"]])
    occ = set(
        zip(
            pos_neg["codpes"].astype(int),
            pos_neg["coddis"],
            pos_neg["codtur"],
            strict=True,
        )
    )
    nov = set(
        zip(out["codpes"].astype(int), out["coddis"], out["codtur"], strict=True)
    )
    assert not (nov & occ)
    # fora do currículo do aluno (não em necess)
    nec = set(
        zip(
            f["necess"]["codpes"].astype(int),
            f["necess"]["coddis"],
            strict=True,
        )
    )
    assert not (set(zip(out["codpes"].astype(int), out["coddis"], strict=True)) & nec)
    # alunos com ≥1 matrícula
    assert out["codpes"].astype(int).isin([101, 102]).all()


def test_l_balanceados_atinge_razao_alvo() -> None:
    f = _fixture()
    out = _negativos_l_balanceados(
        _cfg(), f["pos"], f["neg"], f["necess"], f["aprovados"], f["tur"], 20181
    )
    # pos: O=1, E/C=1, L=1. neg: O=1, E/C=0.
    # razão média (1/1)=1.0 → target_neg_L = round(1/1)=1, sem L-neg pré.
    assert len(out) == 1


def test_l_balanceados_sem_positivos_l_retorna_vazio() -> None:
    f = _fixture()
    pos_no_l = f["pos"][f["pos"]["coddis"] != "DISC3"]
    out = _negativos_l_balanceados(
        _cfg(), pos_no_l, f["neg"], f["necess"], f["aprovados"], f["tur"], 20181
    )
    assert out.empty


def test_l_balanceados_nao_reamostra_neg_l_existente() -> None:
    """Se já há L-neg suficientes para o alvo, não amostra mais."""
    f = _fixture()
    # alvo = 1; já existe 1 L-neg (101/DISC4) → to_sample = 0
    neg_extra = pd.concat(
        [
            f["neg"],
            pd.DataFrame(
                {
                    "codpes": [101],
                    "coddis": _sr("DISC4"),
                    "codtur": _sr("20181"),
                    "alvo_matriculado": [0],
                }
            ),
        ],
        ignore_index=True,
    )
    out = _negativos_l_balanceados(
        _cfg(), f["pos"], neg_extra, f["necess"], f["aprovados"], f["tur"], 20181
    )
    assert out.empty
