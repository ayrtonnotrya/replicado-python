"""Testes do backstop de jubilamento em ``_alunos_ativos``.

Cobrem a regra da USP (Regimento Geral, art. 75/76): o aluno é desligado se
não se matricular / não obtiver nenhum crédito em dois semestres consecutivos.
Alunos reais com evidência HISTPROGGR passam pelo ramo de evidência; o backstop
só atinge quem **não** tem HISTPROGGR ≤ Dia D no cache (os "fantasmas").

Roda **sem banco/túnel** (dados sintéticos).
"""

from __future__ import annotations

import pandas as pd

from replicado.dataset_aluno import (
    DatasetAlunoConfig,
    _alunos_ativos,
    _sem_atividade_recente,
)

SEM = 20241
DTA_CORTE = pd.Timestamp("2024-06-01")


def _cfg(**kw) -> DatasetAlunoConfig:
    base = dict(codundclg=45, prefixos=("MAC",), sufixo_min=40, ano_min=2018,
                ano_max=2024)
    base.update(kw)
    return DatasetAlunoConfig.from_env(**base)


def _cand() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "codpes": [101, 102, 103, 104, 105],
            "codpgm": [1, 1, 1, 1, 1],
            "codcur": [45024] * 5,
            "codhab": [4] * 5,
            "dtaclcgru": [pd.NaT] * 5,
            "dtaing": [
                pd.Timestamp("2023-02-10"),   # novo (últimos 2 semestres)
                pd.Timestamp("2020-02-10"),   # velho, crédito recente
                pd.Timestamp("2020-02-10"),   # velho, sem atividade → jubilado
                pd.Timestamp("2020-02-10"),   # velho, evidência E (morto)
                pd.Timestamp("2020-02-10"),   # velho, evidência H (vivo)
            ],
            "_stapgm_pit": [pd.NA, pd.NA, pd.NA, "E", "H"],
        }
    )


def _hist() -> pd.DataFrame:
    # Apenas o aluno 102 tem crédito aprovado no semestre t-1 (20232),
    # consolidado antes do Dia D.
    return pd.DataFrame(
        {
            "codpes": [102],
            "coddis": ["MAC0101"],
            "codtur": ["20232MAC0101-41"],
            "rstfim": ["A"],
            "dtaultalt": [pd.Timestamp("2024-01-20")],
            "dtacrihst": [pd.Timestamp("2023-08-05")],
            "ano_sem": [20232],
        }
    )


def test_sem_atividade_recente() -> None:
    cand = _cand()
    ativo = _sem_atividade_recente(cand, _hist(), SEM, DTA_CORTE)
    # 101 (novo) e 102 (crédito recente) → ativos; 103/104/105 → não.
    assert list(ativo) == [True, True, False, False, False]


def test_alunos_ativos_remove_fantasmas() -> None:
    cfg = _cfg()
    dados = {
        "habilprog": _cand().drop(columns=["_stapgm_pit"]),
        "histprog_unidade": pd.DataFrame(
            {
                "codpes": [104, 105],
                "codpgm": [1, 1],
                "dtaoco": [pd.Timestamp("2023-03-01")] * 2,
                "stapgm": ["E", "H"],
            }
        ),
        "hist_aluno": _hist(),
    }
    out = _alunos_ativos(cfg, dados, SEM, DTA_CORTE)
    ativos = set(out["codpes"].astype(int))
    # 101 (novo), 102 (crédito recente) e 105 (evidência H) ativos.
    assert ativos == {101, 102, 105}
    # 103 (jubilado por inatividade) e 104 (evidência E) removidos.
    assert 103 not in ativos and 104 not in ativos


def test_alunos_ativos_preserva_sem_histprog_com_atividade() -> None:
    cfg = _cfg()
    dados = {
        "habilprog": _cand().drop(columns=["_stapgm_pit"]),
        "histprog_unidade": pd.DataFrame(),  # sem evidência PIT para ninguém
        "hist_aluno": _hist(),
    }
    out = _alunos_ativos(cfg, dados, SEM, DTA_CORTE)
    ativos = set(out["codpes"].astype(int))
    assert 101 in ativos and 102 in ativos
    assert 103 not in ativos
