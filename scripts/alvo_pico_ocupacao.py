"""
Alvo preditivo para alocação de salas: PICO DE OCUPAÇÃO (1ªs 2 semanas de aula).

Problema
--------
Para dimensionar salas, o que importa é a ocupação nas DUAS PRIMEIRAS SEMANAS
de aula (pico de presença). O nummtr consolidado SUBESTIMA esse pico (alunos
trancam/desistem ao longo do semestre) e o estmtr FALHA nos calouros do 1º
semestre (injetados por carga ~1 dia antes das aulas, depois do estmtr).

Este script NÃO depende de dados do momento da distribuição: o alvo é
extraído do Replicado histórico (2010+) e o modelo será treinado com as
features disponíveis na sexta anterior ao início das aulas (estmtr etc.).

Reconstrução (mesma "máquina do tempo" do estmtr, agora olhando PARA FRENTE)
----------------------------------------------------------------------------
    ocupacao(turma, D) = criados até D  -  excluídos até D

    criados até D   = | HISTESCOLARGR.dtacrihst <= D |
    excluídos até D = | stamtr IN ('E','R') AND dtaultalt <= D |

Sinais validados nos dados (2010-2026):
  - dtacrihst 100% preenchido desde 2010; dtaultalt 100% desde 2012;
  - trancamentos aparecem como stamtr='E' + rstfim='T' (~22k registros),
    com dtaultalt = data do trancamento → o "vazamento" pós-semana-2 é
    totalmente rastreável (diferente das inscrições rejeitadas, que são
    deletadas fisicamente e só afetam snapshots pré-aulas).

Alvos candidatos avaliados
--------------------------
  T_final   : nummtr consolidado (baseline — subestima o pico)
  T_d0..d21 : ocupacao reconstruída em marcos do início das aulas a +21 dias
  T_pico    : max(ocupacao) entre D+0 e D+21 — ALVO RECOMENDADO
              (ver conclusões impressas ao final)

Escopo: disciplinas do IME (prefixos 45/MAC/MAE/MAT/MAP/MPM — as 43xxxxx da
Física ministradas no IME ficam de fora a pedido do usuário), turmas com
sufixo >= 40, 2010+.

ARMADILHAS DA RÉPLICA (descobertas na validação):
  - Em 2023-2024 a TURMAGR local contém turmas de TODA a USP (~18k em 2023,
    prefixos CMU/RCG/FLC/LES...), SEM cobertura no HISTESCOLARGR local. Sem o
    filtro de escopo IME, a ocupação reconstruída sai zerada para elas.
  - A TURMAGR cobre turmas desde ~1980, mas o HISTESCOLARGR local só existe
    de 2010 em diante — série confiável: 2010+.
  - O último semestre do cache pode estar em andamento (consolidação
    incompleta): excluído das estatísticas via somente_concluidos().

Uso
---
    poetry run python scripts/extrair_cache_replicado.py   # 1x: baixa os dados
    poetry run python scripts/alvo_pico_ocupacao.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

CACHE_DIR = Path("temp/cache_maquina_tempo")
SAIDA = Path("temp/alvo_pico_ocupacao.csv")

MARCOS = [-5, 0, 7, 11, 14, 21]  # dias relativos ao início das aulas
DIA_PICO = 11  # sexta-feira da 2ª semana de aulas (início na segunda)

# Escopo: disciplinas do IME (codundclg=45). As numéricas 43xxxxx são da
# Física (IFUSP) ministradas no IME — ficam de fora a pedido do usuário.
# Validado nos dados: cobertura HISTESCOLARGR/TURMAGR = 1.00 nesses prefixos.
PREFIXOS_IME = ("45", "MAC", "MAE", "MAT", "MAP", "MPM")
SUFIXO_MIN = 40  # turmas "reais" de oferecimento (mesmo critério do estmtr)
ANOS = range(2010, 2027)


# ---------------------------------------------------------------------------
# Carga
# ---------------------------------------------------------------------------
def carregar() -> tuple[pd.DataFrame, pd.DataFrame]:
    turmas = pd.read_pickle(CACHE_DIR / "turmagr_full.pkl")
    hist = pd.concat(
        [pd.read_pickle(CACHE_DIR / f"histescolar_{a}.pkl") for a in ANOS],
        ignore_index=True,
    )
    return turmas, hist


# ---------------------------------------------------------------------------
# Núcleo: curva de ocupação por turma
# ---------------------------------------------------------------------------
def construir_curva_ocupacao(turmas: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    t = turmas.copy()
    t["dtainitur"] = pd.to_datetime(t["dtainitur"])
    # Escopo: disciplinas IME, codtur padrão AAAASTT com sufixo >= 40,
    # tipmtr normal e data de início conhecida.
    t = t[t["coddis"].str.startswith(PREFIXOS_IME)]
    t = t[t["codtur"].str.match(r"^20\d{5}$")]
    t = t[t["codtur"].str[:4].astype(int).isin(ANOS)]  # cobertura do HIST
    t = t[t["codtur"].str[-2:].astype(int) >= SUFIXO_MIN]
    t = t[t["dtainitur"].notna()]
    t = t[t["tipmtr"].isin(["N", None]) | t["tipmtr"].isna()]

    h = hist.merge(
        t[["coddis", "codtur", "dtainitur"]], on=["coddis", "codtur"], how="inner"
    )
    h["cri_d"] = (pd.to_datetime(h["dtacrihst"]) - h["dtainitur"]).dt.days
    exc = h["stamtr"].isin(["E", "R"])
    h["exc_d"] = (pd.to_datetime(h["dtaultalt"]) - h["dtainitur"]).dt.days.where(exc)

    out = t[["coddis", "verdis", "codtur", "tiptur", "dtainitur", "statur",
             "numvagtur", "numvagopt", "numvagoptlre", "numvagturcpl", "numvagecr",
             "nummtr", "nummtropt", "nummtroptlre", "nummtrturcpl", "nummtrecr"]].copy()

    for d in MARCOS:
        cri = h[h["cri_d"] <= d].groupby(["coddis", "codtur"]).size()
        exc_ate = h[h["exc_d"].notna() & (h["exc_d"] <= d)].groupby(["coddis", "codtur"]).size()
        col = pd.DataFrame({"cri": cri, "exc": exc_ate}).fillna(0)
        out = out.merge(
            (col["cri"] - col["exc"]).rename(f"ocup_d{d:+d}"),
            left_on=["coddis", "codtur"], right_index=True, how="left",
        )
        out[f"ocup_d{d:+d}"] = out[f"ocup_d{d:+d}"].fillna(0).astype(int)

    out["nummtr_final"] = out[["nummtr", "nummtropt", "nummtroptlre",
                               "nummtrturcpl", "nummtrecr"]].fillna(0).sum(axis=1).astype(int)
    # T_pico: máximo de ocupação por turma entre o início das aulas e o fim
    # da 3ª semana. O argmax modal é D+0 (a turma esvazia depois do início);
    # ocup_d+11 fica a ~1 aluno do máximo, mas T_pico nunca subestima.
    cols_pico = [f"ocup_d{d:+d}" for d in MARCOS if d >= 0]
    out["pico_max"] = out[cols_pico].max(axis=1)
    out["ano"] = out["codtur"].str[:4].astype(int)
    out["sem_tipo"] = out["codtur"].str[4].map({"1": "1S", "2": "2S"})
    out["sufixo"] = out["codtur"].str[-2:].astype(int)
    return out


# ---------------------------------------------------------------------------
# Análises
# ---------------------------------------------------------------------------
def somente_concluidos(df: pd.DataFrame) -> pd.DataFrame:
    """Remove o semestre em andamento (último semestre do cache pode estar
    com consolidação incompleta, ex.: 20262)."""
    sem_max = df["codtur"].str[:5].max()
    return df[df["codtur"].str[:5] < sem_max]


def analise_curva_media(df: pd.DataFrame) -> None:
    print("\n" + "=" * 72)
    print("A1. CURVA MÉDIA DE OCUPAÇÃO (por turma ativa) — decaimento pós-pico")
    print("=" * 72)
    ativos = somente_concluidos(df[df["nummtr_final"] > 0])
    cols = [f"ocup_d{d:+d}" for d in MARCOS] + ["pico_max", "nummtr_final"]
    tab = ativos.groupby("sem_tipo")[cols].mean().round(1)
    tab.columns = ["D-5(estmtr)", "D0", "D+7", "D+11", "D+14", "D+21",
                   "pico_max", "nummtr_final"]
    print(tab.to_string())
    print("\nLeitura: o pico médio ocorre nas 2 primeiras semanas; nummtr_final")
    print("fica abaixo dele — é o alvo errado para dimensionar sala.")


def analise_uplift(df: pd.DataFrame) -> None:
    print("\n" + "=" * 72)
    print("A2. UPLIFT DO PICO vs nummtr_final  (T_pico / T_final)")
    print("=" * 72)
    ativos = somente_concluidos(df[df["nummtr_final"] >= 5]).copy()
    ativos["uplift"] = ativos["pico_max"] / ativos["nummtr_final"]
    print("Distribuição do uplift (pico_max / nummtr_final):")
    print(ativos["uplift"].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9]).round(3).to_string())
    print("\nUplift mediano por ano (estabilidade temporal — chave p/ treinar 2010+):")
    print(ativos.groupby(["ano", "sem_tipo"])["uplift"].median().unstack().round(3).to_string())
    print("\n% de turmas onde nummtr_final SUBESTIMA o pico em >10%:",
          f"{(ativos['uplift'] > 1.10).mean():.1%}")
    print("Subestimativa agregada do nummtr_final:",
          f"{1 - ativos['nummtr_final'].sum() / ativos['pico_max'].sum():.1%}")


def analise_trancamentos(hist: pd.DataFrame, turmas: pd.DataFrame) -> None:
    print("\n" + "=" * 72)
    print("A3. QUANDO OCORREM AS EXCLUSÕES (stamtr=E/R) em relação ao início das aulas")
    print("=" * 72)
    t = turmas[["coddis", "codtur", "dtainitur"]].copy()
    t["dtainitur"] = pd.to_datetime(t["dtainitur"])
    t = t[t["coddis"].str.startswith(PREFIXOS_IME)]
    t = t[t["codtur"].str.match(r"^20\d{5}$")]
    t = t[t["codtur"].str[-2:].astype(int) >= SUFIXO_MIN]
    h = hist.merge(t.dropna(subset=["dtainitur"]), on=["coddis", "codtur"], how="inner")
    exc = h[h["stamtr"].isin(["E", "R"])].copy()
    exc["dias"] = (pd.to_datetime(exc["dtaultalt"]) - exc["dtainitur"]).dt.days
    exc = exc[exc["dias"].between(-60, 120)]
    bins = [-60, -15, -5, 0, 7, 11, 14, 21, 30, 45, 60, 90, 120]
    labels = ["<-15", "-15..-5", "-5..0", "0..7", "7..11", "11..14", "14..21",
              "21..30", "30..45", "45..60", "60..90", "90..120"]
    dist = pd.cut(exc["dias"], bins=bins, labels=labels).value_counts().sort_index()
    tab = pd.DataFrame({"n_exclusoes": dist, "%": (dist / len(exc) * 100).round(1)})
    print(tab.to_string())
    print("\nLeitura: exclusões ANTES de D+11 já estavam fora da sala; as DEPOIS")
    print("são o 'vazamento' que faz o nummtr_final subestimar o pico real.")


def analise_previsibilidade(df: pd.DataFrame) -> None:
    print("\n" + "=" * 72)
    print("A4. PREVISIBILIDADE: correlação de cada alvo com o estmtr (D-5)")
    print("   (o modelo terá o estmtr na sexta pré-distribuição)")
    print("=" * 72)
    ativos = somente_concluidos(df[(df["nummtr_final"] > 0) & (df["ano"] >= 2015)]).copy()
    for alvo in ["ocup_d-5", "ocup_d+0", "ocup_d+7", "ocup_d+11", "ocup_d+14",
                 "ocup_d+21", "pico_max", "nummtr_final"]:
        r = ativos[["ocup_d-5", alvo]].corr().iloc[0, 1]
        print(f"  corr(estmtr_D-5, {alvo:12s}) = {r:.3f}")
    x = ativos["ocup_d-5"].values
    y = ativos["pico_max"].values
    b = pd.Series(y).cov(pd.Series(x)) / pd.Series(x).var()
    a = y.mean() - b * x.mean()
    resid = y - (a + b * x)
    print(f"\n  Ajuste linear pico_max ~ estmtr: pico ≈ {a:.1f} + {b:.2f}*estmtr "
          f"(MAE={abs(resid).mean():.1f} alunos)")


def analise_calouros(df: pd.DataFrame) -> None:
    print("\n" + "=" * 72)
    print("A5. O PROBLEMA DOS CALOUROS: estmtr vs pico em turmas de 1º semestre")
    print("=" * 72)
    ativos = somente_concluidos(df[df["nummtr_final"] > 0]).copy()
    ativos["gap_calouro"] = ativos["pico_max"] - ativos["ocup_d-5"]
    grp = ativos.groupby(["sem_tipo"])[["ocup_d-5", "pico_max", "gap_calouro"]].mean().round(1)
    grp.columns = ["estmtr(D-5)", "pico_max", "gap"]
    print(grp.to_string())
    print("\nLeitura: no 1S o pico supera o estmtr (carga dos calouros); no 2S o")
    print("gap é menor. O modelo precisa aprender esse gap — e o alvo T_pico o")
    print("captura corretamente, diferente do nummtr_final.")


def analise_argmax(df: pd.DataFrame) -> None:
    print("\n" + "=" * 72)
    print("A6. EM QUE MOMENTO OCORRE O PICO DE CADA TURMA (argmax D0..D21)")
    print("=" * 72)
    ativos = somente_concluidos(df[df["nummtr_final"] > 0]).copy()
    cols = [f"ocup_d{d:+d}" for d in MARCOS if d >= 0]
    ativos["argmax"] = ativos[cols].idxmax(axis=1)
    print(ativos.groupby(["sem_tipo", "argmax"]).size().unstack(fill_value=0).to_string())
    gap11 = (ativos["pico_max"] - ativos["ocup_d+11"]).mean()
    print(f"\nCusto de usar D+11 fixo em vez do máximo: {gap11:.2f} alunos/turma (média)")


def conclusao(df: pd.DataFrame) -> None:
    ativos = somente_concluidos(df[df["nummtr_final"] > 0])
    pico = ativos["pico_max"]
    fin = ativos["nummtr_final"]
    print("\n" + "=" * 72)
    print("CONCLUSÃO — ALVO RECOMENDADO")
    print("=" * 72)
    print(f"""
  ALVO: T_pico = max(ocupacao(turma, D)) para D em [dtainitur, dtainitur+21]
        ocupacao(D) = (registros criados até D) - (exclusões com dtaultalt <= D)

  Por quê:
  1. O argmax modal é o PRÓPRIO INÍCIO DAS AULAS (D+0): as turmas nascem
     cheias e só esvaziam. O máximo garante que a sala nunca será
     subdimensionada; um marco fixo (D+11) ficaria a ~1 aluno do máximo.
  2. 100% reconstruível desde 2010 (dtacrihst/dtaultalt sem nulos;
     cobertura HISTESCOLARGR/TURMAGR = 1.00 nas disciplinas IME);
  3. Trancamentos DEIXAM RASTRO (stamtr='E', rstfim='T', dtaultalt) — o alvo
     não sofre do piso irredutível que afetou o estmtr;
  4. Nas turmas ativas do histórico: pico mediano = {pico.median():.0f},
     nummtr_final mediano = {fin.median():.0f} → o alvo atual subestima
     {(1 - fin.sum()/pico.sum()):.1%} da ocupação no agregado, e em
     {((pico / fin.clip(lower=1)) > 1.10).mean():.1%} das turmas a
     subestimativa individual passa de 10%.

  Uso no treino: X = features da sexta pré-aulas (estmtr, histórico da
  disciplina, semestre, vagas...), y = T_pico. Para alocação com margem de
  segurança, prever quantil alto (ex.: q90 com LightGBM quantile) em vez da
  média — salas não podem estourar.
""")


def main() -> None:
    turmas, hist = carregar()
    print(f"TURMAGR: {len(turmas)} | HISTESCOLARGR: {len(hist)}")
    df = construir_curva_ocupacao(turmas, hist)
    print(f"Turmas com curva reconstruída: {len(df)}")

    analise_curva_media(df)
    analise_uplift(df)
    analise_trancamentos(hist, turmas)
    analise_previsibilidade(df)
    analise_calouros(df)
    analise_argmax(df)
    conclusao(df)

    df.to_csv(SAIDA, index=False)
    print(f"Dataset por turma salvo em {SAIDA}")


if __name__ == "__main__":
    sys.exit(main())
