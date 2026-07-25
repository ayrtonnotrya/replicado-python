"""
Máquina do Tempo do `estmtr` (Estimativa de Matrículas - Júpiter).

Objetivo
--------
Reconstruir, via engenharia reversa no Replicado, o valor do `estmtr` que o
Júpiter exibia ~1 semana antes do início das aulas de cada turma, validando
contra o ground truth `temp/turmas_internas.csv` (2022-2026).

Motivação
---------
Nas tabelas consolidadas (TURMAGR) os contadores "colapsam" com o tempo:
inscrições (numins) e pré-matrículas (numpmt*) viram matrículas (nummtr*) ou
são zerados. Usar os contadores atuais como proxy do estmtr para anos antigos
causaria data leakage (o alvo nummtr contamina a feature).

A regra descoberta (validada contra o ground truth)
---------------------------------------------------
    estmtr(turma) = | HISTESCOLARGR.dtacrihst <= dtainitur - 5 dias |

ou seja: CONTE TODOS os registros de matrícula criados até 5 dias antes do
início das aulas, SEM filtro de situação atual (stamtr). Justificativas:

1. `dtacrihst` é o carimbo de criação do registro (inscrição, pré-matrícula
   ou matrícula) — é a nossa "máquina do tempo": só existiam na data de
   corte os registros com dtacrihst <= corte.

2. NÃO se deve filtrar por stamtr: registros hoje excluídos (E/R) estavam
   ativos na data de corte e ENTRAVAM no estmtr (a exclusão ocorreu depois).
   Filtrar stamtr degrada o MAE de 2.65 para 6.24.

3. O offset de 4-5 dias cai na "janela morta" entre o fim da matrícula dos
   veteranos e a carga em lote dos calouros (visível como um salto brusco na
   curva acumulada de registros ~1 dia antes das aulas nos 1os semestres).
   Offsets <= 3 dias são catastróficos nos semestres ímpares (1S) porque
   capturam a carga dos calouros (MAE salta de ~2.7 para ~12).

4. HIPÓTESE DE NEGÓCIO VALIDADA: o estmtr não contabiliza ingressantes
   (FUVEST/SISU) porque seus registros são criados por carga (aplori='C')
   ~0-1 dias antes das aulas — DEPOIS de qualquer corte na janela morta.
   Não é um filtro explícito do Júpiter; é um efeito temporal.

5. Piso irredutível: em ~15% das turmas, estmtr > total de registros que
   existem HOJE no HISTESCOLARGR. Inscrições rejeitadas no processamento
   (ex.: exclusão por requisito faltante, ver TIPODATAGR 'Excl s/req') são
   fisicamente deletadas e não deixam rastro no Replicado — erro
   irrecuperável por qualquer método baseado no banco atual.

Observações de calibração
-------------------------
- O offset ótimo por semestre varia (1S: 4-17 dias; 2S: 1-5 dias), refletindo
  a data em que o ground truth foi extraído a cada semestre. Para gerar a
  base histórica (2015-2021), recomenda-se o offset fixo de 5 dias: robusto
  em todos os semestres do GT e imune à carga de calouros.
- Apenas turmas com sufixo do codtur >= 40 são avaliadas (turmas "reais" de
  oferecimento; sufixos menores foram inseridos manualmente no GT).

Taxonomia dos outliers (|erro| > 10 alunos; ~5% das turmas)
-----------------------------------------------------------
Análise dos resíduos contra o GT revelou TRÊS padrões disjuntos:

1. SUBESTIMADOS (rec < estmtr) — 100% irredutíveis:
   em TODOS, estmtr > total de registros que existem hoje no HISTESCOLARGR.
   Inscrições rejeitadas no processamento (ex.: 'Excl s/req') são fisicamente
   deletadas e não deixam rastro no Replicado. Sem solução pelo banco atual.

2. SUPERESTIMADOS (rec > estmtr) — captura precoce do GT:
   a data implícita do estmtr (quando a contagem acumulada atinge o valor do
   GT) concentra-se em 42-49 dias ANTES das aulas (mediana: -45d), ou seja,
   esses estmtr foram registrados no INÍCIO da janela de inscrições, não 1
   semana antes das aulas. Fortemente concentrado no semestre 20251 (17 dos
   33 casos) e em disciplinas optativas de alta rotatividade (MAC0425/427/460,
   MAE0515). É ruído do ground truth, não do modelo.

3. SWAPS ENTRE TURMAS IRMÃS — ruído de alocação por turma:
   em disciplinas multi-turma (ex.: MAC0122/20222 e /20232), os erros por
   turma se cancelam no agregado (ex.: 20232: GT 93/31/120 vs rec 58/54/121;
   soma 244 vs 233). A distribuição de alunos entre turmas irmãs ainda estava
   em fluxo quando o GT foi capturado. Para o modelo de ML, features
   agregadas por (disciplina, semestre) são imunes a esse ruído.

Consequência para a coleta futura (pipeline padronizado, sexta de manhã):
- Capturar o estmtr CEDO na sexta de manhã: nos 1os semestres a carga dos
  calouros acontece sexta/sábado (salto na curva acumulada 1-2 dias antes das
  aulas). Extrair antes dessa carga mantém o estmtr na "janela morta".
- Com a coleta padronizada, os padrões 2 e 3 desaparecem; o padrão 1 é
  inerente ao Replicado e só afeta o BACKFILL histórico (2015-2021).

Uso
---
    poetry run python scripts/maquina_tempo_estmtr.py
    poetry run python scripts/maquina_tempo_estmtr.py --sem-cache
    poetry run python scripts/maquina_tempo_estmtr.py --dias-corte 7
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from replicado.connection import DB  # noqa: E402

GT_PATH = Path("temp/turmas_internas.csv")
CACHE_DIR = Path("temp/cache_maquina_tempo")
SAIDA_PATH = Path("temp/validacao_estmtr.csv")

DIAS_CORTE = 5  # offset robusto identificado na validação (ver docstring)
SUFIXO_MIN = 40  # só turmas com sufixo >= 40 importam para a validação


# ---------------------------------------------------------------------------
# Carga de dados
# ---------------------------------------------------------------------------
def carregar_ground_truth() -> pd.DataFrame:
    gt = pd.read_csv(GT_PATH, dtype={"coddis": str, "codtur": str, "estmtr": str})
    # Normalizações defensivas: o CSV contém anotações manuais ('MAC0110*',
    # 'CinIME' com case misto) e linhas sem estmtr (turmas inseridas
    # manualmente), que não servem para validação.
    gt["coddis"] = gt["coddis"].str.strip().str.replace("*", "", regex=False).str.upper()
    gt["codtur"] = gt["codtur"].str.strip()
    gt = gt[gt["codtur"].str.match(r"^20\d{5}$")]  # AAAASTT
    gt = gt[gt["estmtr"].notna() & gt["estmtr"].str.match(r"^\d+$", na=False)]
    gt = gt.drop_duplicates(subset=["coddis", "codtur"], keep="first")
    gt["estmtr"] = gt["estmtr"].astype(int)
    gt["sufixo"] = gt["codtur"].str[-2:].astype(int)
    gt["sem"] = gt["codtur"].str[:5]
    return gt.reset_index(drop=True)


def extrair_turmagr(gt: pd.DataFrame, usar_cache: bool = True) -> pd.DataFrame:
    """Extrai TURMAGR (metadados das turmas, incl. dtainitur) dos anos do GT."""
    cache = CACHE_DIR / "turmagr_gt.pkl"
    if usar_cache and cache.exists():
        return pd.read_pickle(cache)

    anos = sorted(gt["codtur"].str[:4].unique())
    like_anos = " OR ".join(f"t.codtur LIKE '{a}%'" for a in anos)
    rows = DB.fetch_all(
        f"""
        SELECT t.coddis, t.verdis, t.codtur, t.tiptur,
               t.dtainitur, t.dtafimtur, t.statur, t.dtacritur,
               t.numvagtur, t.numvagopt, t.numvagoptlre, t.numvagturcpl, t.numvagecr,
               t.numins, t.numinsopt, t.numinsoptlre, t.numinscpl, t.numinsecr,
               t.numpmtobg, t.numpmtopt, t.numpmtoptlre, t.numpmtcpl, t.numpmtecr,
               t.nummtr, t.nummtropt, t.nummtroptlre, t.nummtrturcpl, t.nummtrecr
        FROM TURMAGR t
        WHERE {like_anos}
        """
    )
    df = pd.DataFrame(rows)
    df["coddis"] = df["coddis"].str.strip().str.upper()
    df["codtur"] = df["codtur"].str.strip()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_pickle(cache)
    return df


def extrair_histescolar(gt: pd.DataFrame, usar_cache: bool = True) -> pd.DataFrame:
    """Extrai HISTESCOLARGR (microdados das matrículas) dos anos do GT."""
    cache = CACHE_DIR / "histescolar_gt.pkl"
    if usar_cache and cache.exists():
        return pd.read_pickle(cache)

    anos = sorted(gt["codtur"].str[:4].unique())
    like_anos = " OR ".join(f"h.codtur LIKE '{a}%'" for a in anos)
    rows = DB.fetch_all(
        f"""
        SELECT h.codpes, h.codpgm, h.coddis, h.verdis, h.codtur,
               h.dtacrihst, h.stamtr, h.stacrihstesc, h.dtaultalt,
               h.discrl, h.aplori, h.rstfim
        FROM HISTESCOLARGR h
        WHERE {like_anos}
        """
    )
    df = pd.DataFrame(rows)
    df["coddis"] = df["coddis"].str.strip().str.upper()
    df["codtur"] = df["codtur"].str.strip()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_pickle(cache)
    return df


# ---------------------------------------------------------------------------
# Núcleo da Máquina do Tempo
# ---------------------------------------------------------------------------
def reconstruir_estmtr(
    turmas: pd.DataFrame,
    hist: pd.DataFrame,
    dias_corte: int = DIAS_CORTE,
) -> pd.DataFrame:
    """
    estmtr reconstruído = nº de registros de HISTESCOLARGR com
    dtacrihst <= dtainitur - dias_corte, sem filtro de situação atual.

    Turmas do GT não encontradas na TURMAGR (inseridas manualmente) são
    simplesmente ignoradas (não participam da validação).
    """
    base = turmas[["coddis", "verdis", "codtur", "dtainitur"]].copy()
    base["dtainitur"] = pd.to_datetime(base["dtainitur"])
    base["dta_corte"] = base["dtainitur"] - pd.Timedelta(days=dias_corte)

    h = hist.copy()
    h["dtacrihst"] = pd.to_datetime(h["dtacrihst"])
    h = h.merge(
        base[["coddis", "codtur", "dta_corte"]], on=["coddis", "codtur"], how="inner"
    )

    est = (
        h[h["dtacrihst"] <= h["dta_corte"]]
        .groupby(["coddis", "codtur"])
        .size()
        .rename("estmtr_rec")
        .reset_index()
    )
    out = base.merge(est, on=["coddis", "codtur"], how="left").fillna({"estmtr_rec": 0})
    out["estmtr_rec"] = out["estmtr_rec"].astype(int)
    return out


# ---------------------------------------------------------------------------
# Avaliação contra o ground truth
# ---------------------------------------------------------------------------
def avaliar(rec: pd.DataFrame, gt: pd.DataFrame) -> pd.DataFrame:
    gt_val = gt[gt["sufixo"] >= SUFIXO_MIN]
    cmp = gt_val.merge(
        rec[["coddis", "codtur", "estmtr_rec", "dtainitur", "dta_corte"]],
        on=["coddis", "codtur"],
        how="inner",  # turmas não encontradas no Replicado são ignoradas
    )
    cmp["erro"] = cmp["estmtr_rec"] - cmp["estmtr"]

    print(f"\n=== VALIDAÇÃO vs GROUND TRUTH (sufixo>={SUFIXO_MIN}, n={len(cmp)}) ===")
    print(f"Acurácia exata : {(cmp['erro'] == 0).mean():.1%}")
    print(f"|erro| <= 2    : {(cmp['erro'].abs() <= 2).mean():.1%}")
    print(f"MAE            : {cmp['erro'].abs().mean():.2f} alunos")
    print(f"RMSE           : {(cmp['erro'] ** 2).mean() ** 0.5:.2f} alunos")
    print(f"Correlação     : {cmp[['estmtr','estmtr_rec']].corr().iloc[0,1]:.3f}")

    print("\nErro por semestre:")
    tab = (
        cmp.groupby("sem")["erro"]
        .agg(n="size", mae=lambda s: s.abs().mean(),
             exato=lambda s: (s == 0).mean(),
             dentro_2=lambda s: (s.abs() <= 2).mean(),
             outliers_10=lambda s: (s.abs() > 10).sum())
        .round(3)
    )
    print(tab.to_string())

    print("\nPiores 15 desvios:")
    print(
        cmp.reindex(cmp["erro"].abs().sort_values(ascending=False).index)
        .head(15)[["coddis", "codtur", "estmtr", "estmtr_rec", "erro", "dta_corte"]]
        .to_string(index=False)
    )
    return cmp


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sem-cache", action="store_true",
                        help="Ignora o cache e reextrai do Replicado")
    parser.add_argument("--dias-corte", type=int, default=DIAS_CORTE,
                        help=f"Dias antes do início das aulas (default: {DIAS_CORTE})")
    args = parser.parse_args()

    gt = carregar_ground_truth()
    print(f"Ground truth: {len(gt)} turmas com estmtr "
          f"({gt['sem'].min()} a {gt['sem'].max()}); "
          f"{(gt['sufixo'] >= SUFIXO_MIN).sum()} com sufixo>={SUFIXO_MIN}")

    turmas = extrair_turmagr(gt, usar_cache=not args.sem_cache)
    print(f"TURMAGR extraída: {len(turmas)} turmas")

    hist = extrair_histescolar(gt, usar_cache=not args.sem_cache)
    print(f"HISTESCOLARGR extraída: {len(hist)} registros de matrícula")

    rec = reconstruir_estmtr(turmas, hist, dias_corte=args.dias_corte)
    cmp = avaliar(rec, gt)
    cmp.to_csv(SAIDA_PATH, index=False)
    print(f"\nDetalhe por turma salvo em {SAIDA_PATH}")


if __name__ == "__main__":
    main()
