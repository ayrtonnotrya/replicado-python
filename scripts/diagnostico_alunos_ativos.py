"""Diagnóstico do bug de data leakage em ``_alunos_ativos``.

Demonstra (Teste 1, ponto 1 do chamado) que a condicional atual
(``dtaclcgru`` ausente/posterior ao Dia D) retém como "ativos" alunos cuja
verdade temporal point-in-time (extraída da ``HISTPROGGR``) mostra evasão/
trancamento ANTES do Dia D — os "alunos fantasmas".

Roda **somente sobre o cache local** (``temp/cache_maquina_tempo/``): não
precisa do túnel SSH nem do banco. Por padrão analisa 2018.1; accept ``--sem
<ano><dígito>`` (ex ``20222``).

Confronta, para o ``dta_corte`` do semestre alvo, três cortes de "ativos":

1. **Corte A atual (buggy)** — só ``dtaclcgru``:
   ``dtaing<dta_corte & (dtaclcgru isna | dtaclcgru>dta_corte)``
2. **Corte B fallback sugerido** — ``HABILPROGGR.dtafim``:
   adiciona ``dtafim isna | dtafim>dta_corte``.
3. **Corte C correto (point-in-time)** — última HISTPROGGR ≤ ``dta_corte``:
   ativo só se ``stapgm ∈ {'A','R'}`` (sem.gateway temporal).

Corte A − Corte C = alunos fantasmas (evasão histórica mascarada de ativa).

Uso
---
    poetry run python scripts/diagnostico_alunos_ativos.py
    poetry run python scripts/diagnostico_alunos_ativos.py --sem 20181
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from replicado.dataset_alocacao import carregar_dados, filtrar_turmas
from replicado.dataset_aluno import MORTO_STAPGM, DatasetAlunoConfig

# Status mortos (evasão) — lógica por EXCLUSÃO, igual à de _alunos_ativos.
# ``H``/``EH`` (habilitação/ênfase) e ``A``/``R`` mantêm o programa vivo.
MORTO: frozenset[str] = MORTO_STAPGM


def _ultimo_stapgm_pit(
    histprog: pd.DataFrame, dta_corte: pd.Timestamp
) -> pd.DataFrame:
    """Snapshot point-in-time do último ``stapgm`` por ``(codpes, codpgm)`` no
    Dia D — lógica equivalente à do denominador de ``_macro_trancamento``.

    Filtra ``dtaoco <= dta_corte`` (NUNCA lê eventos futuros), ordena por
    ``dtaoco`` e retém a última linha de cada par ``(codpes, codpgm)``.
    """
    h = histprog.dropna(subset=["dtaoco"]).copy()
    if not len(h):
        return pd.DataFrame(columns=["codpes", "codpgm", "stapgm_pit"])
    h = h[h["dtaoco"] <= dta_corte]
    if not len(h):
        return pd.DataFrame(columns=["codpes", "codpgm", "stapgm_pit"])
    h = h.sort_values("dtaoco")
    ult = h.groupby(["codpes", "codpgm"], sort=False).tail(1)
    ult["stapgm_pit"] = ult["stapgm"].astype(str).str.strip()
    return ult[["codpes", "codpgm", "stapgm_pit"]]


def _corte_b_dtafim(
    habilprog: pd.DataFrame, dta_corte: pd.Timestamp
) -> pd.DataFrame:
    """Corte B (fallback sugerido no chamado): filtro por ``HABILPROGGR.dtafim``.

    Como o cache atual não extrai ``dtafim`` (a query de ``dados['habilprog']``
    seleciona só ``codpes,codpgm,codcur,codhab,dtaclcgru,dtaing``), o Corte B
    fica idêntico ao Corte A — esta função retorna o Corte A e sinaliza a
    limitação para o relatório.
    """
    a = habilprog.copy()
    a = a[a["codpes"].notna()]
    a["codpes"] = a["codpes"].astype(int)
    ativos = a[
        (a["dtaing"] < dta_corte)
        & (a["dtaclcgru"].isna() | (a["dtaclcgru"] > dta_corte))
    ].copy()
    return ativos


def _resume(
    habilprog: pd.DataFrame,
    histprog: pd.DataFrame,
    dta_corte: pd.Timestamp,
    *,
    sem_alvo: int,
    cfg,
    dados,
    sem_turmas_global: pd.DataFrame | None = None,
    hist_aluno_global: pd.DataFrame | None = None,
) -> int:
    """Imprime a comprovação volumétrica do leakage."""
    # ---- Corte A: atual (buggy) — só dtaclcgru, todos os codpgm ----------
    raw_a = habilprog.copy()
    raw_a = raw_a[raw_a["codpes"].notna()]
    raw_a["codpes"] = raw_a["codpes"].astype(int)
    raw_a = raw_a[
        (raw_a["dtaing"] < dta_corte)
        & (raw_a["dtaclcgru"].isna() | (raw_a["dtaclcgru"] > dta_corte))
    ][["codpes", "codpgm", "codcur", "codhab"]].drop_duplicates()
    n_total = len(raw_a)
    n_total_codpes = int(raw_a["codpes"].nunique())

    # ``codpgm`` na PROGRAMAGR é o NÚMERO de (re)ingresso da pessoa na USP
    # (1=1º ingresso, 2=reingresso, ...), NÃO o tipo de programa. Alunos
    # reingressantes (codpgm>=2) são LEGITIMAMENTE ativos no Dia D e não
    # devem ser tratados como leakage — aqui reportados só p/ auditoria.
    n_reingresso = int((raw_a["codpgm"].astype("Int64", errors="ignore") >= 2)
                       .sum()) if "codpgm" in raw_a.columns else 0

    # ---- Verdade temporal point-in-time (HISTPROGGR) ---------------------
    pit = _ultimo_stapgm_pit(histprog, dta_corte)
    cruz = raw_a.merge(pit, on=["codpes", "codpgm"], how="left")
    n_sem_hist = int(cruz["stapgm_pit"].isna().sum())
    n_com_hist = n_total - n_sem_hist

    # "Fantasmas" (lógica de EXCLUSÃO): último stapgm point-in-time É um
    # status morto ∈ {E,T,S}. ``H``/``EH``/``A``/``R`` mantêm o programa vivo
    # — exigir ∈ {A,R} (versão anterior) deletava veteranos cujo último
    # evento é ``H`` (escolha/troca de habilitação), desabando os y=1.
    nao_ativo = cruz[cruz["stapgm_pit"].notna() & cruz["stapgm_pit"].isin(MORTO)]
    n_fantasmas = len(nao_ativo)
    encerrados = int((cruz["stapgm_pit"] == "E").sum())
    trancados = int((cruz["stapgm_pit"] == "T").sum())
    suspensos = int((cruz["stapgm_pit"] == "S").sum())
    # Quantos dos "fantasmas" estão no codpgm==1 (1ª grad abandonada)?
    n_fant_c1 = int((nao_ativo["codpgm"].astype("Int64", errors="ignore") == 1)
                    .sum()) if "codpgm" in nao_ativo.columns else 0

    # Distribuição do último stapgm point-in-time dos "ativos" do Corte A:
    dist = cruz["stapgm_pit"].fillna("(sem HISTPROGGR <= Dia D)").value_counts().to_dict()

    # ---- Refatoração aplicada (exclusão E/T/S + dtaclcgru backstop) ------
    from replicado.dataset_aluno import _alunos_ativos as _alunos_refatorado
    novos = _alunos_refatorado(cfg, dados, dta_corte)
    n_novos = len(novos)
    n_novos_codpes = int(novos["codpes"].nunique())
    n_evasao_dtaclcgru_pct = 100.0 * n_fantasmas / n_total if n_total else 0.0
    n_reducao_codpes_pct = 100.0 * (1 - n_novos_codpes / n_total_codpes) if n_total_codpes else 0.0

    # ---- Metrica de "Alunos Reais (>=1 matricula)" ---------------------
    # Cruza o elenco de ativos com as matriculas consolidadas do semestre
    # (HISTESCOLARGR vivo no pico de ocupacao da turma — _positivos_sem).
    # A metrica que testa a regressao: y=1 NAO pode cair apos a refatoracao.
    n_reais_refatorado = -1
    if sem_turmas_global is not None:
        codpes_set = set(novos["codpes"].astype(int))
        from replicado.dataset_aluno import _positivos_sem
        pos = _positivos_sem(cfg, hist_aluno_global, codpes_set, sem_turmas_global)
        n_reais_refatorado = int(pos["codpes"].nunique()) if len(pos) else 0
        # Mesma metrica sob o Corte A (buggy) para comparar baseline.
        codpes_a = set(raw_a["codpes"].astype(int))
        pos_a = _positivos_sem(cfg, hist_aluno_global, codpes_a, sem_turmas_global)
        n_reais_corte_a = int(pos_a["codpes"].nunique()) if len(pos_a) else 0
    else:
        n_reais_corte_a = -1

    print(f"=== Diagnóstico de leakage em _alunos_ativos — semestre {sem_alvo} ===")
    print(f"dta_corte (Dia D = dtainitur.min - dias_corte): {dta_corte.date()}")
    print(f"HABILPROGGR (cache): {len(habilprog):,} linhas")
    print(f"HISTPROGGR (cache):  {len(histprog):,} linhas")
    print()
    print("[Corte A - atual/buggy] 'ativos' (só dtaclcgru):")
    print(f"  linhagens (codpes,codcur,codhab): {n_total:,} | codpes unicos: {n_total_codpes:,}")
    print(f"  destes, reingressos (codpgm>=2, ativos legitimos): {n_reingresso:,}")
    print(f"  destes, com evidência HISTPROGGR <= Dia D: {n_com_hist:,}")
    print(f"  destes, SEM HISTPROGGR <= Dia D:            {n_sem_hist:,}")
    print()
    print(f"[FANTASMAS] último stapgm PIT ∈ {{E,T,S}}: {n_fantasmas:,}"
          f"  ({n_evasao_dtaclcgru_pct:.1f}% do Corte A)")
    print(f"  . stapgm='E' (Encerrado): {encerrados:,}")
    print(f"  . stapgm='T' (Trancado):  {trancados:,}")
    print(f"  . stapgm='S' (Suspenso):  {suspensos:,}")
    print(f"  . dos fantasmas, codpgm==1 (1a grad abandonada): {n_fant_c1:,}")
    print()
    print("Distribuição do último stapgm (PIT) entre os 'ativos' do Corte A:")
    for k, v in sorted(dist.items(), key=lambda kv: -kv[1]):
        print(f"  {k!s:28s} = {v:,}")
    print()
    print("[Corte B (fallback dtafim)] não aplicável: o cache de habilprog não")
    print("  contém dtafim (query seleciona só dtaclcgru/dtaing) — sem primazia")
    print("  temporal do dtafim. Solução definitiva = Corte C.")
    print()
    print("[Corte C - refatoração aplicada] _alunos_ativos(dados, Dia D):")
    print(f"  linhagens (codpes,codcur,codhab): {n_novos:,} | codpes unicos: {n_novos_codpes:,}")
    print(f"  redução vs Corte A: -{n_reducao_codpes_pct:.1f}% de codpes "
          f"(remove evadidos PIT E/T/S; preserva H/EH e reingressos codpgm>=2)")
    print()
    if n_reais_refatorado >= 0:
        print("[MÉTRICA ALUNOS REAIS — ≥1 matrícula consolidada no semestre]")
        print(f"  Corte A (buggy):   {n_reais_corte_a:,} codpes com y=1")
        print(f"  Corte C (refator): {n_reais_refatorado:,} codpes com y=1")
        delta = n_reais_refatorado - n_reais_corte_a
        sinal = "+" if delta >= 0 else ""
        print(f"  Δ y=1: {sinal}{delta:,}  (deve NÃO cair: ~1.400-1.500 por sem)")
    else:
        print("[MÉTRICA ALUNOS REAIS] indisponível (sem turmas_sem/hist_aluno).")
    print()
    print("Conclusão: a base atual incorporava evadidos/trancados da 1a")
    print("graduação (stapgm E/T/S) como ativos, preservando veteranos com")
    print("último evento H (habilitação) e reingressos codpgm>=2 — ativos.")
    return n_fantasmas


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sem", type=int, default=20181,
                    help="ano_sem alvo (ex 20181, 20222). Default 20181.")
    ap.add_argument("--cache", type=Path,
                    default=Path("temp/cache_maquina_tempo"),
                    help="diretório de cache (default temp/cache_maquina_tempo).")
    args = ap.parse_args(argv)

    cfg = DatasetAlunoConfig.from_env()
    dados = carregar_dados(cfg)
    # Histprog já é carregado/strip por carregar_macrosensores; garantia extra:
    h = dados.get("histprog_unidade")
    if h is None or not len(h):
        cache_h = args.cache / f"aux_histprog_unidade_{cfg.codundclg}.pkl"
        if cache_h.exists():
            h = pd.read_pickle(cache_h)
            h["dtaoco"] = pd.to_datetime(h["dtaoco"], errors="coerce")
            h["stapgm"] = h["stapgm"].astype(str).str.strip()
            dados["histprog_unidade"] = h
        else:
            print("ERRO: sem HISTPROGGR cacheada — rode carregar_dados_aluno antes.")
            return 2

    turmas = filtrar_turmas(cfg, dados["turmas"])
    prefix = str(args.sem)
    sem_turmas = turmas[turmas["codtur"].astype(str).str.startswith(prefix)]
    if not len(sem_turmas):
        print(f"ERRO: sem turmas para o semestre {args.sem} (prefix {prefix}).")
        return 2
    sem_turmas = sem_turmas.dropna(subset=["dtainitur"])
    if not len(sem_turmas):
        print("ERRO: turmas do semestre sem dtainitur.")
        return 2
    dta_corte = sem_turmas["dtainitur"].min() - pd.Timedelta(days=cfg.dias_corte)

    # Carrega HISTESCOLARGR-aluno (com notas) p/ métrica de matrículas reais.
    from replicado.dataset_aluno import _carregar_hist_aluno
    hist_aluno = _carregar_hist_aluno(cfg)

    n_fant = _resume(
        dados["habilprog"], h, dta_corte, sem_alvo=args.sem, cfg=cfg, dados=dados,
        sem_turmas_global=sem_turmas, hist_aluno_global=hist_aluno,
    )
    return 0 if n_fant >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
