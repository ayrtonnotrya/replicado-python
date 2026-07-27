"""
Macro-features semestrais (sinais globais de crise/caos) para o dataset de
alocação.

Quatro macro-sensores durante o **Dia D** (``dtainitur - dias_corte``), o
instante em que o modelo faz sua previsão:

1.  ``macro_gap_calendario_dias`` — **compressão do calendário**: diferença,
    em dias, entre o ``dtainitur`` de T0 e o final real (com recuperação) do
    semestre anterior T-1. Semestre<T-1> que termina em/depois do início de
    T0 indica greve/calendário comprimido (gaps negativos ou muito curtos).

2.  ``macro_requerimentos_30d_pre_dia_d`` — **termômetro da burocracia**:
    contagem de requerimentos da unidade (undergrad, ``codpgm=1``) criados
    nos 30 dias que antecedem o Dia D da turma. ``dtacadrqm`` é imutável →
    barreira temporal segura.

3.  ``macro_frac_atraso_notas_T1`` — **atraso sistêmico de notas**: fração
    das matrículas do semestre anterior T-1 ainda **não consolidadas** no
    Dia D do semestre atual T0. "Não consolidada" = ``dtaultalt > Dia D``
    (a linha foi alterada depois do Dia D → conteúdo desconhecido naquele
    instante, portanto a nota final não estava disponível).

4.  ``macro_trancamentos_3m_pre_dia_d`` e ``macro_taxa_trancamento_90d`` —
    **taxa global de trancamento de curso**: nº de programas da unidade
    (undergrad) que migraram para ``stapgm='T'`` (trancamento) nos 90 dias
    que antecedem o Dia D, e a razão pelo número de programas da unidade
    ativos no snapshot do Dia D (``stapgm='A'`` ou ``'R'`` no último evento
    ``dtaoco<=Dia D``).

Princípios de sobrevivência temporal (a regra do Dia D):

*   Eventos cuja timestamp é **imutável** (``REQUERIMENTOGR.dtacadrqm``,
    ``HISTPROGGR.dtaoco``, ``HISTESCOLARGR.dtacrihst``) são filtrados
    estritamente por ``<= Dia D(T0)`` — valem como "foto do banco naquele
    instante".
*   Eventos que consolidam DEPOIS do Dia D (``HISTESCOLARGR.dtaultalt``) só
    são vistos no futuro; aqui eles são tratados como " ещё não existentes"
    —Ou seja, contribuem para o "atraso" da H3, ou são silenciados pela
    barreira (vide :func:`features_macrosensores`).
*   A CALENDGR é cadastrall (``stacld``/``atzcld``): ler suas datas para
    semestres passados não vaza futuro, pois a data é registrada quando o
    calendário daquele semestre é definido.

Escopo da unidade (``codundclg``): todos os filtros de student membership
passam pelo conjunto ``codpes`` derivado de ``HABILPROGGR`` (já em ``dados``
após ``carregar_dados``), restrito ao undergrad da unidade (``codpgm=1``).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from tqdm import tqdm

from .connection import DB

if TYPE_CHECKING:
    from .dataset_alocacao import DatasetConfig


class _DatasetConfigAttrs:
    """Marker de anotação: ``cfg`` é ``DatasetConfig`` (importado lazily em
    runtime para evitar import circular com :mod:`dataset_alocacao`)."""


def _load_pickled(caminho: pd.Series) -> pd.DataFrame:
    return pd.read_pickle(caminho)


def _stream_to_pickle(caminho, query, desc, chunksize=5000):
    """Replica :func:`replicado.dataset_alocacao._stream_to_pickle` localmente
    para evitar import circular."""
    n = DB.fetch_count(f"SELECT COUNT(*) AS c FROM ({query}) AS __t")
    chunks: list[pd.DataFrame] = []
    bar = tqdm(
        DB.iter_chunks(query, chunksize=chunksize),
        total=0 if n == 0 else None,
        desc=desc,
        unit="ln",
        unit_scale=True,
    )
    try:
        bar.reset(total=n)
        for chunk in bar:
            chunks.append(chunk)
    finally:
        bar.close()
    df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    df.to_pickle(caminho)
    return df


DEFAULT_JANELA_REQUERIMENTOS_D = 30
DEFAULT_JANELA_TRANCAMENTO_D = 90


# ---------------------------------------------------------------------------
# Carga / cache (extendido a partir de ``carregar_dados``)
# ---------------------------------------------------------------------------
def carregar_macrosensores(
    cfg: DatasetConfig, dados: dict[str, pd.DataFrame], forcar: bool = False
) -> dict[str, pd.DataFrame]:
    """Extrai/cacheia as 3 tabelas auxiliares dos macro-sensores e injeta em
    ``dados`` (chaves ``calend``, ``req_unidade``, ``histprog_unidade``).

    São filtradas no banco por :class:`DatasetConfig.codundclg` (via JOIN com
    ``HABILPROGGR`` → ``CURSOGR``) e por ``codpgm=1`` (graduação), de modo a
    restringir o cache a eventos da unidade-alvo.

    Migração: renomeados os caches legados ``aux_req_ime_<cod>.pkl`` /
    ``aux_histprog_ime_<cod>.pkl`` para ``*_unidade_*``. Se o arquivo novo não
    existir mas o legado existir, ele é usado (e regravado no novo nome) —
    evita reextrair do banco só por causa da renomeação estética.
    """
    cod = cfg.codundclg
    cache = cfg.cache_dir
    anos = list(cfg.anos)
    ano_min = min(anos)
    ano_max = max(anos)
    # Recuperação do ``AlgorithmException`` absurdamente longa atrás: estica
    # a janela em um ano para cobrir T-1 do primeiro ano do dataset.
    data_min = pd.Timestamp(year=ano_min - 1, month=1, day=1)

    def _resolve_cache(novo: Path, legado: Path | None = None) -> tuple[Path, bool]:
        """Devolve (caminho_para_ler, ja_existe). Promove caches legados."""
        if novo.exists():
            return novo, True
        if legado is not None and legado.exists():
            try:
                legado.rename(novo)
            except OSError:
                return legado, True  # rename falhou (ex.: FS): lê do velho
            return novo, True
        return novo, False

    # ---- CALENDGR (pequena; só leitura) ---------------------------------
    c = cache / f"aux_calend_{cod}.pkl"
    if c.exists() and not forcar:
        dados["calend"] = _load_pickled(c)
    else:
        q = (
            "SELECT anocld, codclg, perref, tipdtagrd, dtainimax, dtafim, stacld "
            "FROM CALENDGR "
            f"WHERE codclg = {cod} AND anocld BETWEEN {ano_min - 1} AND {ano_max + 1}"
        )
        dados["calend"] = _stream_to_pickle(c, q, "aux:calend")

    # ---- REQUERIMENTOGR (undergrad da unidade) -------------------------
    # JOIN por (codpes, codpgm) com habilit-programa único da unidade: impede
    # multiplicar linhas (aluno com várias habilitações no mesmo codpgm).
    c = cache / f"aux_req_unidade_{cod}.pkl"
    c_legado = cache / f"aux_req_ime_{cod}.pkl"
    c_ler, existe = _resolve_cache(c, c_legado)
    if existe and not forcar:
        dados["req_unidade"] = _load_pickled(c_ler)
    else:
        q = (
            "SELECT DISTINCT R.codpes, R.codpgm, R.dtacadrqm, R.tiprqm "
            "FROM REQUERIMENTOGR R "
            "INNER JOIN ( "
            "  SELECT DISTINCT HP.codpes, HP.codpgm "
            f"  FROM HABILPROGGR HP INNER JOIN CURSOGR C ON HP.codcur = C.codcur "
            f"  WHERE C.codclg = {cod} "
            ") UNID ON R.codpes = UNID.codpes AND R.codpgm = UNID.codpgm "
            f"WHERE R.codpgm = 1 AND R.dtacadrqm >= '{data_min.date().isoformat()}'"
        )
        dados["req_unidade"] = _stream_to_pickle(c, q, "aux:req_unidade")
    r = dados["req_unidade"].copy()
    if len(r):
        r["dtacadrqm"] = pd.to_datetime(r["dtacadrqm"], errors="coerce")
        if "tiprqm" in r.columns:
            r["tiprqm"] = r["tiprqm"].astype(str).str.strip()
    dados["req_unidade"] = r

    # ---- HISTPROGGR (undergrad da unidade) ------------------------------
    c = cache / f"aux_histprog_unidade_{cod}.pkl"
    c_legado = cache / f"aux_histprog_ime_{cod}.pkl"
    c_ler, existe = _resolve_cache(c, c_legado)
    if existe and not forcar:
        dados["histprog_unidade"] = _load_pickled(c_ler)
    else:
        q = (
            "SELECT h.codpes, h.codpgm, h.dtaoco, h.stapgm, h.motstapgm, "
            "       h.anoref, h.perref "
            "FROM HISTPROGGR h "
            "INNER JOIN ( "
            "  SELECT DISTINCT HP.codpes, HP.codpgm "
            f"  FROM HABILPROGGR HP INNER JOIN CURSOGR C ON HP.codcur = C.codcur "
            f"  WHERE C.codclg = {cod} "
            f") UNID ON h.codpes = UNID.codpes AND h.codpgm = UNID.codpgm "
            f"WHERE h.codpgm = 1 AND h.dtaoco >= '{data_min.date().isoformat()}'"
        )
        dados["histprog_unidade"] = _stream_to_pickle(c, q, "aux:histprog_unidade")
    h = dados["histprog_unidade"].copy()
    if len(h):
        h["dtaoco"] = pd.to_datetime(h["dtaoco"], errors="coerce")
        h["stapgm"] = h["stapgm"].astype(str).str.strip()
        if "motstapgm" in h.columns:
            h["motstapgm"] = h["motstapgm"].astype(str).str.strip()
    dados["histprog_unidade"] = h
    return dados


# ---------------------------------------------------------------------------
# Helpers - linhagem calendária
# ---------------------------------------------------------------------------
def _t1_ano_sem(ano_sem: int) -> int:
    """``ano_sem`` (ano*10+sem, sem∈{1,2}) do semestre anterior na linhagem
    calendária: 1S ano N ← 2S ano N-1; 2S ano N ← 1S ano N."""
    ano = ano_sem // 10
    sem = ano_sem % 10
    return (ano - 1) * 10 + 2 if sem == 1 else ano * 10 + 1


def _codtur_prefix_t1(codtur_t0: str) -> str:
    """Prefixo ``<ano><sem>`` do codtur do semestre anterior ao T0 (mesma
    codificação do ``codtur`` USP): usado para filtrar qual fatia anual do
    cache HISTESCOLARGR cobre T-1."""
    ano = int(codtur_t0[:4])
    sem = int(codtur_t0[4])
    if sem == 1:
        return f"{ano - 1}2"
    return f"{ano}1"


# ---------------------------------------------------------------------------
# H1 — Compressão do calendário (gap entre fim de T-1 e início de T0)
# ---------------------------------------------------------------------------
def _fim_t1_calendgr(cal: pd.DataFrame) -> dict[int, pd.Timestamp]:
    """Mapeia ``ano_sem(T-1)`` → ``dtafim`` do período de recuperação
    cadastrado na CALENDGR (o "final real" do semestre, janela de
    recuperação inclusa). Semestre sem 'Per Recup' (IME pós-2020) ausente.
    """
    if cal is None or not len(cal):
        return {}
    pr = cal[cal["tipdtagrd"].astype(str).str.strip().eq("Per Recup")].copy()
    if not len(pr):
        return {}
    pr["perref"] = pr["perref"].astype(str).str.strip()
    pr["ano_sem"] = pr["anocld"].astype(int) * 10 + pr["perref"].str[0].astype(int)
    pr["dtafim"] = pd.to_datetime(pr["dtafim"], errors="coerce")
    pr = pr.dropna(subset=["dtafim"])
    # Se houver duplicidades (raras), fique com a maior dtafim.
    return pr.sort_values("dtafim").drop_duplicates("ano_sem", keep="last").set_index(
        "ano_sem"
    )["dtafim"].to_dict()


def _fim_t1_turmagr(turmas: pd.DataFrame) -> dict[int, pd.Timestamp]:
    """Fallback pós-2020 (sem 'Per Recup' na CALENDGR): aproximação do final
    real do semestre a partir do ``dtafimtur`` da TURMAGR — instrumento
    robusto contra outliers.

    Estratégia: para cada ``(ano_sem)`` do semestre T-1, toma o maior
    ``dtafimtur`` dentre as turmas reais (``sufixo >= sufixo_min``) cujo fim
    está **dentro da janela plausível do semestre** (1S termina entre maio e
    agosto; 2S entre novembro e abril do ano seguinte). Evita os raros
    "Estágio/Trabalho" turmas que conting a dataset (vão a julho de depois).
    """
    if turmas is None or not len(turmas):
        return {}
    t = turmas.copy()
    t["codtur"] = t["codtur"].astype(str).str.strip()
    t = t[t["codtur"].str.match(r"^20\d{5}$", na=False)]
    t["ano"] = t["codtur"].str[:4].astype(int)
    t["sem"] = t["codtur"].str[4].astype(int)
    t = t[t["sem"].isin([1, 2])] if "sem" in t.columns else t
    t["ano_sem"] = t["ano"] * 10 + t["sem"]
    t["sufixo"] = t["codtur"].str[-2:].astype(int)
    t = t[t["sufixo"] >= 40]
    t["dtafimtur"] = pd.to_datetime(t["dtafimtur"], errors="coerce")
    t = t.dropna(subset=["dtafimtur"])
    out: dict[int, pd.Timestamp] = {}
    for as_, grp in t.groupby("ano_sem"):
        fim = grp["dtafimtur"]
        # Ano-semestre decorre do ano_t0 esperado: 1S_ano = [mai-01, ago-15]
        # do mesmo ano; 2S_ano = [nov-01, abr-30 do ano+1].
        as_ano = as_ // 10
        as_sem = as_ % 10
        if as_sem == 1:
            lb = pd.Timestamp(year=as_ano, month=5, day=1)
            ub = pd.Timestamp(year=as_ano, month=8, day=15)
        else:
            lb = pd.Timestamp(year=as_ano, month=11, day=1)
            ub = pd.Timestamp(year=as_ano + 1, month=4, day=30)
        cortado = fim[(fim >= lb) & (fim <= ub)]
        if len(cortado):
            out[int(as_)] = cortado.max()
    return out


def _macro_gap_calendario(
    cfg: DatasetConfig, df: pd.DataFrame, dados: dict[str, pd.DataFrame]
) -> pd.Series:
    """H1: ``gap = dtainitur(T0) − fim(T-1)`` (dias). Fim de T-1 vem da
    CALENDGR\\ 'Per Recup' (preferencial; 2010-2019 IME) ou do fallback
    TURMAGR (2020+).

    Gap pequeno/*negativo* ⇒ T-1 foi comprimido pelo atraso (término em/depois
    do início de T0) — um "macro-sensor" forte para greves.
    """
    cal = dados.get("calend")
    calend_map = _fim_t1_calendgr(cal) if cal is not None else {}
    fb_map = _fim_t1_turmagr(dados.get("turmas"))

    # Coluna de flag: 1 = fim de T-1 veio da CALENDGR (autoridade); 0 =
    # fallback TURMAGR; NaN = sem dado verificável.
    gaps: list[float] = []
    flags: list[float] = []
    for as0, dt0 in zip(df["ano_sem"], df["dtainitur"], strict=True):
        as_t1 = _t1_ano_sem(int(as0))
        if as_t1 in calend_map:
            fim_t1 = calend_map[as_t1]
            source = 1
        elif as_t1 in fb_map:
            fim_t1 = fb_map[as_t1]
            source = 0
        else:
            gaps.append(np.nan)
            flags.append(np.nan)
            continue
        gaps.append(float((pd.Timestamp(dt0) - fim_t1).days))
        flags.append(source)
    return (
        pd.Series(gaps, index=df.index, dtype=float),
        pd.Series(flags, index=df.index, dtype="object"),
    )


# ---------------------------------------------------------------------------
# H2 — Termômetro da burocracia (contagem de requerimentos)
# ---------------------------------------------------------------------------
def _macro_requerimentos(
    cfg: DatasetConfig, df: pd.DataFrame, dados: dict[str, pd.DataFrame]
) -> pd.Series:
    """H2: contagem de requerimentos da unidade (undergrad) criados em
    ``[Dia D - 30d, Dia D]``. ``dtacadrqm`` é imutável ⇒ barreira segura."""
    req = dados.get("req_unidade")
    dias_janela = DEFAULT_JANELA_REQUERIMENTOS_D
    out = pd.Series(0, index=df.index, dtype="int64")
    if req is None or not len(req):
        return out
    r = req.dropna(subset=["dtacadrqm"]).sort_values("dtacadrqm")
    if not len(r):
        return out
    arr = r["dtacadrqm"].astype("datetime64[ns]").values
    dia_d = pd.to_datetime(df["dtainitur"], errors="coerce") - pd.Timedelta(
        days=cfg.dias_corte
    )
    ini = dia_d - pd.Timedelta(days=dias_janela)
    ini_ns = ini.astype("datetime64[ns]").values.astype("datetime64[ns]")
    fim_ns = dia_d.astype("datetime64[ns]").values.astype("datetime64[ns]")
    # Quantos registros em [ini_i, fim_i]: cnt = searchsorted(fim) - searchsorted(ini, side='left')
    lo = np.searchsorted(arr, ini_ns, side="left")
    hi = np.searchsorted(arr, fim_ns, side="right")
    out[:] = np.clip(hi - lo, 0, None).astype("int64")
    return out


# ---------------------------------------------------------------------------
# H3 — Atraso sistêmico de notas
# ---------------------------------------------------------------------------
def _macro_atraso_notas(
    cfg: DatasetConfig, df: pd.DataFrame, dados: dict[str, pd.DataFrame]
) -> pd.Series:
    """H3: fração das matrículas do semestre T-1 ainda **não consolidadas**
    no Dia D do semestre atual (calendar lineage). Válido por semestre T0
    (broadcasts valores constantes a todas as turmas do mesmo ``ano_sem``).

    Para cada ``ano_sem(T0)``:

    1.  Seleciona histórico de T-1 (``HISTESCOLARGR.codtur LIKE '<anoT1><semT1>%'``).
    2.  Conserva apenas registros criados **até o Dia D(T0)** — seguindo a
        regra do :func:`replicado.dataset_alocacao.reconstruir_estmtr`
        (sem filtro ``stamtr``; inscrições deletadas hoje existiam naquele
        instante).
    3.  Marca como "ainda não consolidada" a linha cujo ``dtaultalt > Dia D``
        (modificada depois → conteúdo indeterminado naquele instante ⇒ nota
        final não estava disponível).
    4.  ``frac = nº pendente / nº existente``. Modelo usa ``[0,1]``.

    Macro/coarse: o Dia D por semestre T0 usa a mediana dos ``dtainitur``
    daquele semestre (variação natural intra-semestre é desprezível para
    um macro-signal).
    """
    hist = dados.get("hist")
    out = pd.Series(np.nan, index=df.index, dtype="float64")
    if hist is None or not len(hist):
        return out
    h = hist.copy()
    h["codtur"] = h["codtur"].astype(str).str.strip()
    h["dtacrihst"] = pd.to_datetime(h["dtacrihst"], errors="coerce")
    h["dtaultalt"] = pd.to_datetime(h["dtaultalt"], errors="coerce")
    h = h.dropna(subset=["dtacrihst"])

    # Median dia_d por ano_sem(T0).
    df = df.copy()
    df["dia_d"] = df["dtainitur"] - pd.Timedelta(days=cfg.dias_corte)
    sem_dia = (
        df.dropna(subset=["dia_d"])
        .groupby("ano_sem")["dia_d"]
        .median()
        .to_dict()
    )
    for as_t0, dia_d in sem_dia.items():
        as_t1 = _t1_ano_sem(int(as_t0))
        prefix = f"{as_t1 // 10}{as_t1 % 10}"
        sub = h[h["codtur"].str.startswith(prefix, na=False)]
        if not len(sub):
            continue
        existentes = sub[sub["dtacrihst"] <= dia_d]
        if len(existentes) == 0:
            continue
        if "dtaultalt" in existentes.columns:
            pend = existentes[existentes["dtaultalt"] > dia_d]
        else:
            pend = existentes
        frac = float(len(pend)) / float(len(existentes))
        mask = df["ano_sem"] == as_t0
        out.loc[mask] = frac
    return out


# ---------------------------------------------------------------------------
# H4 — Taxa global de trancamento
# ---------------------------------------------------------------------------
def _macro_trancamento(
    cfg: DatasetConfig, df: pd.DataFrame, dados: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """H4: número de trancamentos da unidade (``stapgm='T'``) nos 90 dias
    pré-Dia D e a taxa sobre o total de programas ativos no snapshot do
    Dia D.

    Numerador: ``HISTPROGGR`` cujo ``stapgm='T'`` AND ``dtaoco ∈ [Dia D −
    90d, Dia D]``. Não contamos ``stapgm='E'`` porque é majoritariamente
    "Conclusão" (vide motstapgm no cache), fonte de ruído e não de crise-
    temporária (a hipótese fala em "abandono temporário").

    Denominador: snapshot de programas da unidade ativos no Dia D — último
    evento ``dtaoco <= Dia D`` por ``(codpes, codpgm)``; ativo se ``stapgm
    ∈ {'A','R'}``. Snapshot é seguro: nunca lê eventos futuros.
    """
    hp = dados.get("histprog_unidade")
    out_n = pd.Series(0, index=df.index, dtype="int64")
    out_rate = pd.Series(np.nan, index=df.index, dtype="float64")
    if hp is None or not len(hp):
        return out_n, out_rate
    h = hp.dropna(subset=["dtaoco"]).copy()
    if not len(h):
        return out_n, out_rate

    df = df.copy()
    df["dia_d"] = df["dtainitur"] - pd.Timedelta(days=cfg.dias_corte)

    # Pre-sort: ultimo evento por par (codpes, codpgm) <= Dia D.
    h = h.sort_values("dtaoco").reset_index(drop=True)
    # Vetores para numerador (eventos T).
    tranc = h[h["stapgm"].astype(str).str.strip().eq("T")].copy()
    tranc_arr = tranc["dtaoco"].astype("datetime64[ns]").values if len(tranc) else (
        np.array([], dtype="datetime64[ns]")
    )
    # Eventos "ativos-A/R/T/E" para snapshot — usamos todos (cobre o "último
    # status de cada par" via groupby.tail dentro do filtro dtaoco<=Dia D).
    full_arr = h["dtaoco"].astype("datetime64[ns]").values
    # Indices de origem para atacar os pares e stapgm:
    hp_codpes = h["codpes"].values
    hp_codpgm = h["codpgm"].values
    hp_stapgm = h["stapgm"].astype(str).str.strip().values

    dia_d_ns = df["dia_d"].astype("datetime64[ns]").values.astype("datetime64[ns]")
    ini_janela_ns = (df["dia_d"] - pd.Timedelta(days=DEFAULT_JANELA_TRANCAMENTO_D)).astype(
        "datetime64[ns]"
    ).values.astype("datetime64[ns]")

    # Numerador: trancamentos na janela [Dia D-90d, Dia D].
    if len(tranc_arr):
        lo = np.searchsorted(tranc_arr, ini_janela_ns, side="left")
        hi = np.searchsorted(tranc_arr, dia_d_ns, side="right")
        out_n[:] = np.clip(hi - lo, 0, None).astype("int64")

    # Denominador: snapshot ativos. Para cada Dia D, we filter rows com
    # dtaoco <= Dia D, agrupar por (codpes, codpgm), último stapgm, contar
    # ativos.
    ativo_set = {"A", "R"}
    dia_d_unique, inv = np.unique(dia_d_ns, return_inverse=True)
    ativos_por_dia: dict[Any, int] = {}

    # Vetor booleano de quais linhas têm stapgm ativo.
    ativo_mask = np.array(
        [s in ativo_set for s in hp_stapgm], dtype=bool
    )

    for j, ddt in enumerate(dia_d_unique):
        # Linhas com dtaoco <= Ddia
        bound = np.searchsorted(full_arr, np.array([ddt], dtype="datetime64[ns]"), side="right")[0]
        if bound == 0:
            ativos_por_dia[ddt] = 0
            continue
        # Última linha por (codpes, codpgm): groupby sobre as primeiras
        # ``bound`` linhas. Implementação vetorizada com pandas (rápido o
        # suficiente por Dia D único; ~30 Dia Ds no dataset).
        sub_p = pd.DataFrame(
            {
                "cp": hp_codpes[:bound],
                "pg": hp_codpgm[:bound],
                "s": hp_stapgm[:bound],
                "am": ativo_mask[:bound],
            }
        )
        sub_p = sub_p.drop(columns=["am"])
        sub_p["ativo"] = ativo_mask[:bound]
        ult = sub_p.sort_values("s").groupby(["cp", "pg"], sort=False).tail(1)
        ativos_por_dia[ddt] = int(ult["ativo"].sum())

    # Broadcast de volta para cada linha df (via inv).
    ativos_series = pd.Series(
        [ativos_por_dia.get(d, 0) for d in dia_d_unique], index=pd.Index(dia_d_unique)
    )
    ativos_por_linha = ativos_series.iloc[inv].reset_index(drop=True)
    out_rate[:] = np.where(
        (ativos_por_linha.values > 0) & out_n.notna(),
        out_n.values / np.maximum(ativos_por_linha.values, 1).astype("float64"),
        np.nan,
    )
    return out_n, out_rate


# ---------------------------------------------------------------------------
# Orquestrador — uma única entrada que injeta todas as 4 features no df
# ---------------------------------------------------------------------------
def _momentum_por_sem(serie: pd.Series) -> pd.DataFrame:
    """Derivadas temporais (momentum/velocity/anomaly) de uma série indexada
    por ``ano_sem`` (uma observação por semestre letivo do colegiado).

    Camada temporal sobre as macro-features de nível — transforma "foto no
    Dia D" em "dinâmica" (a greve é um *evento*, não um estado). Barreira
    do Dia D: ``shift(1)`` é aplicado ANTES de qualquer ``rolling()``,
    exatamente como em :func:`replicado.dataset_alocacao.features_espaco_fase`
    — o semestre corrente nunca entra no seu próprio baseline.

    Devolve:

    - ``delta_t1``    — X(T0) − X(T−1). Momento semester-over-semester na
                        linhagem calendária (1S←2S ano anterior; 2S←1S
                        mesmo ano). Carrega sazonalidade que existe entre
                        1S e 2S (picos de ingressantes, calendário).
    - ``delta_yoy``   — X(T0) − X(T−2), mesmo semestre do ano anterior
                        (``shift(2)`` sobre ``ano_sem`` ordenado). Sinal de
                        momentum **sazonalidade-controlado**: apropriado
                        para features com pico sazonal forte (ex. 1S sempre
                        tem mais requerimentos que 2S por causa dos
                        ingressantes FUVEST/SISU).
    - ``z_score_3sem`` — (X(T0) − média_passada_3sem) / std_passado_3sem.
                        :math:`z`-score de anomalia padronizado, comparável
                        entre features de escala distinta (contagem absoluta
                        vs fração [0,1]). ``shift(1).rolling(3,
                        min_periods=1)`` usa só semestres estritamente
                        anteriores a T0 → imune a vazamento do Dia D.

    Semestres sem baseline (primórdios do dataset, menos de 2 amostras
    passadas) devolvem NaN — interpretado pelo modelo como "sem sinal de
    momentum conhecido".
    """
    s = serie.sort_index().astype("float64")
    df = pd.DataFrame(index=s.index)
    df["delta_t1"] = s - s.shift(1)
    df["delta_yoy"] = s - s.shift(2)
    roll = s.shift(1).rolling(window=3, min_periods=1)
    media = roll.mean()
    df["z_score_3sem"] = (s - media) / roll.std().replace(0.0, np.nan)
    return df[["delta_t1", "delta_yoy", "z_score_3sem"]]


# Features de nível que recebem momentum: o ``macro_gap_calendario_dias``
# é, ele próprio, um **diff** temporal (T0 início − T−1 fim), mas as
# derivadas isolam a componente de crise do encurtamento estrutural do
# calendário ao longo dos anos. ``macro_trancamentos_90d_pre_dia_d`` (contagem
# absoluta,scala varia com coorte) é representado pela taxa (``macro_taxa_*
# é escala-estável e comparável entre semestres).
MACRO_MOMENTUM_FROM = [
    "macro_gap_calendario_dias",
    "macro_requerimentos_30d_pre_dia_d",
    "macro_frac_atraso_notas_T1",
    "macro_taxa_trancamento_90d",
]


def features_macrosensores(
    cfg: DatasetConfig, df: pd.DataFrame, dados: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Calcula as 4 macro-features de **nível** + 3 derivadas de **momentum**
    por feature (12 colunas de momentum no total).

    Nível (snapshot no Dia D de T0):

    - ``macro_gap_calendario_dias``  (H1)
    - ``macro_gap_calendario_fonte``  (H1 flag: 1=CALENDGR, 0=TURMAGR)
    - ``macro_requerimentos_30d_pre_dia_d``  (H2)
    - ``macro_frac_atraso_notas_T1``  (H3)
    - ``macro_trancamentos_90d_pre_dia_d``  (H4)
    - ``macro_taxa_trancamento_90d``  (H4 taxa/ativos)

    Momentum (derivadas temporais, ``__delta_t1``/``__delta_yoy``/
    ``__z_score_3sem``), computadas sobre a série agregada por ``ano_sem``
    (mediana intra-semestre — sinal macro do colegiado) e broadcast de
    volta por semestre. Barreira do Dia D via ``shift(1)`` antes de
    ``rolling()`` (cf. :func:`features_espaco_fase`).
    """
    if "dtainitur" not in df.columns:
        return df
    df = df.copy()
    df["dtainitur"] = pd.to_datetime(df["dtainitur"], errors="coerce")

    # H1
    gap_series, flag_series = _macro_gap_calendario(cfg, df, dados)
    df["macro_gap_calendario_dias"] = gap_series
    df["macro_gap_calendario_fonte"] = flag_series

    # H2
    df["macro_requerimentos_30d_pre_dia_d"] = _macro_requerimentos(cfg, df, dados)

    # H3
    df["macro_frac_atraso_notas_T1"] = _macro_atraso_notas(cfg, df, dados)

    # H4
    n_tranc, rate = _macro_trancamento(cfg, df, dados)
    df["macro_trancamentos_90d_pre_dia_d"] = n_tranc
    df["macro_taxa_trancamento_90d"] = rate

    # ---- Momentum / velocity (derivadas temporais) ----------------------
    # Agrega por ``ano_sem`` (mediana intra-semestre → sinal macro do
    # colegiado; um valor por semestre). ``MACRO_MOMENTUM_FROM`` lista as
    # features de nível a derivar (cf. constante acima).
    if not len(df) or "ano_sem" not in df.columns:
        return df
    agg = (
        df.groupby("ano_sem")[MACRO_MOMENTUM_FROM]
        .median()
        .sort_index()
    )
    parts: list[pd.DataFrame] = []
    for col in tqdm(MACRO_MOMENTUM_FROM, desc="Macro-momentum", unit="feat"):
        m = _momentum_por_sem(agg[col]).add_prefix(f"{col}__")
        parts.append(m)
    mom = pd.concat(parts, axis=1).reset_index()  # ano_sem vira coluna
    df = df.merge(mom, on="ano_sem", how="left")
    return df


__all__ = [
    "carregar_macrosensores",
    "features_macrosensores",
    "DEFAULT_JANELA_REQUERIMENTOS_D",
    "DEFAULT_JANELA_TRANCAMENTO_D",
]