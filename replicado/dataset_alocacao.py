"""
Fornecedor de dataset para o modelo de alocação de salas (Replicado).

Este módulo é **agnóstico em relação à unidade USP**: todas as
especificidades (colegiado, prefixos de disciplina, piso de vagas, janela de
anos) são parametrizadas em :class:`DatasetConfig`, com defaults lidos do
``.env``. O ponto de entrada é :func:`montar_dataset`, que devolve um
``DataFrame`` linha-por-turma com **features + alvos** pronto para treino.

Pipeline (rodando sobre cache local; bate no banco apenas para extrair
tabelas auxiliares ainda não cacheadas, com barra de progresso tqdm):

1.  ``carregar_dados``      — resgata/cacheia TURMAGR, HISTESCOLARGR e as
                              tabelas auxiliares (GRADECURRICULAR, OCUPTURMA,
                              PROGRAMAGR, HABILPROGGR, MINISTRANTE,
                              DETTURMAGR, DISCIPLINAGR, DISCIPGRCODIGO,
                              PERIODOHORARIO, CURSOGR).
2.  ``reconstruir_estmtr``    — ``estmtr`` histórico (regra
                              dtacrihst<=dtainitur-Nd, sem filtro de stamtr;
                              ver ``scripts/maquina_tempo_estmtr.py``). É a
                              FEATURE central do dataset (sinal disponível no
                              "Dia D", quando as salas são distribuídas: os
                              alunos já estão inscritos) E também um dos
                              dois ingredientes do alvo.
3.  ``reconstruir_alvo_nummtr_max`` — ``nummtr_max`` = máx. ocupação nas 3
                              primeiras semanas de aula (ver
                              ``scripts/alvo_pico_ocupacao.py``). Não vira
                              feature: é ingrediente do alvo e, mantida crua,
                              permitira ao modelo resolver o alvo por uma
                              subtração trivial. Aparece aqui só para derivar
                              o alvo e em seguida é descartada.
4.  Alvo: ``delta = nummtr_max - estmtr`` — o quanto o ``estmtr`` (proxy do
                              Júpiter no Dia D) erra em relação ao pico de
                              ocupação esperado. É o ÚNICO alvo preditivo: o
                              modelo aprende o fator de correção da estimativa
                              institucional.
5.  ``features_*``          — base + histórico + demanda + professor/horário
                              + ingressantes; mais as features **avançadas**
                              (Módulos 2-4: espaço de fase do ``estmtr``,
                              pressão de represamento, métricas de rede,
                              concorrência horária, atratividade docente).
                              NENHUMA feature usa ``nummtr``/``nummtr_max``/
                              ``nummtr_total``: tudo que depende do pico
                              consolidado vaza o alvo. As antigas features de
                              histórico/espaço de fase que usavam
                              ``nummtr_total`` foram reinterpretadas em função
                              de ``estmtr`` (média/máx/resíduo do proxy no
                              passado) — onde a reinterpretação faz sentido
                              matemático; contas degeneradas (``nummtr/estmtr``
                              → sempre 1) foram descartadas.
6.  merge final             — um ``DataFrame`` por turma, com a feature
                              ``estmtr`` e o alvo ``delta``. As colunas cruas
                              que vazam o alvo (``nummtr`` consolidado da
                              TURMAGR, o próprio ``nummtr_max`` e as
                              ``ocup_d+*`` que o geram) são descartadas.

Uso
---
    poetry run python scripts/build_dataset.py
    poetry run python scripts/build_dataset.py --codundclg 45 --prefixos MAC MAT
    REPLICADO_PREFIXOS_DISC="MAC,MAT,MAE" poetry run python scripts/build_dataset.py
"""

from __future__ import annotations

import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from tqdm import tqdm

try:
    import networkx as nx
except Exception:  # pragma: no cover - dependência opcional
    nx = None  # type: ignore[assignment]

# Set by CLI scripts that add the project root to sys.path.
from .connection import DB  # noqa: E402

CACHE_DIR = Path("temp/cache_maquina_tempo")
SAIDA_DEFAULT = Path("temp/dataset_alocacao.csv")

# Defaults compatíveis com o IME-USP (escobalcados na engenharia reversa).
DEFAULT_PREFIXOS = ("45", "MAC", "MAE", "MAT", "MAP", "MPM")
DEFAULT_SUFIXO_MIN = 40
DEFAULT_ANO_MIN = 2010
DEFAULT_ANO_MAX = 2026  # último ano com HISTESCOLARGR local completa.
DEFAULT_DIAS_CORTE = 5
DEFAULT_PISO_VAGAS = 30
DEFAULT_DIAS_PICO = (0, 7, 11, 14, 21)
DEFAULT_TOP_CURSOS = 12

RSTFM_REPROVACAO = ("RN", "RF", "RA", "AB")


# ---------------------------------------------------------------------------
# Configuração agnóstica à unidade
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DatasetConfig:
    """Parâmetros de escopo / saneamento do dataset, independentes da unidade.

    Todos têm defaults IME, mas podem vir do ``.env`` (:meth:`from_env`) ou
    serem passados explicitamente pelo CLI.
    """

    codundclg: int = 45
    prefixos: tuple[str, ...] = DEFAULT_PREFIXOS
    sufixo_min: int = DEFAULT_SUFIXO_MIN
    ano_min: int = DEFAULT_ANO_MIN
    ano_max: int = DEFAULT_ANO_MAX
    dias_corte: int = DEFAULT_DIAS_CORTE
    piso_vagas: int = DEFAULT_PISO_VAGAS
    dias_pico: tuple[int, ...] = DEFAULT_DIAS_PICO
    top_cursos: int = DEFAULT_TOP_CURSOS
    cache_dir: Path = CACHE_DIR
    saida: Path = SAIDA_DEFAULT

    @classmethod
    def from_env(cls, **overrides: Any) -> DatasetConfig:
        import os

        def env_int(name: str) -> int | None:
            v = os.getenv(name)
            return int(v) if v and v.strip() else None

        def env_csv(name: str) -> tuple[str, ...] | None:
            v = os.getenv(name)
            if not v or not v.strip():
                return None
            return tuple(p.strip() for p in v.split(",") if p.strip())

        cfg = {
            "codundclg": env_int("REPLICADO_CODUNDCLG"),
            "prefixos": env_csv("REPLICADO_PREFIXOS_DISC"),
            "sufixo_min": env_int("REPLICADO_SUFIXO_MIN"),
            "ano_min": env_int("REPLICADO_ANO_MIN"),
            "ano_max": env_int("REPLICADO_ANO_MAX"),
            "dias_corte": env_int("REPLICADO_DIAS_CORTE"),
            "piso_vagas": env_int("REPLICADO_PISO_VAGAS"),
            "top_cursos": env_int("REPLICADO_TOP_CURSOS"),
        }
        cfg = {k: v for k, v in cfg.items() if v is not None}
        cfg.update(overrides)
        return cls(**cfg)  # type: ignore[arg-type]

    @property
    def anos(self) -> range:
        return range(self.ano_min, self.ano_max + 1)

    def prefixos_match(self, coddis: str) -> bool:
        return coddis.startswith(self.prefixos)

    def cache(self, nome: str) -> Path:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        return self.cache_dir / nome


# ---------------------------------------------------------------------------
# Carga de dados (com cache + tqdm streaming)
# ---------------------------------------------------------------------------
def _stream_to_pickle(
    caminho: Path,
    query: str,
    desc: str,
    chunksize: int = 5000,
) -> pd.DataFrame:
    """Extrai uma query em chunks (tqdm com ETA via COUNT) e salva em pickle.

    Essa é a "barra de progresso de importação". Como as tabelas auxiliares
    podem demorar, fazemos ``SELECT COUNT(*)`` primeiro para dar à tqdm um
    ``total`` real (com ETA) e então transmitimos via
    :meth:`DB.iter_chunks`.
    """
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


def _load_pickled(caminho: Path) -> pd.DataFrame:
    return pd.read_pickle(caminho)


def carregar_dados(cfg: DatasetConfig, forcar: bool = False) -> dict[str, pd.DataFrame]:
    """Carrega/cacheia todas as tabelas usadas pelas features.

    TURMAGR e HISTESCOLARGR aproveitam o cache já produzido por
    ``scripts/extrair_cache_replicado.py`` (cross-unit, sem sufixo de unidade).
    As auxiliares são filtradas por ``codundclg`` e cacheadas com sufixo
    ``_<codundclg>.pkl`` para isolar diferentes unidades.
    """
    dados: dict[str, pd.DataFrame] = {}
    cod = cfg.codundclg
    cache = cfg.cache_dir

    # --- TURMAGR (já existe via extrair_cache_replicado.py) ----------------
    c = cache / "turmagr_full.pkl"
    if c.exists() and not forcar:
        dados["turmas"] = _load_pickled(c)
    else:
        dados["turmas"] = _stream_to_pickle(c, "SELECT * FROM TURMAGR", "TURMAGR")
    t = dados["turmas"].copy()
    t["coddis"] = t["coddis"].astype(str).str.strip().str.upper()
    t["codtur"] = t["codtur"].astype(str).str.strip()
    dados["turmas"] = t

    # --- HISTESCOLARGR por ano (já existe via extrair_cache) ----------------
    hist_chunks = []
    anos_existentes = []
    for ano in cfg.anos:
        c = cache / f"histescolar_{ano}.pkl"
        if not c.exists():
            print(
                f"[aviso] {c.name} ausente — rode scripts/extrair_cache_replicado.py",
                file=sys.stderr,
            )
            continue
        hist_chunks.append(_load_pickled(c))
        anos_existentes.append(ano)
    h = pd.concat(hist_chunks, ignore_index=True) if hist_chunks else pd.DataFrame()
    if len(h):
        h["coddis"] = h["coddis"].astype(str).str.strip().str.upper()
        h["codtur"] = h["codtur"].astype(str).str.strip()
        h["dtacrihst"] = pd.to_datetime(h["dtacrihst"], errors="coerce")
        h["dtaultalt"] = pd.to_datetime(h["dtaultalt"], errors="coerce")
    dados["hist"] = h
    if not anos_existentes:
        print(
            "[aviso] nenhuma fatia HISTESCOLARGR no cache — rode "
            "scripts/extrair_cache_replicado.py",
            file=sys.stderr,
        )

    # --- DISCIPGRCODIGO (mapa coddis -> codclg; escopo) --------------------
    definicoes = [
        ("discipgr", f"SELECT coddis, codclg FROM DISCIPGRCODIGO WHERE codclg = {cod}"),
        (
            "disciplina",
            "SELECT coddis, verdis, creaul, cretrb, nomdis FROM DISCIPLINAGR",
        ),
        (
            "grade",
            f"SELECT G.codcrl, G.coddis, G.numsemidl, G.tipobg, C.codcur, C.codhab "
            f"FROM GRADECURRICULAR G "
            f"INNER JOIN CURRICULOGR C ON G.codcrl = C.codcrl "
            f"WHERE C.codcur IN (SELECT codcur FROM CURSOGR WHERE codclg = {cod})",
        ),
        ("curso", f"SELECT codcur, codclg, nomcur FROM CURSOGR WHERE codclg = {cod}"),
        (
            "detturma",
            f"SELECT DT.coddis, DT.codtur, DT.discrl, DT.numvag FROM DETTURMAGR DT "
            f"INNER JOIN DISCIPGRCODIGO DC ON DT.coddis = DC.coddis "
            f"WHERE DC.codclg = {cod}",
        ),
        (
            "ministrante",
            f"SELECT M.coddis, M.codtur, M.codpes AS codpes_prof FROM MINISTRANTE M "
            f"INNER JOIN DISCIPGRCODIGO DC ON M.coddis = DC.coddis "
            f"WHERE DC.codclg = {cod}",
        ),
        (
            "ocup",
            f"SELECT O.coddis, O.codtur, O.diasmnocp, P.codperhor, P.horent, P.horsai "
            f"FROM OCUPTURMA O "
            f"INNER JOIN PERIODOHORARIO P ON O.codperhor = P.codperhor "
            f"INNER JOIN DISCIPGRCODIGO DC ON O.coddis = DC.coddis "
            f"WHERE DC.codclg = {cod}",
        ),
        (
            "programa",
            f"SELECT P.codpes, P.codpgm FROM PROGRAMAGR P "
            f"WHERE P.codpes IN ( "
            f"  SELECT DISTINCT HP.codpes FROM HABILPROGGR HP "
            f"  INNER JOIN CURSOGR CS ON HP.codcur = CS.codcur "
            f"  WHERE CS.codclg = {cod})",
        ),
        (
            "habilprog",
            f"SELECT HP.codpes, HP.codpgm, HP.codcur, HP.codhab, HP.dtaclcgru, "
            f"P.dtaing FROM HABILPROGGR HP "
            f"INNER JOIN PROGRAMAGR P ON HP.codpes = P.codpes AND HP.codpgm = P.codpgm "
            f"INNER JOIN CURSOGR CS ON HP.codcur = CS.codcur "
            f"WHERE CS.codclg = {cod}",
        ),
    ]
    for chave, query in tqdm(definicoes, desc="Carga aux", unit="tab"):
        c = cache / f"aux_{chave}_{cod}.pkl"
        if c.exists() and not forcar:
            dados[chave] = _load_pickled(c)
        else:
            dados[chave] = _stream_to_pickle(c, query, f"aux:{chave}")
        df = dados[chave].copy()
        if "coddis" in df.columns:
            df["coddis"] = df["coddis"].astype(str).str.strip().str.upper()
        if "codtur" in df.columns:
            df["codtur"] = df["codtur"].astype(str).str.strip()
        dados[chave] = df

    # datas de ingresso / conclusão em programa+habilprog ------------------
    hp = dados["habilprog"].copy() if len(dados["habilprog"]) else pd.DataFrame()
    if len(hp):
        hp["dtaclcgru"] = pd.to_datetime(hp["dtaclcgru"], errors="coerce")
        hp["dtaing"] = pd.to_datetime(hp["dtaing"], errors="coerce")
    dados["habilprog"] = hp
    return dados


# ---------------------------------------------------------------------------
# Escopo / saneamento base
# ---------------------------------------------------------------------------
def filtrar_turmas(cfg: DatasetConfig, turmas: pd.DataFrame) -> pd.DataFrame:
    """Aplica os filtros de escopo da unidade (prefixos, sufixo, anos, padrão
    do codtur, datas de início conhecidas)."""
    t = turmas.copy()
    t = t[t["coddis"].map(cfg.prefixos_match)]
    t = t[t["codtur"].str.match(r"^20\d{5}$", na=False)]
    t["ano"] = t["codtur"].str[:4].astype(int)
    t = t[t["ano"].between(cfg.ano_min, cfg.ano_max)]
    t["sufixo"] = t["codtur"].str[-2:].astype(int)
    t = t[t["sufixo"] >= cfg.sufixo_min]
    t["dtainitur"] = pd.to_datetime(t["dtainitur"], errors="coerce")
    t = t[t["dtainitur"].notna()]
    t = t[t["tipmtr"].isin(["N", None]) | t["tipmtr"].isna()]
    t["ano_sem"] = t["ano"] * 10 + t["codtur"].str[4].astype(int)
    t["sem_tipo"] = t["codtur"].str[4].map({"1": "1S", "2": "2S"})
    return t.reset_index(drop=True)


def anonimizar_codpes(
    value: Any, mapa: dict[int, str], contador: list[int]
) -> str | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    key = int(value)
    if key not in mapa:
        contador[0] += 1
        mapa[key] = f"ID_{contador[0]}"
    return mapa[key]


# ---------------------------------------------------------------------------
# Alvo 1: estmtr (baseline/proxy) e Alvo 2: T_pico
# ---------------------------------------------------------------------------
def reconstruir_estmtr(
    cfg: DatasetConfig, turmas: pd.DataFrame, hist: pd.DataFrame
) -> pd.DataFrame:
    """Para cada turma no escopo, conta registros de HISTESCOLARGR criados
    até ``dtainitur - dias_corte`` (sem filtro de stamtr). Regra validada em
    ``scripts/maquina_tempo_estmtr.py`` (MAE 2.66 / corr 0.980)."""
    base = turmas[["coddis", "codtur", "dtainitur"]].copy()
    base["dta_corte"] = base["dtainitur"] - pd.Timedelta(days=cfg.dias_corte)

    h = hist[["coddis", "codtur", "dtacrihst"]].merge(
        base[["coddis", "codtur", "dta_corte"]], on=["coddis", "codtur"], how="inner"
    )
    h = h.dropna(subset=["dtacrihst", "dta_corte"])
    est = (
        h[h["dtacrihst"] <= h["dta_corte"]]
        .groupby(["coddis", "codtur"])
        .size()
        .rename("estmtr_val")
        .reset_index()
    )
    out = base.merge(est, on=["coddis", "codtur"], how="left").fillna({"estmtr_val": 0})
    out["estmtr_val"] = out["estmtr_val"].astype(int)
    return out[["coddis", "codtur", "estmtr_val"]]


def reconstruir_alvo_nummtr_max(
    cfg: DatasetConfig, turmas: pd.DataFrame, hist: pd.DataFrame
) -> pd.DataFrame:
    """``nummtr_max = max(ocupacao)`` em ``dias_pico`` pós-início, onde
    ``ocupacao(D) = |criados<=D| - |stamtr∈(E,R) com dtaultalt<=D|``.

    Único alvo preditivo do dataset: dimensiona a sala pelo pico de ocupação
    nas primeiras semanas. Regra validada em
    ``scripts/alvo_pico_ocupacao.py`` (``T_pico`` no script; renomeado
    ``nummtr_max`` no dataset por clareza)."""
    base = turmas[["coddis", "codtur", "dtainitur"]].copy()
    h = hist[["coddis", "codtur", "dtacrihst", "stamtr", "dtaultalt"]].merge(
        base, on=["coddis", "codtur"], how="inner"
    )
    h = h.dropna(subset=["dtacrihst", "dtainitur"])
    h["cri_d"] = (h["dtacrihst"] - h["dtainitur"]).dt.days
    excl = h["stamtr"].isin(["E", "R"])
    h["exc_d"] = (h["dtaultalt"] - h["dtainitur"]).dt.days.where(excl)

    out = base[["coddis", "codtur"]].copy()
    for d in cfg.dias_pico:
        cri = h[h["cri_d"] <= d].groupby(["coddis", "codtur"]).size()
        exc_ate = (
            h[h["exc_d"].notna() & (h["exc_d"] <= d)]
            .groupby(["coddis", "codtur"])
            .size()
        )
        col = pd.DataFrame({"cri": cri, "exc": exc_ate}).fillna(0)
        out[f"ocup_d{d:+d}"] = (
            (col["cri"] - col["exc"])
            .reindex(pd.MultiIndex.from_frame(out[["coddis", "codtur"]]), fill_value=0)
            .astype(int)
            .values
        )
    cols = [f"ocup_d{d:+d}" for d in cfg.dias_pico if d >= 0]
    out["nummtr_max"] = out[cols].max(axis=1).astype(int)
    return out[["coddis", "codtur", "nummtr_max", *cols]]


# ---------------------------------------------------------------------------
# Features base (operacional — replica o motor saneado do notebook)
# ---------------------------------------------------------------------------
def features_base(
    cfg: DatasetConfig, turmas_f: pd.DataFrame, dados: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Vagas saneadas (piso configurável), cargas, sufixo/departamento, flags
    operacionais.

    Note: a consolidação ``nummtr_total`` (soma das vias de matrícula) era
    computada aqui por :func:`features_historico`/:func:`features_espaco_fase`
    e :func:`features_professor_horario`. Como o alvo virou ``delta =
    nummtr_max - estmtr``, qualquer uso de ``nummtr`` vaza o alvo, e todas
    essas features passaram a usar ``estmtr``. Por isso a consolidação crua
    deixou de ser necessária e foi removida — persistindo apenas no
    ``COLUNAS_VAZAMENTO`` para garantir descarte."""
    t = turmas_f.copy()

    # Vagas detalhadas por fatia (DETTURMAGR.discrl: O/C/L/...)
    det = dados["detturma"]
    if len(det):
        det = det[det["codtur"].isin(t["codtur"])]
        vag = (
            det.pivot_table(
                index=["coddis", "codtur"],
                columns="discrl",
                values="numvag",
                aggfunc="sum",
                fill_value=0,
            )
            .add_prefix("vagas_")
            .reset_index()
        )
        t = t.merge(vag, on=["coddis", "codtur"], how="left")
    for col in ["vagas_O", "vagas_C", "vagas_L"]:
        if col not in t.columns:
            t[col] = 0
    t = t.fillna({c: 0 for c in t.columns if str(c).startswith("vagas_")})

    som = t[[c for c in t.columns if str(c).startswith("vagas_")]].sum(axis=1)
    tem_detalhe = (t["vagas_O"] + t["vagas_C"] + t["vagas_L"]) > 0
    t["vagas_reais"] = np.where(tem_detalhe, som, t["numvagtur"].fillna(0))
    t["vagas_reais"] = np.maximum(cfg.piso_vagas, t["vagas_reais"]).astype(int)
    t["flag_vagas_baixas"] = (t["vagas_reais"] <= cfg.piso_vagas).astype(int)
    t["departamento"] = t["coddis"].str[:3]

    # Carga (creaul, cretrb)
    disc = dados["disciplina"][
        ["coddis", "verdis", "creaul", "cretrb"]
    ].drop_duplicates(["coddis", "verdis"])
    t = t.merge(disc, on=["coddis", "verdis"], how="left")
    t["creaul"] = pd.to_numeric(t["creaul"], errors="coerce").fillna(0)
    t["cretrb"] = pd.to_numeric(t["cretrb"], errors="coerce").fillna(0)
    t["carga_total_creditos"] = (t["creaul"] + t["cretrb"]).astype(int)
    return t


def features_historico(cfg: DatasetConfig, t: pd.DataFrame) -> pd.DataFrame:
    """Perfil histórico reescrito em função de ``estmtr`` (sem ``nummtr``).

    Como o alvo é ``delta = nummtr_max - estmtr``, qualquer estatística que
    use ``nummtr_total``/``nummtr_max`` vazia o alvo. Reinterpretamos todas
    as séries em função do proxy ``estmtr_val`` (disponível no Dia D),
    computadas APENAS com semestres passados (``.shift(1).expanding()``):

    - ``media/max_hist_estmtr_(sufixo|dis)`` — nível histórico do proxy. Faz
      sentido: são a "memória" do tamanho típico da turma segundo o Júpiter;
      - ``hist_taxa_estouro`` — frequência com que o ``estmtr`` passado
      ULTRAPASSOU as vagas reais. Mede o quão agressivo o proxy tem sido
      frente à oferta de vagas (não envolve ``nummtr``).
    - ``hist_max_excesso`` — maior valor de ``estmtr - vagas_reais`` no
      passado (proxy de "pior excesso já sinalizado pelo Júpiter").
    """
    if "estmtr_val" not in t.columns:
        return t.copy()
    df = t.sort_values(["coddis", "sufixo", "ano_sem"]).reset_index(drop=True)

    g_suf = df.groupby(["coddis", "sufixo"], sort=False)["estmtr_val"]
    df["media_hist_sufixo"] = g_suf.transform(lambda s: s.shift(1).expanding().mean())
    df["max_hist_sufixo"] = g_suf.transform(lambda s: s.shift(1).expanding().max())

    g_dis = df.sort_values(["coddis", "ano_sem"]).groupby("coddis", sort=False)[
        "estmtr_val"
    ]
    df = df.sort_values(["coddis", "ano_sem"]).reset_index(drop=True)
    df["media_hist_dis"] = g_dis.transform(lambda s: s.shift(1).expanding().mean())
    df["max_hist_dis"] = g_dis.transform(lambda s: s.shift(1).expanding().max())

    df = df.sort_values(["coddis", "sufixo", "ano_sem"]).reset_index(drop=True)
    df["media_hist_estmtr"] = df["media_hist_sufixo"].fillna(df["media_hist_dis"])
    df["max_hist_estmtr"] = df["max_hist_sufixo"].fillna(df["max_hist_dis"])

    # Resíduo do proxy contra sua própria média histórica (passada): sinal
    # puro de estmtr, sem nummtr — diferença (não razão, que degeneraria).
    df["estmtr_residuo_media"] = (
        df["estmtr_val"] - df["media_hist_estmtr"]
    ).fillna(0)

    df["_over"] = (df["estmtr_val"] > df["vagas_reais"]).astype(int)
    df["_exc"] = np.where(
        df["estmtr_val"] > df["vagas_reais"],
        df["estmtr_val"] - df["vagas_reais"],
        0,
    )
    grp = df.groupby(["coddis", "sufixo"], sort=False)
    df["hist_taxa_estouro"] = (
        grp["_over"].transform(lambda s: s.shift(1).expanding().mean()).fillna(0)
    )
    df["hist_max_excesso"] = (
        grp["_exc"].transform(lambda s: s.shift(1).expanding().max()).fillna(0)
    )
    df.drop(
        columns=[
            "media_hist_sufixo",
            "max_hist_sufixo",
            "media_hist_dis",
            "max_hist_dis",
            "_over",
            "_exc",
        ],
        inplace=True,
    )
    return df


def features_demanda(
    cfg: DatasetConfig,
    t: pd.DataFrame,
    hist: pd.DataFrame,
    dados: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Estoque de reprovados e fluxo ideal por curso (top-K), agregados como
    colunas + métricas de pressão. Usa apenas o histórico anterior a cada
    semestre (sem vazamento do alvo)."""
    df = t.copy()
    grade = dados["grade"]
    hp = dados["habilprog"]
    cursos_ime = sorted(grade["codcur"].dropna().astype(int).unique().tolist())[
        : cfg.top_cursos
    ]

    # Reprovados por (disciplina, curso): casamos HISTESCOLARGR com o curso
    # do aluno via HABILPROGGR (codpes -> codcur), evitando multiplicar linhas
    # pela grade curricular.
    hist_cursos: pd.DataFrame | None = None
    if len(hist) and len(hp):
        hp_map = hp[["codpes", "codcur"]].drop_duplicates(["codpes", "codcur"]).copy()
        hp_map["codcur"] = pd.to_numeric(hp_map["codcur"], errors="coerce")
        hp_map = hp_map.dropna(subset=["codcur"])
        hg = hist.merge(hp_map, on="codpes", how="left").dropna(subset=["codcur"])
        hg["codcur"] = hg["codcur"].astype(int)
        hg = hg[hg["codcur"].isin(cursos_ime)]
        hg = _hist_com_ano_sem(hg)
        hist_cursos = hg

    rep_cols = [f"rep_{c}" for c in cursos_ime]
    fluxo_cols = [f"fluxo_{c}" for c in cursos_ime]
    for c in rep_cols + fluxo_cols:
        df[c] = 0

    for sem_alvo in tqdm(sorted(df["ano_sem"].unique()), desc="Demanda", unit="sem"):
        ano = sem_alvo // 10
        sem = sem_alvo % 10

        # Estoque de reprovados: último status do aluno por (disc,curso) antes
        # do sem_alvo. Conta apenas reprovações como "presas" no momento.
        rep: dict[tuple[str, int], int] = {}
        if hist_cursos is not None and len(hist_cursos):
            ph = hist_cursos[hist_cursos["ano_sem"] < sem_alvo]
            if len(ph):
                ult = (
                    ph.sort_values("ano_sem")
                    .groupby(["codpes", "coddis", "codcur"], sort=False)
                    .tail(1)
                )
                rep_set = ult[ult["rstfim"].isin(RSTFM_REPROVACAO)]
                for (coddis, codcur), g in rep_set.groupby(["coddis", "codcur"]):
                    rep[(coddis, int(codcur))] = len(g)
        for codcur in cursos_ime:
            col = f"rep_{codcur}"
            mask = df["ano_sem"] == sem_alvo
            df.loc[mask, col] = df.loc[mask, "coddis"].map(
                lambda d, c=codcur, _r=rep: _r.get((d, c), 0)
            )

        # Fluxo ideal: alunos ativos no semestre curricular nominal da
        # disciplina segundo a grade obrigatória, por curso, no sem_alvo.
        fluxo: dict[tuple[str, int], int] = {}
        if len(hp) and len(grade):
            ativos = hp[(hp["dtaclcgru"].isna()) | (hp["dtaclcgru"].dt.year >= ano)]
            ativos = ativos[ativos["codcur"].isin(cursos_ime)].copy()
            if len(ativos):
                ativos["ano_ing"] = ativos["dtaing"].dt.year
                ativos["sem_ing"] = np.where(ativos["dtaing"].dt.month > 6, 2, 1)
                ativos["sem_cursados"] = (
                    (ano - ativos["ano_ing"]) * 2 + (sem - ativos["sem_ing"]) + 1
                )
                ativos = ativos[ativos["sem_cursados"] > 0]
                obg = grade[grade["tipobg"] == "O"].drop_duplicates(
                    ["codcur", "codhab", "coddis"]
                )
                bf = ativos.merge(obg, on=["codcur", "codhab"], how="inner")
                bf = bf[
                    bf["sem_cursados"]
                    == pd.to_numeric(bf["numsemidl"], errors="coerce")
                ]
                if len(bf):
                    for (coddis, codcur), g in bf.groupby(["coddis", "codcur"]):
                        fluxo[(coddis, int(codcur))] = len(g)
        for codcur in cursos_ime:
            col = f"fluxo_{codcur}"
            mask = df["ano_sem"] == sem_alvo
            df.loc[mask, col] = df.loc[mask, "coddis"].map(
                lambda d, c=codcur, _f=fluxo: _f.get((d, c), 0)
            )

    for c in rep_cols + fluxo_cols:
        df[c] = df[c].fillna(0).astype(int)
    df["demanda_estimada_bruta"] = df[rep_cols + fluxo_cols].sum(axis=1)
    df["pressao_demanda"] = df["demanda_estimada_bruta"] / (df["vagas_reais"] + 1)
    return df


def _hist_com_ano_sem(hist: pd.DataFrame) -> pd.DataFrame:
    if "ano_sem" in hist.columns:
        return hist
    h = hist.copy()
    h["ano_sem"] = h["codtur"].str[:5].pipe(pd.to_numeric, errors="coerce")
    return h


def features_professor_horario(
    cfg: DatasetConfig, t: pd.DataFrame, dados: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Quantidade de docentes, turmas abertas, força histórica do docente,
    atratividade (Δ docente), e sinais de horário (noturno/sexta/dias)."""
    df = t.copy()
    minis = dados["ministrante"].drop_duplicates(["coddis", "codtur", "codpes_prof"])
    qtd_profs = minis.groupby(["coddis", "codtur"]).size().rename("qtd_professores")
    df = df.merge(qtd_profs, on=["coddis", "codtur"], how="left")
    df["qtd_professores"] = df["qtd_professores"].fillna(0).astype(int)

    turmas_ativas = df[df["statur"] != "D"]
    qtd_turmas = (
        turmas_ativas.groupby(["coddis", "ano_sem"]).size().rename("qtd_turmas_abertas")
    )
    df = df.merge(qtd_turmas, on=["coddis", "ano_sem"], how="left")
    df["qtd_turmas_abertas"] = df["qtd_turmas_abertas"].fillna(0).astype(int)

    # Força histórica do docente: média de ``estmtr`` (proxy disponível no
    # Dia D, sem ``nummtr``) das turmas que ele ministrou em semestres
    # PASSADOS. Anonimização determinística (LGPD). Não há contas degeneradas
    # aqui (uma média pura em estmtr, não uma razão envolvendo nummtr).
    mapa: dict[int, str] = {}
    cont: list[int] = [0]
    minis2 = minis.copy()
    minis2["id_prof"] = minis2["codpes_prof"].map(
        lambda v: anonimizar_codpes(v, mapa, cont)
    )
    turmas_past = df[["coddis", "codtur", "ano_sem", "estmtr_val"]]
    mp = minis2.merge(turmas_past, on=["coddis", "codtur"], how="inner")
    mp = mp.sort_values(["id_prof", "ano_sem"])
    mp["media_hist_prof"] = mp.groupby("id_prof")["estmtr_val"].transform(
        lambda s: s.shift(1).expanding().mean()
    )
    forca = (
        mp.groupby(["coddis", "codtur", "ano_sem"])["media_hist_prof"]
        .mean()
        .reset_index()
    )
    df = df.merge(forca, on=["coddis", "codtur", "ano_sem"], how="left")
    # Delta de atratividade do docente entre a turma atual e o semestre anterior
    df = df.sort_values(["coddis", "sufixo", "ano_sem"])
    df["delta_atratividade_docente"] = (
        df.groupby(["coddis", "sufixo"])["media_hist_prof"].diff().fillna(0)
    )

    # Horários (OCUPTURMA + PERIODOHORARIO já mesclados em 'ocup')
    ocup = dados["ocup"]
    if len(ocup):
        ocup = ocup[ocup["codtur"].isin(df["codtur"])].copy()
        ocup["horent"] = ocup["horent"].astype(str)
        ocup["horent_int"] = pd.to_numeric(ocup["horent"].str[:2], errors="coerce")
        ocup["flag_noturno"] = (ocup["horent_int"] >= 18).astype(int)
        ocup["flag_sexta"] = (ocup["diasmnocp"] == "sex").astype(int)
        agreg = (
            ocup.groupby(["coddis", "codtur"])
            .agg(
                qtd_dias_semana=("diasmnocp", "nunique"),
                flag_noturno=("flag_noturno", "max"),
                flag_sexta=("flag_sexta", "max"),
            )
            .reset_index()
        )
        df = df.merge(agreg, on=["coddis", "codtur"], how="left")
    for c in ["qtd_dias_semana", "flag_noturno", "flag_sexta"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = df[c].fillna(0 if c != "qtd_dias_semana" else 1).astype(int)
    return df


def features_ingressantes(
    cfg: DatasetConfig, t: pd.DataFrame, grade: pd.DataFrame
) -> pd.DataFrame:
    """Flag determinística: disciplina obrigatória de 1º semestre curricular
    ofertada em semestre ímpar. Baseada na GRADECURRICULAR da unidade."""
    df = t.copy()
    if len(grade):
        ing = grade[(grade["numsemidl"] == 1) & (grade["tipobg"] == "O")][
            "coddis"
        ].unique()
        df["flag_turma_ingressantes"] = np.where(
            (df["ano_sem"] % 10 == 1) & df["coddis"].isin(ing), 1, 0
        )
    else:
        df["flag_turma_ingressantes"] = 0
    return df


# ---------------------------------------------------------------------------
# Features avançadas (Módulos 2-4: espaço de fase, rede, concorrência, sincronia)
# ---------------------------------------------------------------------------
def features_espaco_fase(cfg: DatasetConfig, t: pd.DataFrame) -> pd.DataFrame:
    """Sinais de fase (velocidade/volatilidade) reescritos em função de
    ``estmtr`` apenas — sem ``nummtr_max``.

    O antigo resíduo ``δ = nummtr_max - estmtr`` era LAGGED e portanto não
    vazava o alvo do semestre corrente, mas o usuário optou pela regra mais
    conservadora de **nenhuma** feature derivada de ``nummtr`` (sequer
    lagged), para isolar totalmente o alvo ``delta``. As contas que
    sobrevivem têm interpretação autossuficiente em ``estmtr``:

    - ``var_pct_estmtr`` — ``Δestmtr / estmtr_passado`` (tendência do proxy
      entre semestres consecutivos lagged). Não é razão ``nummtr/estmtr``
      (que degeneraria em 1): numerador e denominador vêm de estágios
      temporais distintos da mesma série.
    - ``d_estmtr_dt_t1`` — diferença (variação absoluta) do ``estmtr`` entre
      os dois semestres anteriores (velocidade do proxy, sem normalização).
    - ``volatilidade_estmtr`` — desvio-padrão expansivo do ``estmtr``
      passado: quão estável é o proxy da turma ao longo do tempo.
    """
    if "estmtr_val" not in t.columns:
        return t.copy()
    df = t.sort_values(["coddis", "sufixo", "ano_sem"]).reset_index(drop=True)
    g = df.groupby(["coddis", "sufixo"], sort=False)
    prev_est = g["estmtr_val"].shift(1)
    # Variação percentual: Δ / proxy_passado (denominador ≠ 0). Não degenera.
    df["var_pct_estmtr"] = (
        (df["estmtr_val"] - prev_est) / prev_est.replace(0, np.nan)
    ).fillna(0)
    # Velocidade absoluta lagged — puro estmtr.
    df["d_estmtr_dt_t1"] = prev_est.diff().fillna(0)
    # Volatilidade expansiva do proxy.
    df["volatilidade_estmtr"] = (
        g["estmtr_val"]
        .transform(lambda s: s.shift(1).expanding().std())
        .fillna(0)
    )
    return df


def features_rede_requisitos(
    cfg: DatasetConfig, t: pd.DataFrame, grade: pd.DataFrame, hist: pd.DataFrame
) -> pd.DataFrame:
    """Pressão de represamento a jusante (reprovações em pré-requisitos no
    t−1) e métricas topológicas (in/out-degree, betweenness, PageRank) do
    grafo de pré-requisitos da unidade."""
    df = t.copy()
    g_req = _montar_grafo_requisitos(grade)
    if g_req is not None and nx is not None and g_req.number_of_edges() > 0:
        betw = nx.betweenness_centrality(g_req)
        pr = nx.pagerank(g_req, alpha=0.85)
        in_deg = dict(g_req.in_degree())
        out_deg = dict(g_req.out_degree())
        df["net_in_degree"] = df["coddis"].map(in_deg).fillna(0).astype(int)
        df["net_out_degree"] = df["coddis"].map(out_deg).fillna(0).astype(int)
        df["net_betweenness"] = df["coddis"].map(betw).fillna(0.0)
        df["net_pagerank"] = df["coddis"].map(pr).fillna(0.0)
    else:
        df["net_in_degree"] = 0
        df["net_out_degree"] = 0
        df["net_betweenness"] = 0.0
        df["net_pagerank"] = 0.0

    # Mapa rápido disciplina -> predecessores (pré-requisitos diretos).
    preds_map: dict[str, Sequence[str]] = {}
    if g_req is not None:
        for d in df["coddis"].unique():
            preds_map[d] = _predecessores(g_req, d)

    # Pressão represada por turma: soma de reprovados nos pré-requisitos
    # diretos no semestre imediatamente anterior (t-1), normalizada pelo
    # estmtr da própria turma (+1). Usa apenas passado.
    hg = _hist_com_ano_sem(hist) if len(hist) else hist
    rep_prev: dict[tuple[str, int], int] = {}
    if len(hg):
        rset = hg[hg["rstfim"].isin(RSTFM_REPROVACAO)]
        rset = rset.groupby(["coddis", "ano_sem"]).size().rename("n").reset_index()
        rmap = {
            (r["coddis"], int(r["ano_sem"])): int(r["n"]) for _, r in rset.iterrows()
        }
        for disc_atual, sem_alvo in df[["coddis", "ano_sem"]].itertuples(index=False):
            preds = preds_map.get(disc_atual, [])
            if not preds:
                continue
            sem_prev = (sem_alvo // 10 - (0 if sem_alvo % 10 == 1 else 1)) * 10 + (
                1 if sem_alvo % 10 == 2 else 2
            )
            if sem_prev < cfg.ano_min * 10:
                continue
            press = sum(rmap.get((p, sem_prev), 0) for p in preds)
            sub = df[(df["ano_sem"] == sem_alvo) & (df["coddis"] == disc_atual)]
            denom = int(sub["vagas_reais"].iloc[0]) if len(sub) else 1
            rep_prev[(disc_atual, sem_alvo)] = press / max(denom, 1)

    df["pressao_represada"] = df.set_index(["coddis", "ano_sem"]).index.map(
        lambda k: rep_prev.get(k, 0.0)
    )
    return df


def _montar_grafo_requisitos(grade: pd.DataFrame):
    """Constrói um DiGraph aproximado de pré-requisitos.

    A GRADECURRICULAR não traz as arestas diretas; como fallback aproximado,
    criamos arestas (coddis_sem_n < coddis_sem_n+k) dentro de cada
    (codcur, codhab): disciplinas de semestre menor precedem as de maior
    semestre. Isso aproxima a topologia quando GRUPOREQUISITO não está
    disponível.
    """
    if nx is None or grade is None or not len(grade):
        return None
    g = nx.DiGraph()
    grade = grade.dropna(subset=["coddis", "numsemidl"]).copy()
    grade["numsemidl"] = pd.to_numeric(grade["numsemidl"], errors="coerce")
    for (_codcur, _codhab), grp in grade.groupby(["codcur", "codhab"]):
        sems = grp.drop_duplicates("coddis")[["coddis", "numsemidl"]].sort_values(
            "numsemidl"
        )
        discs = sems["coddis"].tolist()
        for i in range(len(discs)):
            for j in range(i + 1, len(discs)):
                g.add_edge(discs[i], discs[j])
    return g


def _predecessores(g: Any, disc: str) -> Sequence[str]:
    if g is None:
        return []
    try:
        return list(g.predecessors(disc))
    except nx.NetworkXError:
        return []


def features_concorrencia_horaria(
    cfg: DatasetConfig, t: pd.DataFrame, dados: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Concorrência horária: variação média do estmtr dos vizinhos do mesmo
    bloco de horário (excluindo a própria turma) e indicador de sincronia do
    bloco (densidade real do bloco, agregando 1 linha por turma mesmo quando
    ela tem múltiplos horários/dias)."""
    df = t.copy()
    ocup = dados["ocup"]
    if "var_pct_estmtr" not in df.columns:
        df["var_pct_estmtr"] = 0.0
    if not len(ocup):
        df["delta_estmtr_vizinhos"] = 0.0
        df["ind_sincronia_bloco"] = 0.0
        return df

    o = ocup[["coddis", "codtur", "diasmnocp", "horent"]].copy()
    o = o[o["codtur"].isin(df["codtur"])]
    if not len(o):
        df["delta_estmtr_vizinhos"] = 0.0
        df["ind_sincronia_bloco"] = 0.0
        return df
    o["bloco"] = o["diasmnocp"].astype(str) + "_" + o["horent"].astype(str)
    blocos = o[["coddis", "codtur", "bloco"]].drop_duplicates()

    # Cada turma pode pertencer a 1+ blocos (multi-dia); replicamos a turma
    # por bloco, calculamos vizinhança por bloco e agregamos (1 linha final
    # por turma) para não inflar o dataset.
    tmp = df[["coddis", "codtur", "ano_sem", "var_pct_estmtr"]].copy()
    tmp = tmp.merge(blocos, on=["coddis", "codtur"], how="left")
    tmp = tmp.dropna(subset=["bloco"]).copy() if len(tmp) else tmp
    if not len(tmp):
        df["delta_estmtr_vizinhos"] = 0.0
        df["ind_sincronia_bloco"] = 0.0
        return df
    tmp["bloco_sem"] = tmp["bloco"].astype(str) + "_" + tmp["ano_sem"].astype(str)

    soma = tmp.groupby("bloco_sem")["var_pct_estmtr"].transform("sum")
    qtd = tmp.groupby("bloco_sem")["var_pct_estmtr"].transform("count")
    tmp["delta_vizinhos_bloco"] = np.where(
        qtd > 1, (soma - tmp["var_pct_estmtr"]) / (qtd - 1), 0.0
    )
    # Sincronia: densidade do próprio bloco (fração de co-turmas que existem);
    # 0 quando há só a própria, tende a 1 em blocos cheios.
    tmp["sincronia_bloco"] = np.where(qtd > 1, (qtd - 1) / qtd, 0.0)

    agg = (
        tmp.groupby(["coddis", "codtur", "ano_sem"])
        .agg(
            delta_estmtr_vizinhos=("delta_vizinhos_bloco", "mean"),
            ind_sincronia_bloco=("sincronia_bloco", "max"),
        )
        .reset_index()
    )
    df = df.merge(agg, on=["coddis", "codtur", "ano_sem"], how="left")
    df["delta_estmtr_vizinhos"] = df["delta_estmtr_vizinhos"].fillna(0.0)
    df["ind_sincronia_bloco"] = df["ind_sincronia_bloco"].fillna(0.0)
    return df


# ---------------------------------------------------------------------------
# Montagem final
# ---------------------------------------------------------------------------
COLUNAS_IDENT = [
    "coddis",
    "verdis",
    "codtur",
    "ano_sem",
    "ano",
    "sem_tipo",
    "sufixo",
    "departamento",
    "tiptur",
    "statur",
]
# Único alvo preditivo: ``delta = nummtr_max - estmtr`` — o quanto o proxy
# institucional ``estmtr`` (feature, conhecido no Dia D) erra em relação ao
# pico de ocupação esperado. ``estmtr`` é FEATURE (não aqui); ``nummtr_max``
# cru também NÃO é feature: é ingrediente do alvo e descartado.
COLUNAS_ALVO = ["delta"]

# Colunas cruas da TURMAGR/HIST que consolidam DEPOIS do "Dia D" (ou que
# reconstruem o próprio alvo) e portanto vazam o alvo se usadas como feature.
# O dataset sai sem elas. O ``delta`` é derivado explicitamente de
# ``nummtr_max`` (reconstrução de ocupação) e ``estmtr`` (reconstrução do
# proxy), e em seguida tanto o ``nummtr_max`` cru quanto as ``ocup_d+*`` que
# o geram, quanto o ``nummtr`` consolidado da TURMAGR, são descartados.
COLUNAS_VAZAMENTO = [
    "numins",
    "numinsopt",
    "numinsoptlre",
    "numinscpl",
    "numinsecr",
    "numpmtobg",
    "numpmtopt",
    "numpmtoptlre",
    "numpmtcpl",
    "numpmtecr",
    "nummtr",
    "nummtropt",
    "nummtroptlre",
    "nummtrturcpl",
    "nummtrecr",
    "numvagtur",
    "numvagopt",
    "numvagoptlre",
    "numvagturcpl",
    "numvagecr",
    "dtacritur",
    "dtafimtur",
    "dtainitur",
    "tipmtr",
    "estmtr_val",
    "nummtr_max",
    "nummtr_total",
    "dta_corte",
]
COLUNAS_DESCARTE = [
    *COLUNAS_VAZAMENTO,
    "ocup_d+0",
    "ocup_d+7",
    "ocup_d+11",
    "ocup_d+14",
    "ocup_d+21",
]


def montar_dataset(
    cfg: DatasetConfig | None = None,
    forcar_extracao: bool = False,
) -> pd.DataFrame:
    """Constrói o DataFrame mestre (features + alvo) por turma.

    Sai em ``cfg.saida`` (CSV). Retorna o DataFrame.
    """
    cfg = cfg or DatasetConfig.from_env()
    print(
        f"=== DatasetConfig ===\n  codundclg: {cfg.codundclg}\n  prefixos: {cfg.prefixos}\n"
        f"  anos: {cfg.ano_min}-{cfg.ano_max}\n  sufixo_min: {cfg.sufixo_min}\n"
        f"  saida: {cfg.saida}"
    )

    dados = carregar_dados(cfg, forcar=forcar_extracao)
    turmas_f = filtrar_turmas(cfg, dados["turmas"])
    print(f"Turmas no escopo: {len(turmas_f)}")

    # ``estmtr`` (feature central) e ``nummtr_max`` (ingrediente do alvo).
    # Nenhum ``nummtr`` consoliddado vira coluna: vaza o alvo.
    est = reconstruir_estmtr(cfg, turmas_f, dados["hist"])
    pico = reconstruir_alvo_nummtr_max(cfg, turmas_f, dados["hist"])

    # Base
    df = features_base(cfg, turmas_f, dados)
    df = df.merge(est, on=["coddis", "codtur"], how="left")
    df = df.merge(pico, on=["coddis", "codtur"], how="left")
    df["estmtr"] = df["estmtr_val"].astype(int)
    df["nummtr_max"] = df["nummtr_max"].astype(int)
    # Alvo único: ``delta = nummtr_max - estmtr``. O modelo aprende a
    # correção a ser aplicada ao proxy do Júpiter no Dia D.
    df["delta"] = df["nummtr_max"] - df["estmtr"]

    # Histórico + demanda + prof/horário + ingressantes (todos em estmtr)
    df = features_historico(cfg, df)
    df = features_demanda(cfg, df, dados["hist"], dados)
    df = features_professor_horario(cfg, df, dados)
    df = features_ingressantes(cfg, df, dados["grade"])

    # Avançadas (precisam de estmtr_val ainda presente como auxiliar)
    df = features_espaco_fase(cfg, df)
    df = features_rede_requisitos(cfg, df, dados["grade"], dados["hist"])
    df = features_concorrencia_horaria(cfg, df, dados)

    df.drop(columns=["estmtr_val", "nummtr_total"], inplace=True, errors="ignore")

    # Ordenação final + remoção de colunas que vazam o alvo (inclui o
    # ``nummtr_max`` cru e as ``ocup_d+*`` que o geraram).
    df = df.sort_values(["ano_sem", "coddis", "sufixo", "codtur"]).reset_index(
        drop=True
    )
    for c in COLUNAS_DESCARTE:
        if c in df.columns:
            df.drop(columns=[c], inplace=True)

    cfg.saida.parent.mkdir(parents=True, exist_ok=True)
    with tqdm(total=1, desc="Salvando CSV", unit="file") as bar:
        df.to_csv(cfg.saida, index=False)
        bar.update(1)
    print(
        f"\nDataset salvo em {cfg.saida} ({len(df)} turmas, {len(df.columns)} colunas)"
    )
    print("Colunas alvo:", [c for c in COLUNAS_ALVO if c in df.columns])

    return df


def main(argv: Iterable[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Fornecedor de dataset de alocação")
    p.add_argument(
        "--codundclg", type=int, default=None, help="Colegiado/unidade (default env/45)"
    )
    p.add_argument("--prefixos", nargs="+", default=None, help="Prefixos de disciplina")
    p.add_argument("--sufixo-min", type=int, default=None)
    p.add_argument("--ano-min", type=int, default=None)
    p.add_argument("--ano-max", type=int, default=None)
    p.add_argument(
        "--forcar-extracao", action="store_true", help="Reextrai tabelas auxiliares"
    )
    p.add_argument("--saida", type=Path, default=None)
    args = p.parse_args(list(argv) if argv is not None else None)

    overrides: dict[str, Any] = {}
    if args.codundclg is not None:
        overrides["codundclg"] = args.codundclg
    if args.prefixos:
        overrides["prefixos"] = tuple(args.prefixos)
    if args.sufixo_min is not None:
        overrides["sufixo_min"] = args.sufixo_min
    if args.ano_min is not None:
        overrides["ano_min"] = args.ano_min
    if args.ano_max is not None:
        overrides["ano_max"] = args.ano_max
    if args.saida is not None:
        overrides["saida"] = args.saida

    cfg = DatasetConfig.from_env(**overrides)
    montar_dataset(cfg, forcar_extracao=args.forcar_extracao)
    return 0


if __name__ == "__main__":
    sys.exit(main())
