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
                              PROGRAMAGR, HABILPROGGR, HABILITACAOGR, MINISTRANTE,
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
                              + ingressantes; mais features de **vagas do
                              curso** (``vagas_curso_<codcur>`` e
                              ``vagas_curso_<codcur>_faltam`` reconstruídas de
                              HABILITACAOGR por datas de vigência, com
                              continuidade passada); e as features **avançadas**
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

    O colegiado (``codundclg``) e os prefixos de disciplina (``prefixos``) NÃO
    têm fallback: devem estar no ``.env`` (``REPLICADO_CODUNDCLG`` /
    ``REPLICADO_PREFIXOS_DISC``) ou passados no CLI. Ausência => erro explícito.
"""

from __future__ import annotations

import sys
import warnings
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
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

# Defaults genéricos (não atrelados a nenhuma unidade). Colegiado e prefixos
# de disciplina NÃO têm default: devem vir do ``.env`` (``from_env``) ou do
# CLI, e a ausência levanta ``ValueError``.
DEFAULT_SUFIXO_MIN = 40
DEFAULT_ANO_MIN = 2010
DEFAULT_ANO_MAX = datetime.now().year  # ano corrente; exige cache já extraído.
DEFAULT_DIAS_CORTE = 5
DEFAULT_PISO_VAGAS = 30
DEFAULT_DIAS_PICO = (0, 7, 11, 14, 21)
DEFAULT_TOP_CURSOS = 12

RSTFM_REPROVACAO = ("RN", "RF", "RA", "AB")

# Colunas da TURMAGR efetivamente usadas pelo pipeline (lista explícita
# para nunca depender de ``SELECT *``). É a **fonte da verdade**: o script
# ``scripts/extrair_cache_replicado.py`` importa destaqui, e
# :func:`carregar_dados` (caminho ``forcar=True``) também. Usar ``SELECT *``
# injetaria colunas-brutas de texto/código (obstur, timestamp, cgahorpra,
# cgahorteo, obsadctur, codlindis, staexucslmtr, staofesgdavl, staturati)
# que virariam feature e quebrariam o esquema estável consumido pelo modelo.
COLS_TURMAGR = """
    coddis, verdis, codtur, tiptur, tipmtr, dtainitur, dtafimtur, statur,
    dtacritur, numvagtur, numvagopt, numvagoptlre, numvagturcpl, numvagecr,
    numins, numinsopt, numinsoptlre, numinscpl, numinsecr,
    numpmtobg, numpmtopt, numpmtoptlre, numpmtcpl, numpmtecr,
    nummtr, nummtropt, nummtroptlre, nummtrturcpl, nummtrecr
"""


# ---------------------------------------------------------------------------
# Configuração agnóstica à unidade
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DatasetConfig:
    """Parâmetros de escopo / saneamento do dataset, independentes da unidade.

    ``codundclg`` e ``prefixos`` são **obrigatórios** (sem fallback de
    unidade): devem vir do ``.env`` (:meth:`from_env`) ou serem passados
    explicitamente pelo CLI. Os demais têm defaults genéricos.
    """

    codundclg: int
    prefixos: tuple[str, ...]
    sufixo_min: int = DEFAULT_SUFIXO_MIN
    ano_min: int = DEFAULT_ANO_MIN
    ano_max: int = DEFAULT_ANO_MAX
    dias_corte: int = DEFAULT_DIAS_CORTE
    piso_vagas: int = DEFAULT_PISO_VAGAS
    dias_pico: tuple[int, ...] = DEFAULT_DIAS_PICO
    top_cursos: int = DEFAULT_TOP_CURSOS
    cache_dir: Path = CACHE_DIR
    saida: Path = SAIDA_DEFAULT
    # Catálogos fixos de ``codcur`` (freeze de schema). Quando definidos, as
    # features ``rep_<codcur>``/``fluxo_<codcur>`` e ``vagas_curso_<codcur>``
    # enumeram EXATAMENTE estes cursos, na ORDEM dada — independentemente do
    # que CURSOGR/HABILITACAOGR digam hoje. É o freeze de longo prazo: um
    # .pkl treinado hoje continua enxergando as mesmas colunas mesmo se um
    # curso for desativado/criado no banco amanhã (zeros no slot inativo).
    # A ORDEM dos inteiros na tupla DEFINE a ordem das colunas no CSV; não
    # reordene — modelos treinados dependem dela (``feature_name_``).
    # Se ``None``: fallback ao comportamento dinâmico histórico (com warn).
    cursos_rep_fluxo: tuple[int, ...] | None = None
    cursos_vagas: tuple[int, ...] | None = None

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

        def env_int_csv(name: str) -> tuple[int, ...] | None:
            v = os.getenv(name)
            if not v or not v.strip():
                return None
            try:
                return tuple(int(p.strip()) for p in v.split(",") if p.strip())
            except ValueError as e:
                raise ValueError(f"{name} deve ser uma CSV de inteiros: {e}") from e

        cfg = {
            "codundclg": overrides.pop("codundclg", env_int("REPLICADO_CODUNDCLG")),
            "prefixos": overrides.pop("prefixos", env_csv("REPLICADO_PREFIXOS_DISC")),
            "sufixo_min": env_int("REPLICADO_SUFIXO_MIN"),
            "ano_min": env_int("REPLICADO_ANO_MIN"),
            "ano_max": env_int("REPLICADO_ANO_MAX"),
            "dias_corte": env_int("REPLICADO_DIAS_CORTE"),
            "piso_vagas": env_int("REPLICADO_PISO_VAGAS"),
            "top_cursos": env_int("REPLICADO_TOP_CURSOS"),
            "cursos_rep_fluxo": overrides.pop(
                "cursos_rep_fluxo", env_int_csv("REPLICADO_CURSOS_REP_FLUXO")
            ),
            "cursos_vagas": overrides.pop(
                "cursos_vagas", env_int_csv("REPLICADO_CURSOS_VAGAS")
            ),
        }
        cfg = {k: v for k, v in cfg.items() if v is not None}
        cfg.update(overrides)
        if cfg.get("codundclg") is None or cfg.get("prefixos") is None:
            raise ValueError(
                "As variáveis REPLICADO_CODUNDCLG (int) e REPLICADO_PREFIXOS_DISC "
                "(lista CSV) são obrigatórias no .env (ou via CLI "
                "--codundclg/--prefixos) para definir o escopo da unidade."
            )
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


def carregar_dados(
    cfg: DatasetConfig,
    forcar: bool = False,
    *,
    atualizar_anos: Iterable[int] = (),
) -> dict[str, pd.DataFrame]:
    """Carrega/cacheia todas as tabelas usadas pelas features.

    TURMAGR e HISTESCOLARGR aproveitam o cache já produzido por
    ``scripts/extrair_cache_replicado.py`` (cross-unit, sem sufixo de unidade).
    As auxiliares são filtradas por ``codundclg`` e cacheadas com sufixo
    ``_<codundclg>.pkl`` para isolar diferentes unidades.

    Refresh da HISTESCOLARGR (ortogonal ao ``forcar``):

    Por padrão ``carregar_dados`` só lia HISTESCOLARGR do cache (pickle
    existente) ou emitia ``[aviso]`` se faltasse — **ignorando ``forcar``**,
    um bug de coerência: ``montar_dataset(forcar_extracao=True)`` não
    refazia o histórico. Agora o loop de histórico respeita:

    - **``atualizar_anos`` não-vazio** (lista explícita de anos quentes):
      re-extraí do banco EXATAMENTE os anos listados, sobrescrevendo
      ``histescolar_<ano>.pkl``; os demais anos vêm do cache existente
      (ou ``[aviso]`` se faltar). ``forcar`` É IGNORADO para a HISTESCOLARGR
      neste modo — é o caminho cirúrgico/lean do retreino
      (T_pico/estmtr só mudam de fato nos últimos anos letivos).
    - **``atualizar_anos`` vazio (default)**: ``forcar`` governa a HIST
      (bug fix honrando o nome do flag). ``forcar=True`` re-extrai TODAS as
      fatias; ``forcar=False`` lê do cache (ou ``[aviso]`` se faltar).

    Assim, ``montar_dataset(forcar_extracao=True, atualizar_anos=[a-1,a])``
    refaz TURMAGR + auxiliares (via ``forcar``) e só os 2 anos quentes de
    HIST (via ``atualizar_anos``) — uma chamada para o pipeline de retreino.
    """
    dados: dict[str, pd.DataFrame] = {}
    cod = cfg.codundclg
    cache = cfg.cache_dir

    # --- TURMAGR (já existe via extrair_cache_replicado.py) ----------------
    c = cache / "turmagr_full.pkl"
    if c.exists() and not forcar:
        dados["turmas"] = _load_pickled(c)
    else:
        dados["turmas"] = _stream_to_pickle(c, f"SELECT {COLS_TURMAGR} FROM TURMAGR", "TURMAGR")
    t = dados["turmas"].copy()
    t["coddis"] = t["coddis"].astype(str).str.strip().str.upper()
    t["codtur"] = t["codtur"].astype(str).str.strip()
    dados["turmas"] = t

    # --- HISTESCOLARGR por ano (refresh incremental) ----------------------
    # ``atualizar_anos`` (não-vazio) re-extraí cirurgicamente só os anos
    # listados — ``forcar`` é IGNORADO para HIST neste modo. Com
    # ``atualizar_anos`` vazio, ``forcar`` governa (bug fix: ``forcar=True``
    # re-extraí todas as fatias; default lê do cache). Ver docstring acima.
    atualizar_set = {int(a) for a in atualizar_anos}
    hist_chunks = []
    anos_existentes = []
    # Import tardio p/ evitar import circular (cache.py importa COLS_TURMAGR
    # deste módulo).
    from .cache import extrair_fatia_histescolar

    for ano in cfg.anos:
        c = cache / f"histescolar_{ano}.pkl"
        if atualizar_set:
            extrair = ano in atualizar_set
        else:
            extrair = forcar
        if extrair:
            hist_chunks.append(extrair_fatia_histescolar(ano, cache))
            anos_existentes.append(ano)
        elif c.exists():
            hist_chunks.append(_load_pickled(c))
            anos_existentes.append(ano)
        else:
            print(
                f"[aviso] {c.name} ausente — rode scripts/extrair_cache_replicado.py",
                file=sys.stderr,
            )
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
        (
            "habilit",
            f"SELECT H.codcur, H.codhab, H.nomhab, H.numvaghab, H.numvaghabcpl, "
            f"H.numvaghabcvn, H.dtaatvhab, H.dtadtvhab "
            f"FROM HABILITACAOGR H "
            f"INNER JOIN CURSOGR C ON H.codcur = C.codcur "
            f"WHERE C.codclg = {cod}",
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

    # datas de vigência das habilitações (período ativo p/ reconstrução de
    # vagas do curso por ano).
    hb = dados.get("habilit")
    if hb is not None and len(hb):
        hb = hb.copy()
        hb["dtaatvhab"] = pd.to_datetime(hb["dtaatvhab"], errors="coerce")
        hb["dtadtvhab"] = pd.to_datetime(hb["dtadtvhab"], errors="coerce")
        dados["habilit"] = hb

    # Tabelas auxiliares dos macro-sensores (CALENDGR, REQUERIMENTOGR e
    # HISTPROGGR filtradas IME). Ver :mod:`replicado.dataset_macrosensores`.
    from .dataset_macrosensores import carregar_macrosensores

    carregar_macrosensores(cfg, dados, forcar=forcar)
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
    """Perfil histórico com "verdade terrestre" defasada e janelas deslizantes.

    O alvo é ``delta = nummtr_max - estmtr``. A versão anterior evitava
    ``nummtr``/``delta`` a todo custo (apenas ``estmtr_val``), exagerando no
    conservadorismo. Aqui reintroduzimos o passado CONSOLIDADO de forma
    segura:

    - **Lags t-1 / t-2** de ``delta`` e ``nummtr_max`` (verdade terrestre
      defasada). A linhagem temporal de uma sala (na USP) é a junção
      disciplina+turma, então o agrupamento ANTES do ``.shift()`` é
      obrigatoriamente ``['coddis', 'sufixo']``, ordenado por ``ano_sem``.
      O ``.shift()`` impede qualquer vazamento do semestre corrente.
    - **Janelas deslizantes** (``rolling(window=3, min_periods=1)``) no
      lugar de ``.expanding()`` — dilui menos a tendência recente.
    - ``media_rolling_nummtr_max_3sem`` — tendência real baseada no
      consolidado passado (lagged), não no proxy ``estmtr``.
    ``delta``/``nummtr_max`` crus permanecem apenas como insumo dos lags e
    são descartados a jusante (``COLUNAS_VAZAMENTO``); os lags viram feature.
    """
    if "estmtr_val" not in t.columns:
        return t.copy()
    df = t.sort_values(["coddis", "sufixo", "ano_sem"]).reset_index(drop=True)

    # --- Lags (verdade terrestre defasada) -----------------------------------
    # Agrupamento OBRIGATÓRIO por [coddis, sufixo] (linhagem da sala) ANTES do
    # .shift(): garante zero vazamento do semestre corrente. Ordenação por
    # ano_sem fora do grupo assegura que shift(1)=semestre imediatamente
    # anterior e shift(2)=mesmo semestre letivo do ano anterior (sazonal).
    # Agregamos por (coddis, sufixo, ano_sem) antes do shift para tolerar
    # múltiplas turmas da mesma linhagem num mesmo semestre.
    lag_cols = []
    if "delta" in df.columns:
        lag_cols.append("delta")
    if "nummtr_max" in df.columns:
        lag_cols.append("nummtr_max")
    if lag_cols:
        agg_sem = (
            df.groupby(["coddis", "sufixo", "ano_sem"], sort=False)[lag_cols]
            .mean()
            .sort_index()
            .reset_index()
        )
        for col in lag_cols:
            g = agg_sem.groupby(["coddis", "sufixo"], sort=False)[col]
            agg_sem[f"{col}_t1"] = g.shift(1)  # semestre imediatamente anterior
            agg_sem[f"{col}_t2"] = g.shift(2)  # sazonal: mesmo semestre ano anterior
        df = df.merge(
            agg_sem[
                ["coddis", "sufixo", "ano_sem"]
                + [f"{c}_t1" for c in lag_cols]
                + [f"{c}_t2" for c in lag_cols]
            ],
            on=["coddis", "sufixo", "ano_sem"],
            how="left",
        )
        for c in [f"{c}_{k}" for c in lag_cols for k in ("t1", "t2")]:
            df[c] = df[c].fillna(0)

    # --- Janelas deslizantes (rolling) no lugar de expanding -----------------
    # Tendência recente: janela fixa de 3 semestres (min_periods=1 para os
    # primórdios). Lag de 1 via .shift(1) exclui o próprio semestre.
    g_suf = df.groupby(["coddis", "sufixo"], sort=False)["estmtr_val"]
    df["media_hist_sufixo"] = g_suf.transform(
        lambda s: s.shift(1).rolling(window=3, min_periods=1).mean()
    )
    df["max_hist_sufixo"] = g_suf.transform(
        lambda s: s.shift(1).rolling(window=3, min_periods=1).max()
    )

    g_dis = df.sort_values(["coddis", "ano_sem"]).groupby("coddis", sort=False)[
        "estmtr_val"
    ]
    df = df.sort_values(["coddis", "ano_sem"]).reset_index(drop=True)
    df["media_hist_dis"] = g_dis.transform(
        lambda s: s.shift(1).rolling(window=3, min_periods=1).mean()
    )
    df["max_hist_dis"] = g_dis.transform(
        lambda s: s.shift(1).rolling(window=3, min_periods=1).max()
    )

    df = df.sort_values(["coddis", "sufixo", "ano_sem"]).reset_index(drop=True)
    df["media_hist_estmtr"] = df["media_hist_sufixo"].fillna(df["media_hist_dis"])
    df["max_hist_estmtr"] = df["max_hist_sufixo"].fillna(df["max_hist_dis"])

    # Tendência REAL baseada no consolidado passado (lagged). Agrupada por
    # [coddis, sufixo] + .shift(1) fora do grupo => imune a vazamento.
    if "nummtr_max" in df.columns:
        g_max = df.groupby(["coddis", "sufixo"], sort=False)["nummtr_max"]
        df["media_rolling_nummtr_max_3sem"] = g_max.transform(
            lambda s: s.shift(1).rolling(window=3, min_periods=1).mean()
        ).fillna(df["media_hist_estmtr"].fillna(0))

    # Resíduo do proxy contra sua própria média histórica (passada): sinal
    # puro de estmtr, sem nummtr — diferença (não razão, que degeneraria).
    df["estmtr_residuo_media"] = (df["estmtr_val"] - df["media_hist_estmtr"]).fillna(0)

    df["_over"] = (df["estmtr_val"] > df["vagas_reais"]).astype(int)
    df["_exc"] = np.where(
        df["estmtr_val"] > df["vagas_reais"],
        df["estmtr_val"] - df["vagas_reais"],
        0,
    )
    grp = df.groupby(["coddis", "sufixo"], sort=False)
    df["hist_taxa_estouro"] = (
        grp["_over"]
        .transform(lambda s: s.shift(1).rolling(window=3, min_periods=1).mean())
        .fillna(0)
    )
    df["hist_max_excesso"] = (
        grp["_exc"]
        .transform(lambda s: s.shift(1).rolling(window=3, min_periods=1).max())
        .fillna(0)
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
    if cfg.cursos_rep_fluxo is not None:
        # Freeze: enumerar EXATAMENTE o catálogo, na ordem dada. Garante que
        # rep_/fluxo_<codcur> virem as mesmas colunas (e na mesma ordem) mesmo
        # que a grade mude no banco — preserva ``feature_name_`` do .pkl.
        top_cursos_ativos = list(cfg.cursos_rep_fluxo)
    else:
        warnings.warn(
            "REPLICADO_CURSOS_REP_FLUXO não definido: top-K de cursos para "
            "rep_/fluxo_ derivado dinamicamente da grade (sorted asc, slice "
            "top_cursos). Defina a variável no .env para um esquema de "
            "colunas estável entre extrações (ver .env.example / AGENTS.md).",
            stacklevel=2,
        )
        top_cursos_ativos = sorted(
            grade["codcur"].dropna().astype(int).unique().tolist()
        )[: cfg.top_cursos]

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
        hg = hg[hg["codcur"].isin(top_cursos_ativos)]
        hg = _hist_com_ano_sem(hg)
        hist_cursos = hg

    rep_cols = [f"rep_{c}" for c in top_cursos_ativos]
    fluxo_cols = [f"fluxo_{c}" for c in top_cursos_ativos]
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
        for codcur in top_cursos_ativos:
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
            ativos = ativos[ativos["codcur"].isin(top_cursos_ativos)].copy()
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
        for codcur in top_cursos_ativos:
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


def _vagas_curso_no_ano(hab: pd.DataFrame, cod: int, ano: int) -> int:
    """Total de vagas cadastrais do ``codcur`` vigentes em ``1o/jul/ano``.

    Soma ``numvaghab + numvaghabcpl + numvaghabcvn`` de TODAS as habilitações
    (qualquer ``codhab``) ativas no corte. A reconstrução por datas de
    vigência (``dtaatvhab`` / ``dtadtvhab``) garante continuidade para o
    passado: anos sem registro em ``HABILVAGA`` recebem o snapshot cadastral
    daquele ano. Cursos só mudam de habilitações — raramente surgem novos
    —  então o histórico é razoavelmente estável.
    """
    corte = pd.Timestamp(year=ano, month=7, day=1)
    h = hab[
        (hab["codcur"] == cod)
        & (hab["dtaatvhab"].isna() | (hab["dtaatvhab"] <= corte))
        & (hab["dtadtvhab"].isna() | (hab["dtadtvhab"] > corte))
    ]
    return int(h["vag_total"].sum()) if len(h) else 0


def features_vagas_curso(
    cfg: DatasetConfig, t: pd.DataFrame, dados: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Vagas do curso (soma dos ``codhab`` vigentes no ano) e sinal de
    "alunos que faltam se matricular" no Dia D.

    Para cada ``codcur`` ativo HOJE (≥1 habilitação sem ``dtadtvhab`` preenchida
    ou com ``dtadtvhab`` futura), cria duas colunas por turma:

    - ``vagas_curso_<codcur>`` — soma das vagas cadastrais das habilitações
      vigentes em ``1o/jul/ano`` da turma. Reconstruído de
      ``HABILITACAOGR`` (datas de vigência) → dá continuidade para o passado
      mesmo onde ``HABILVAGA`` tem buracos (ex.: 2014-2023). Anos sem
      habilitação ativa (curso só cadastrado a meio da janela, ex.: 45052 a
      partir de 2016, 45062 a partir de 2022) recebem a MÉDIA dos anos com
      dado não-zero — filosofia "cursos só mudam, raramente surgem novos".
      Zero para turmas cuja disciplina não pertença à grade do curso.

    - ``vagas_curso_<codcur>_faltam = max(0, vagas_curso_<codcur> - estmtr)``
      — estimativa de "alunos que ainda faltam se matricular"ptions no Dia D:
      o modelo enxerga o quanto o proxy ``estmtr`` (alunos já inscritos)
      deixa de completar a oferta do curso. Não vaza o alvo: vagas é cadastral
      e ``estmtr`` é a feature (proxy do Júpiter disponível no Dia D).

    Restrito aos ``cfg.top_cursos`` cursos ativos com mais turmas servidas no
    escopo (inativos/excluídos hoje não recebem colunas, mesmo que tenham tido
    vagas em 2010, conforme acordado com o usuário).
    """
    df = t.copy()
    hab = dados.get("habilit")
    grade = dados.get("grade")
    if hab is None or not len(hab) or grade is None or not len(grade):
        return df

    hb = hab.copy()
    hb["codcur"] = pd.to_numeric(hb["codcur"], errors="coerce")
    hb = hb.dropna(subset=["codcur"])
    hb["codcur"] = hb["codcur"].astype(int)
    for c in ("numvaghab", "numvaghabcpl", "numvaghabcvn"):
        hb[c] = pd.to_numeric(hb[c], errors="coerce").fillna(0)
    hb["vag_total"] = hb["numvaghab"] + hb["numvaghabcpl"] + hb["numvaghabcvn"]

    # Grade: (codcur, coddis) — qualquer tipobg (obrigatória + optativa).
    g = grade.copy()
    g["codcur"] = pd.to_numeric(g["codcur"], errors="coerce")
    g = g.dropna(subset=["codcur"])
    g["codcur"] = g["codcur"].astype(int)
    g = g.dropna(subset=["coddis"])
    g_pairs = g.drop_duplicates(["codcur", "coddis"])[["codcur", "coddis"]]
    discis_set = set(df["coddis"].unique())

    if cfg.cursos_vagas is not None:
        # Freeze: enumerar EXATAMENTE o catálogo, na ordem dada. Sem
        # depender de "ativos HOJE" — um curso desativado amanhã mantém sua
        # coluna (reconstruída de HABILITACAOGR por datas de vigência; zeros
        # nos anos após a desativação). Preserva ``feature_name_`` do .pkl.
        codcurs = list(cfg.cursos_vagas)
        if not codcurs:
            return df
        disc_por_cur: dict[int, set[str]] = {
            c: set(g_pairs.loc[g_pairs["codcur"] == c, "coddis"]) for c in codcurs
        }
    else:
        warnings.warn(
            "REPLICADO_CURSOS_VAGAS não definido: cursos de vagas_curso_* "
            "derivados dinamicamente (ativos hoje + top por contagem). Defina "
            "a variável no .env para um esquema de colunas estável entre "
            "extrações (ver .env.example / AGENTS.md).",
            stacklevel=2,
        )
        # Cursos ativos HOJE (≥1 habilitação vigente).
        hoje = pd.Timestamp.now().normalize()
        ativas_hoje = hb[
            (hb["dtaatvhab"].isna() | (hb["dtaatvhab"] <= hoje))
            & (hb["dtadtvhab"].isna() | (hb["dtadtvhab"] > hoje))
        ]
        codcurs_ativos = sorted(ativas_hoje["codcur"].astype(int).unique().tolist())
        if not codcurs_ativos:
            return df
        codcurs_grade = set(g_pairs["codcur"].unique())
        candidatos = [c for c in codcurs_ativos if c in codcurs_grade]
        if not candidatos:
            return df
        disc_por_cur = {
            c: set(g_pairs.loc[g_pairs["codcur"] == c, "coddis"]) for c in candidatos
        }
        contagem = {c: len(disc_por_cur[c] & discis_set) for c in candidatos}
        codcurs = sorted(contagem, key=lambda c: contagem[c], reverse=True)[
            : cfg.top_cursos
        ]
        # Reaproveita apenas os conjuntos de código de disciplinas dos escolhidos.
        disc_por_cur = {c: disc_por_cur[c] for c in codcurs}

    # Reconstrói vagas por (codcur, ano) — cache p/ reuso entre colunas do
    # mesmo curso. Vetoriza por mapa ano -> vagas.
    cache_vagas: dict[tuple[int, int], int] = {}
    anos_df = sorted(
        pd.to_numeric(df["ano"], errors="coerce").dropna().astype(int).unique()
    )
    for c in codcurs:
        for ano in anos_df:
            cache_vagas[(c, int(ano))] = _vagas_curso_no_ano(hb, c, int(ano))
        # Preenche anos sem dado cadastral (habilitação só cadastrada a meio
        # da janela, ex.: 45052 a partir de 2016, 45062 a partir de 2022) com
        # a MÉDIA dos anos com dado não-zero — filosofia "cursos só mudam,
        # raramente surgem novos". Preserva a variação temporal onde há sinal
        # (ex.: 45070: 128→143→113 permanece intacto).
        valores = [cache_vagas[(c, int(ano))] for ano in anos_df]
        nao_zero = [v for v in valores if v > 0]
        if nao_zero and 0 in valores:
            media = int(round(sum(nao_zero) / len(nao_zero)))
            for ano in anos_df:
                if cache_vagas[(c, int(ano))] == 0:
                    cache_vagas[(c, int(ano))] = media

    for c in codcurs:
        serve = df["coddis"].isin(disc_por_cur[c])
        vagas_map = {ano: cache_vagas[(c, int(ano))] for ano in anos_df}
        vagas_series = (
            pd.to_numeric(df["ano"], errors="coerce")
            .map(vagas_map)
            .fillna(0)
            .astype(int)
        )
        col_v = f"vagas_curso_{c}"
        col_f = f"vagas_curso_{c}_faltam"
        df[col_v] = np.where(serve, vagas_series, 0).astype(int)
        df[col_f] = np.maximum(0, df[col_v] - df["estmtr_val"].fillna(0)).astype(int)
    return df


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
    # Agregamos estmtr por semestre ANTES do shift para evitar ordem
    # indeterminada entre turmas do mesmo semestre de um mesmo docente: o
    # expanding deve correr sobre a série semestral (1 ponto por semestre),
    # não sobre uma lista arbitrária de turmas.
    mapa: dict[int, str] = {}
    cont: list[int] = [0]
    minis2 = minis.copy()
    minis2["id_prof"] = minis2["codpes_prof"].map(
        lambda v: anonimizar_codpes(v, mapa, cont)
    )
    turmas_past = df[["coddis", "codtur", "ano_sem", "estmtr_val"]]
    mp = minis2.merge(turmas_past, on=["coddis", "codtur"], how="inner")
    serie_sem = (
        mp.groupby(["id_prof", "ano_sem"], as_index=False)["estmtr_val"]
        .mean()
        .sort_values(["id_prof", "ano_sem"])
    )
    serie_sem["media_hist_prof"] = serie_sem.groupby("id_prof")["estmtr_val"].transform(
        lambda s: s.shift(1).rolling(window=3, min_periods=1).mean()
    )
    # Semestres consecutivos que o docente atual leciona a disciplina: mede a
    # fidelidade do docente-disciplina. DIFERENTE da regra de ouro da linhagem
    # da sala, aqui o agrupamento é por [coddis, id_prof] (não sufixo), pois
    # a relação avaliada é docente X disciplina. Ordenado por ano_sem, conta a
    # sequência vigente de semestres contíguos (sem gap) terminada no semestre
    # anterior (lag de 1), evitando vazamento do semestre corrente.
    semestres = mp[["id_prof", "coddis", "ano_sem"]].drop_duplicates()
    semestres = semestres.sort_values(["coddis", "id_prof", "ano_sem"])
    grp_dp = semestres.groupby(["coddis", "id_prof"], sort=False)
    semestres["ano_sem_prev"] = grp_dp["ano_sem"].shift(1)
    # Passo esperado em ano_sem USP: 1 (1S->2S mesmo ano) ou 9 (2S ano N ->
    # 1S ano N+1). Fora disso há gap e reiniciamos a contagem.
    delta_sem = semestres["ano_sem"] - semestres["ano_sem_prev"]
    semestres["_contiguo"] = delta_sem.isin([1, 9]) & semestres["ano_sem_prev"].notna()
    semestres["_run_grp"] = (~semestres["_contiguo"]).cumsum()
    semestres["semestres_consecutivos_prof"] = (
        semestres.groupby(["coddis", "id_prof", "_run_grp"], sort=False).cumcount() + 1
    )
    # Lag de 1 para não usar o próprio semestre da turma corrente (evita
    # vazamento de "este docente está aqui AGORA", sinal que correlaciona com
    # o alvo). O t-1 já reflete a fidelidade passada.
    semestres["semestres_consecutivos_prof"] = (
        grp_dp["semestres_consecutivos_prof"].shift(1).fillna(0).astype(int)
    )
    prof_fidel = semestres[
        ["coddis", "id_prof", "ano_sem", "semestres_consecutivos_prof"]
    ]
    # Repassa o contador de volta ao mp e agrega por turma.
    mp = mp.merge(prof_fidel, on=["coddis", "id_prof", "ano_sem"], how="left")
    fid_por_turma = mp.groupby(["coddis", "codtur", "ano_sem"], as_index=False)[
        "semestres_consecutivos_prof"
    ].max()
    forca = (
        serie_sem.merge(
            mp[["id_prof", "coddis", "codtur", "ano_sem"]],
            on=["id_prof", "ano_sem"],
            how="inner",
        )
        .groupby(["coddis", "codtur", "ano_sem"])["media_hist_prof"]
        .mean()
        .reset_index()
    )
    df = df.merge(forca, on=["coddis", "codtur", "ano_sem"], how="left")
    df = df.merge(fid_por_turma, on=["coddis", "codtur", "ano_sem"], how="left")
    df["semestres_consecutivos_prof"] = (
        df["semestres_consecutivos_prof"].fillna(0).astype(int)
    )
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


def features_sazonalidade(cfg: DatasetConfig, t: pd.DataFrame) -> pd.DataFrame:
    """Feature de eco anual (autocorrelação Lag 2): ``flag_fora_de_epoca``.

    O modelo sofre de autocorrelação no Lag 2 (eco anual): uma disciplina
    ofertada num semestre atípico para ela costuma ter ocupação
    deslocada do histórico. Esta feature diz ao modelo, **sem vazar o
    semestre corrente**, se a turma está sendo dada fora da "época
    típica" da disciplina.

    Lógica (blindada contra data leakage temporal):

    1. Agrega, por ``(coddis, ano_sem)``, a contagem de turmas em ``1S`` e
       ``2S`` — colapsando múltiplas turmas da mesma linhagem num mesmo
       semestre em UM ponto temporal, para que a barreira "strictly past"
       exclua o semestre inteiro e não apenas as linhas anteriores
       (ordenação arbitrária dentro do mesmo ``ano_sem``).
    2. Soma cumulativa (``cumsum``) de "1S"/"2S" por ``coddis`` ordenado por
       ``ano_sem``. Em seguida ``shift(1)`` DENTRO do grupo ``coddis`` exclui
       o próprio semestre da contagem: a "época típica" de uma turma em
       ``ano_sem`` S é calculada só sobre semestres ``< S``. Isso é a janela
       expansiva que respeita a barreira temporal — o semestre da própria
       linha NUNCA é usado para calcular a moda.
    3. ``sem_tipo_tipico`` = ``"1S"`` se ``cum_1S_past > cum_2S_past``,
       ``"2S"`` se ``cum_2S_past > cum_1S_past``, ``""`` em caso de empate
       (incluindo ``0×0``, i.e., primeira vez que a disciplina é dada).
    4. ``flag_fora_de_epoca = 1`` se ``sem_tipo`` da turma difere do
       ``sem_tipo_tipico``; ``0`` caso contrário (época típica OU primeira
       vez da disciplina OU ``sem_tipo`` ausente).

    Como usa só ``coddis``/``ano_sem``/``sem_tipo`` (derivados do ``codtur``
    via ``filtrar_turmas``), não toca em ``nummtr``/``delta`` — não vaza o
    alvo.
    """
    df = t.copy()
    if "sem_tipo" not in df.columns or "ano_sem" not in df.columns:
        df["flag_fora_de_epoca"] = 0
        return df

    # 1) Contagens por (coddis, ano_sem) — uma linha por semestre oferecido.
    #    ``observed=False`` inclui (coddis, ano_sem) sem turmas de um dos
    #    tipos; preenchemos depois com 0.
    cont = (
        df.groupby(["coddis", "ano_sem", "sem_tipo"], sort=False, observed=False)
        .size()
        .unstack("sem_tipo", fill_value=0)
        .reset_index()
    )
    for c in ("1S", "2S"):
        if c not in cont.columns:
            cont[c] = 0
        cont[c] = pd.to_numeric(cont[c], errors="coerce").fillna(0).astype(int)

    # 2) Cumsum inclusivo por coddis ordenado por ano_sem, depois shift(1)
    #    DENTRO do mesmo grupo para excluir o semestre corrente. A barreira
    #    "strictly past" passa a valer no nível do semestre inteiro.
    cont = cont.sort_values(["coddis", "ano_sem"], kind="mergesort").reset_index(
        drop=True
    )
    g = cont.groupby("coddis", sort=False)
    cont["_c1"] = g[("1S")].cumsum()
    cont["_c2"] = g[("2S")].cumsum()
    cont["cum_1S_past"] = g["_c1"].shift(1).fillna(0).astype(int)
    cont["cum_2S_past"] = g["_c2"].shift(1).fillna(0).astype(int)

    # 3) Época típica: argmax das contagens passadas; "" no empate/primeira vez.
    cont["sem_tipo_tipico"] = np.where(
        cont["cum_1S_past"] > cont["cum_2S_past"],
        "1S",
        np.where(cont["cum_2S_past"] > cont["cum_1S_past"], "2S", ""),
    )
    tipico = cont[["coddis", "ano_sem", "sem_tipo_tipico"]]

    # 4) Flag por turma: sem_tipo atual != época típica (e ambos definidos).
    df = df.merge(tipico, on=["coddis", "ano_sem"], how="left")
    df["sem_tipo_tipico"] = df["sem_tipo_tipico"].fillna("")
    df["flag_fora_de_epoca"] = (
        df["sem_tipo"].notna()
        & (df["sem_tipo_tipico"] != "")
        & (df["sem_tipo"] != df["sem_tipo_tipico"])
    ).astype(int)
    df.drop(columns=["sem_tipo_tipico"], inplace=True, errors="ignore")
    return df


# ---------------------------------------------------------------------------
# Features avançadas (Módulos 2-4: espaço de fase, rede, concorrência, sincronia)
# ---------------------------------------------------------------------------
def features_espaco_fase(cfg: DatasetConfig, t: pd.DataFrame) -> pd.DataFrame:
    """Sinais de fase (velocidade/volatilidade) da linhagem da sala.

    Séries cronológicas de lotação ordenadas por ``ano_sem`` e agrupadas por
    ``['coddis', 'sufixo']`` (regra de ouro da linhagem). O ``.shift(1)``
    aplicado ANTES do ``.rolling()`` exclui o semestre corrente do cascalho
    histórico, garantindo imunidade a vazamento do alvo ``delta``.

    - ``var_pct_estmtr`` — ``Δestmtr / estmtr_passado`` (tendência do proxy
      entre semestres consecutivos lagged). Não é razão ``nummtr/estmtr``
      (que degeneraria em 1): numerador e denominador vêm de estágios
      temporais distintos da mesma série.
    - ``d_estmtr_dt_t1`` — diferença (variação absoluta) do ``estmtr`` entre
      os dois semestres anteriores (velocidade do proxy, sem normalização).
    - ``volatilidade_estmtr`` — desvio-padrão *deslizante* (janela 3,
      ``min_periods=1``) do ``estmtr`` passado: quão estável é o proxy da
      linhagem no curto prazo (substitui o ``expanding().std()`` que
      carregava peso muito antigo e diluía a tendência recente).
    """
    if "estmtr_val" not in t.columns:
        return t.copy()
    df = t.sort_values(["coddis", "sufixo", "ano_sem"]).reset_index(drop=True)
    # Agrupamento por [coddis, sufixo] ANTES do .shift(): linhagem da sala.
    g = df.groupby(["coddis", "sufixo"], sort=False)
    prev_est = g["estmtr_val"].shift(1)  # exclui o semestre corrente
    # Variação percentual: Δ / proxy_passado (denominador ≠ 0). Não degenera.
    df["var_pct_estmtr"] = (
        (df["estmtr_val"] - prev_est) / prev_est.replace(0, np.nan)
    ).fillna(0)
    # Velocidade absoluta lagged — puro estmtr.
    df["d_estmtr_dt_t1"] = prev_est.diff().fillna(0)
    # Volatilidade deslizante (window=3) do proxy passado: substitui o
    # .expanding().std() que diluía a tendência recente.
    df["volatilidade_estmtr"] = (
        g["estmtr_val"]
        .transform(lambda s: s.shift(1).rolling(window=3, min_periods=1).std())
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
    # diretos no SEU ÚLTIMO oferecimento estritamente anterior a sem_alvo,
    # normalizada pelas vagas da própria turma(+1). Usa apenas passado.
    #
    # Importante: NÃO calcular ``sem_prev`` como o semestre calendário
    # anterior (1S→2S ano-1, 2S→1S mesmo ano). Muitas disciplinas USP têm
    # oferecimento anual (só em semestres pares ou ímpares): se o
    # pré-requisito só abre no 1S e a turma-alvo é 1S do ano seguinte, o
    # cálculo algébrico olharia o 2S vazio e atribuiria pressão=0. Em vez
    # disso, varremos a série histórica real de oferecimento de cada
    # pré-requisito e casamos por ``merge_asof`` (``allow_exact_matches=
    # False`` → o maior ``ano_sem`` oferecido estritamente menor que o
    # ``sem_alvo``): equivalente a um ``groupby().shift()`` que respeita os
    # gaps anuais da disciplina.
    hg = _hist_com_ano_sem(hist) if len(hist) else hist
    rep_prev: dict[tuple[str, int], float] = {}
    if len(hg) and preds_map:
        # Série de oferecimento real: um (coddis, ano_sem) por vez que a
        # disciplina de fato teve turma/inscrição registrada no HISTESCOLARGR.
        ofertado = (
            hg[["coddis", "ano_sem"]]
            .dropna(subset=["ano_sem"])
            .drop_duplicates()
            .astype({"ano_sem": int})
            .query("ano_sem >= @cfg.ano_min * 10")
            .sort_values("ano_sem", kind="mergesort")
            .reset_index(drop=True)
        )
        # Reprovações por (coddis, ano_sem). Semestre oferecido sem reprovados
        # → n_rep = 0 (a turma existiu, sob pressão zero — informação válida).
        rep_df = (
            hg[hg["rstfim"].isin(RSTFM_REPROVACAO)]
            .groupby(["coddis", "ano_sem"], sort=False)
            .size()
            .rename("n_rep")
            .reset_index()
            .astype({"ano_sem": int})
        )
        ofertado = ofertado.merge(rep_df, on=["coddis", "ano_sem"], how="left")
        ofertado["n_rep"] = ofertado["n_rep"].fillna(0).astype(int)

        # Lista de (disc_alvo, pré-requisito p, sem_alvo) a avaliar.
        alvos = df[["coddis", "ano_sem"]].drop_duplicates().copy()
        alvos["preds"] = alvos["coddis"].map(preds_map)
        alvos = alvos.explode("preds", ignore_index=True).dropna(subset=["preds"])
        alvos = alvos.rename(columns={"coddis": "disc_alvo", "preds": "coddis"})
        alvos["ano_sem"] = alvos["ano_sem"].astype(int)
        alvos = alvos.sort_values("ano_sem", kind="mergesort")

        if len(alvos):
            # Por pré-requisito ``p`` (group by=coddis), casa ``sem_alvo`` ao
            # maior ``ano_sem`` oferecido estritamente menor: direction
            # "backward" com allow_exact_matches=False reproduz "shift()
            # sobre a própria série", sem assumir contiguidade de calendário.
            #
            # Blindagem anti-vazamento temporal (3 pilares):
            # 1. Ambas as tabelas estão pré-ordenadas por ``ano_sem``
            #    (``ofertado`` linha do ``.sort_values("ano_sem")``,
            #    ``alvos`` linha do ``.sort_values("ano_sem")``) — requisito
            #    documentado do ``merge_asof``; sem isso o casamento é
            #    silenciosamente incorreto.
            # 2. ``allow_exact_matches=False`` garante que o oferecimento
            #    casado seja ESTRITAMENTE anterior a ``sem_alvo``: mesmo que o
            #    pré-requisito seja ofertado no MESMO ``ano_sem`` da turma-alvo,
            #    ele é pulado e buscamos o oferecimento realmente passado. Isso
            #    fecha a rota de vazamento em que a reprovação do próprio
            #    semestre-alvo seria usada como feature.
            #    Disciplinas anuais (oferecidas só em 1S ou só em 2S) são
            #    tratadas sem artimanhas: o ``backward`` salta os gaps vazios e
            #    ancora no último oferecimento REAL, por mais antigo que seja.
            # 3. ``direction="backward"`` +  ``allow_exact_matches=False``
            #    significa que, se NÃO existe oferecimento prévio do pré-
            #    requisito (primeira vez que ele é dado, ou só aparece no
            #    mesmo/depósito do sem_alvo), o merge produz NaN em ``n_rep``.
            #    O ``fillna(0)`` logo abaixo converte esses casos em pressão
            #    ZERO — informação honesta ("sem represamento conhecido") e
            #    não em NaN que quebraria o modelo. Os semestres que TIVERAM
            #    oferecimento porém com ZERO reprovados já chegaram como 0
            #    via o ``ofertado["n_rep"].fillna(0)`` anterior, e seguem 0
            #    aqui — distinguindo "oferecido sem reprovados" de "nunca
            #    oferecido antes", ambos sem pressão.
            press = pd.merge_asof(
                alvos,
                ofertado,
                on="ano_sem",
                by="coddis",
                direction="backward",
                allow_exact_matches=False,
            )
            press["n_rep"] = press["n_rep"].fillna(0).astype(int)
            # Soma reprovações de todos os pré-requisitos diretos no seu
            # último oferecimento antes de sem_alvo, por (disc_alvo, sem_alvo).
            soma = press.groupby(["disc_alvo", "ano_sem"], sort=False)["n_rep"].sum()
            rep_prev = {(d, int(s)): float(v) for (d, s), v in soma.items()}

    # Normaliza pelas vagas da própria turma-alvo (mínimo 1 para evitar div 0).
    num = pd.Series(
        [rep_prev.get((c, s), 0.0) for c, s in zip(df["coddis"], df["ano_sem"], strict=True)],
        index=df.index,
        dtype=float,
    )
    df["pressao_represada"] = num / df["vagas_reais"].clip(lower=1).astype(float)
    return df


def _montar_grafo_requisitos(grade: pd.DataFrame):
    """Constrói um DiGraph aproximado de pré-requisitos.

    A GRADECURRICULAR não traz as arestas diretas; como fallback aproximado,
    criamos arestas apenas entre disciplinas de semestres **consecutivos**
    (diferença de ``numsemidl`` <= 2) dentro de cada (codcur, codhab), e não
    mais entre quaisquer i<j. Ligar o 1º ao 8º semestre produzia um grafo
    quase completo que achatava ``net_betweenness`` (tudo homogêneo) e
    inflava ``pressao_represada``. Se ``GRUPOREQUISITO``/``REQUISITOGR``
    estiverem disponíveis no futuro, devem substituir este fallback.
    """
    if nx is None or grade is None or not len(grade):
        return None
    g = nx.DiGraph()
    grade = grade.dropna(subset=["coddis", "numsemidl"]).copy()
    grade["numsemidl"] = pd.to_numeric(grade["numsemidl"], errors="coerce")
    for (_codcur, _codhab), grp in grade.groupby(["codcur", "codhab"]):
        sems = (
            grp.drop_duplicates("coddis")[["coddis", "numsemidl"]]
            .dropna()
            .sort_values("numsemidl")
        )
        discs = sems["coddis"].tolist()
        sems_num = sems["numsemidl"].tolist()
        for i in range(len(discs)):
            for j in range(i + 1, len(discs)):
                # Apenas semestres próximos (consecutivos ou gap <= 2).
                if sems_num[j] - sems_num[i] <= 2:
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
    *,
    atualizar_anos: Iterable[int] = (),
) -> pd.DataFrame:
    """Constrói o DataFrame mestre (features + alvo) por turma.

    Sai em ``cfg.saida`` (CSV). Retorna o DataFrame.

    Refresh incremental do cache (ver :func:`carregar_dados`):

    - ``forcar_extracao`` mantém o comportamento atual: re-extraí TURMAGR e
      todas as tabelas auxiliares com ``SELECT {COLS}`` (rápido). Quando
      ``atualizar_anos`` é vazio, também re-extraí toda a HISTESCOLARGR (bug
      fix — ``forcar`` agora honra o nome no histórico).
    - ``atualizar_anos`` (lista explícita de anos) re-extraí do banco SÓ os
      pickles ``histescolar_<ano>.pkl`` desses anos, sobrescrevendo o cache;
      os anos fora da lista vêm do cache existente. Neste modo ``forcar`` é
      ignorado para a HISTESCOLARGR — refresh cirúrgico, ideal para o
      retreino noturno onde só os 2 últimos anos letivos mudam de fato.

    Default ``atualizar_anos=()`` é no-op e preserva callers existentes.

    Pipeline de retreino da API Skuld em uma chamada::

        ano = datetime.now().year
        df = montar_dataset(
            DatasetConfig.from_env(...),
            forcar_extracao=True,            # refaz TURMAGR + auxiliares
            atualizar_anos=[ano - 1, ano],   # refaz só os 2 anos quentes de HIST
        )
    """
    cfg = cfg or DatasetConfig.from_env()
    print(
        f"=== DatasetConfig ===\n  codundclg: {cfg.codundclg}\n  prefixos: {cfg.prefixos}\n"
        f"  anos: {cfg.ano_min}-{cfg.ano_max}\n  sufixo_min: {cfg.sufixo_min}\n"
        f"  saida: {cfg.saida}"
    )

    dados = carregar_dados(
        cfg, forcar=forcar_extracao, atualizar_anos=atualizar_anos
    )
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
    df = features_vagas_curso(cfg, df, dados)
    df = features_professor_horario(cfg, df, dados)
    df = features_ingressantes(cfg, df, dados["grade"])
    # Sazonalidade / eco anual (flag_fora_de_epoca) — independe de estmtr,
    # só precisa de coddis/ano_sem/sem_tipo (presentes desde filtrar_turmas).
    df = features_sazonalidade(cfg, df)

    # Avançadas (precisam de estmtr_val ainda presente como auxiliar)
    df = features_espaco_fase(cfg, df)
    df = features_rede_requisitos(cfg, df, dados["grade"], dados["hist"])
    df = features_concorrencia_horaria(cfg, df, dados)

    # Macro-sensores (sinais globais de crise/caos por semestre).
    from .dataset_macrosensores import features_macrosensores

    df = features_macrosensores(cfg, df, dados)

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
        "--codundclg",
        type=int,
        default=None,
        help="Colegiado/unidade (obrigatório se REPLICADO_CODUNDCLG ausente no .env)",
    )
    p.add_argument("--prefixos", nargs="+", default=None, help="Prefixos de disciplina")
    p.add_argument("--sufixo-min", type=int, default=None)
    p.add_argument("--ano-min", type=int, default=None)
    p.add_argument("--ano-max", type=int, default=None)
    p.add_argument(
        "--forcar-extracao", action="store_true", help="Reextrai tabelas auxiliares"
    )
    p.add_argument(
        "--atualizar-anos",
        type=int,
        nargs="+",
        default=[],
        help=(
            "Anos da HISTESCOLARGR a re-extraí do banco (refresh cirúrgico; "
            "ignora --forcar-extracao para o histórico). Default: nenhum."
        ),
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
    montar_dataset(
        cfg,
        forcar_extracao=args.forcar_extracao,
        atualizar_anos=args.atualizar_anos,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
