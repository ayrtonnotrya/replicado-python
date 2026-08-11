"""
Fornecedor de dataset para o modelo de **Micro Targeting Probabilístico
(Aluno × Turma)** do Replicado.

Enquanto :mod:`replicado.dataset_alocacao` produz um dataset linha-por-turma
com o alvo ``delta`` (correção do ``estmtr``), este módulo desce para a
granularidade **(aluno, turma)**: o alvo é binário — ``alvo_matriculado``
(1 se o aluno $i$ de fato consolidou matrícula na turma $j$ no semestre $s$,
0 caso contrário).

Pipeline (sob cache local; bate no banco apenas para tabelas auxiliares
ainda não cacheadas):

1.  :func:`carregar_dados_aluno` — herda o cache de ``dataset_alocacao``
    (TURMAGR, auxiliares, macro-sensores) e adiciona **CURRICULOGR**,
    **REQUISITOGR** e uma HISTESCOLARGR **com notas** (cache isolado para
    não invalidar o pipeline de alocação).
2.  :func:`_montar_features_turma` — reaproveita o ferramental de
    ``dataset_alocacao`` (``features_*``) para consolidar as features **da
    turma** (Módulos E e F), por ``(coddis, codtur, ano_sem)``.
3.  :func:`montar_dataset_aluno` — laço por ``ano_sem`` alvo que:

    a. Determina alunos **ativos** no semestre (HABILPROGGR com ingresso
       anterior ao Dia D e sem conclusão prévia).
    b. Levanta **currículos elegíveis** por aluno (Heurística de União: um
       aluno pode se formar por qualquer ``codcrl`` ativo entre o seu
       ingresso e o semestre corrente — ``CURRICULOGR.dtafimcrl`` nula ou
       posterior ao ingresso, ``dtainicrl`` ≤ Dia D).
    c. Constrói a **matriz base** (positivos + negativos plausíveis) com
       ``Alunoco.SuppressLint de Grade`` (horizonte temporal) e o filtro
       obrigatório de exclusão de disciplinas já aprovadas.
    d. Vetoriza os Módulos A–F de features.
    e. Anonimiza ``codpes`` → ``id_aluno`` (LGPD), descarta colunas de
       vazamento e grava o CSV.

Regra de Ouro (Point-in-Time / Dia D)
-------------------------------------
Todo o histórico é calculado APENAS sobre registros da HISTESCOLARGR cuja
``dtacrihst`` (e ``dtaultalt`` para conclusões) sejam **estritamente
anteriores** a ``dtainitur - cfg.dias_corte`` — a mesma ``dta_corte`` de
:mod:`dataset_alocacao`. Negativos triviais e ``nummtr``/``estmtr``
consolidados nunca viram feature.

Uso
---
    poetry run python scripts/build_dataset_aluno.py
    poetry run python scripts/build_dataset_aluno.py --codundclg 45 --prefixos MAC MAT
    poetry run python scripts/build_dataset_aluno.py --forcar-extracao --saida temp/dataset_aluno.csv
"""

from __future__ import annotations

import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from tqdm import tqdm

from .connection import DB
from .dataset_alocacao import (
    DatasetConfig,
    carregar_dados,
    features_base,
    features_concorrencia_horaria,
    features_demanda,
    features_espaco_fase,
    features_historico,
    features_ingressantes,
    features_professor_horario,
    features_rede_requisitos,
    features_sazonalidade,
    filtrar_turmas,
    reconstruir_estmtr,
)

CACHE_DIR = Path("temp/cache_maquina_tempo")
SAIDA_DEFAULT = Path("temp/dataset_aluno.csv")

# Resultados finais que contam como aprovação (não cursará novamente a coddis).
RSTFM_APROVACAO: tuple[str, ...] = ("A", "AR", "D")
# Reprovações que geram "dívida" curricular (reprovação por nota/frequência).
RSTFM_REPROVACAO: tuple[str, ...] = ("RN", "RF", "RA")
# Status de matrícula que indicam matrícula consolidada (vs excluído/removido).
STAMTR_MATRICULADO: tuple[str, ...] = ("M",)

# Colunas da HISTESCOLARGR usadas pelo modelo de aluno — estende ``COLS_HIST``
# (cache.py) com as **notas** (necessárias para a média ponderada do Módulo C).
# Cacheadas em arquivo próprio (``histescolar_aluno_<ano>.pkl``) para isolar do
# pipeline de alocação e evitar re-extração das notas naquele fluxo.
COLS_HIST_ALUNO = """
    codpes, codpgm, coddis, verdis, codtur, dtacrihst, stamtr, dtaultalt,
    rstfim, notfim, notfim2, discrl, aplori
"""


# ---------------------------------------------------------------------------
# Status de programa considerados **mortos** (evasão) no snapshot point-in-time
# da HISTPROGGR (último ``stapgm`` por ``(codpes, codpgm)`` com ``dtaoco <=
# dta_corte``). Lógica por **exclusão** — não por inclusão — porque a
# HISTPROGGR é um log de eventos: o último registro de um veterano quase
# sempre é ``H`` (Histórico/Habilitação) ou ``EH`` (encerramento de
# habilitação anterior ao trocar de ênfase), e o programa **continua ativo**
# nesses casos. ``A`` (Ativo), ``R`` (Reserva/Reingresso ativo) também vivos.
# Apenas ``E`` (Encerrado), ``T`` (Trancado) e ``S`` (Suspenso) selam a morte
# do programa no Dia D. Precedente: ``replicado.graduacao`` consulta
# ``stapgm IN ('A','H','R')`` para "programas vivos" (graduacao.py:967).
# ---------------------------------------------------------------------------
MORTO_STAPGM: frozenset[str] = frozenset({"E", "T", "S"})


# ---------------------------------------------------------------------------
# Configuração agnóstica à unidade (estende DatasetConfig)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DatasetAlunoConfig(DatasetConfig):
    """Parâmetros adicionais do modelo aluno × turma.

    Herda todos os campos de :class:`DatasetConfig` (codundclg, prefixos,
    sufixo_min, dias_corte, etc.) e adiciona os knobs específicos do
    Micro Targeting.
    """

    # Relaxamento de Grade: o aluno só "enxerga" disciplinas cujo semestre
    # ideal seja ≤ (semestre de casa do aluno + horizonte_sem). 2 = padrão
    # da regra de negócio descrita (aluno de 2º sem não vê matéria de 8º).
    horizonte_sem: int = 2
    # Janela (dias) após o Dia D para considerar um requerimento de matrícula
    # como "intenção ativa" salva pelo aluno (Módulo de negativos).
    janela_intencao_dias: int = 45
    # Ativa a inclusão de candidatos via REQUERIMENTOGR (intenção de matrícula).
    usar_intencao_requerimento: bool = True
    # Limite (por aluno/semestre) de candidatos negativos por disciplina, para
    # conter a explosão combinatória quando há muitas turmas da mesma coddis.
    max_neg_turmas_por_disc: int = 6
    # Excluir "alunos fantasmas" — ativos no Dia D com ZERO matrículas IME no
    # semestre alvo — da geração de negativos (y=0). Default False: preserva o
    # comportamento baseline. Flag de PESQUISA para análise de impacto em
    # inferência; ``selection-on-outcome`` intencional (ver AGENTS.md), NÃO
    # usar o dataset resultante como se fosse "limpo" em produção. Sufixo do
    # arquivo de saída: ``_sf``.
    excluir_fantasmas: bool = False
    # Balancear a classe L (livres): amostra negativos L entre alunos com
    # ≥1 matrícula no semestre (qualquer tipo) até ``pos_l/neg_L`` atingir a
    # razão média de O e E/C (média das razões pos/neg) calculada_por semestre
    # sobre os próprios matriculados. Default False: preserva baseline.
    # ``max_neg_turmas_por_disc`` NÃO se aplica a L (volume controlado pelo
    # alvo de razão). Sufixo do arquivo de saída: ``_bl``.
    balancear_l: bool = False
    saida: Path = SAIDA_DEFAULT

    @classmethod
    def from_env(cls, **overrides: Any) -> DatasetAlunoConfig:
        import os

        def env_int(name: str) -> int | None:
            v = os.getenv(name)
            return int(v) if v and v.strip() else None

        def env_bool(name: str) -> bool | None:
            v = os.getenv(name)
            if not v or not v.strip():
                return None
            return v.strip().lower() in {"1", "true", "s", "sim", "yes", "y"}

        for name, field in (
            ("REPLICADO_ALUNO_HORIZONTE", "horizonte_sem"),
            ("REPLICADO_ALUNO_JANELA_INTENCAO", "janela_intencao_dias"),
        ):
            v = env_int(name)
            if v is not None:
                overrides.setdefault(field, v)
        ui = env_bool("REPLICADO_ALUNO_USAR_INTENCAO")
        if ui is not None:
            overrides.setdefault("usar_intencao_requerimento", ui)
        v = env_int("REPLICADO_ALUNO_MAX_NEG_DISC")
        if v is not None:
            overrides.setdefault("max_neg_turmas_por_disc", v)
        # Flags exploratórias: defaults False preservam o baseline.
        # Bug-fix: o padrão antigo ``env_bool(os.getenv("X") or "")`` passava o
        # VALOR como ``name`` (os.getenv("1")) e retornava sempre None — var.
        ef = env_bool("REPLICADO_ALUNO_EXCLUIR_FANTASMAS")
        if ef is not None:
            overrides.setdefault("excluir_fantasmas", ef)
        bl = env_bool("REPLICADO_ALUNO_BALANCEAR_L")
        if bl is not None:
            overrides.setdefault("balancear_l", bl)
        # ``saida`` default já é dataset_aluno.csv; permite override via env.
        s = os.getenv("REPLICADO_ALUNO_SAIDA")
        if s and s.strip():
            overrides.setdefault("saida", Path(s))
        return super().from_env(**overrides)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Carga de dados (herda + extras)
# ---------------------------------------------------------------------------
def _extrair_fatia_hist_aluno(ano: int, cache: Path) -> pd.DataFrame:
    """Extrai UMA fatia anual da HISTESCOLARGR **com notas** (cache isolado).

    Sempre bate no banco (espelho de :func:`replicado.cache.extrair_fatia_histescolar`,
    mas com ``COLS_HIST_ALUNO``).
    """
    caminho = cache / f"histescolar_aluno_{ano}.pkl"
    rows = DB.fetch_all(
        f"SELECT {COLS_HIST_ALUNO} FROM HISTESCOLARGR WHERE codtur LIKE '{ano}%'"
    )
    df = pd.DataFrame(rows)
    if len(df):
        df["coddis"] = df["coddis"].astype(str).str.strip().str.upper()
        df["codtur"] = df["codtur"].astype(str).str.strip()
        df["dtacrihst"] = pd.to_datetime(df["dtacrihst"], errors="coerce")
        df["dtaultalt"] = pd.to_datetime(df["dtaultalt"], errors="coerce")
    df.to_pickle(caminho)
    return df


def _carregar_hist_aluno(
    cfg: DatasetAlunoConfig, forcar: bool = False
) -> pd.DataFrame:
    """Carrega/concatena as fatias anuais de HISTESCOLARGR com notas."""
    cache = cfg.cache_dir
    parts: list[pd.DataFrame] = []
    for ano in tqdm(list(cfg.anos), desc="HIST-aluno", unit="ano"):
        c = cache / f"histescolar_aluno_{ano}.pkl"
        if c.exists() and not forcar:
            parts.append(pd.read_pickle(c))
        else:
            parts.append(_extrair_fatia_hist_aluno(ano, cache))
    h = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if len(h) and "ano_sem" not in h.columns:
        h["ano_sem"] = pd.to_numeric(h["codtur"].str[:5], errors="coerce")
    return h


def _cargar_aux(
    cfg: DatasetAlunoConfig, dados: dict[str, pd.DataFrame], forcar: bool
) -> dict[str, pd.DataFrame]:
    """Extrai/cacheia CURRICULOGR e REQUISITOGR (filtro por cursos da unidade)."""
    cod = cfg.codundclg
    cache = cfg.cache_dir

    extras = [
        (
            "curriculo",
            (
                "SELECT C.codcrl, C.codcur, C.codhab, C.dtainicrl, C.dtafimcrl, "
                "C.cgahortot, C.sitcrl FROM CURRICULOGR C "
                f"WHERE C.codcur IN (SELECT codcur FROM CURSOGR WHERE codclg = {cod})"
            ),
        ),
        (
            "requisito",
            (
                "SELECT R.coddis, R.codcur, R.codhab, R.coddisreq, R.tipreq "
                "FROM REQUISITOGR R "
                f"WHERE R.codcur IN (SELECT codcur FROM CURSOGR WHERE codclg = {cod}) "
                "AND R.tipreq = 'PR'"
            ),
        ),
    ]
    if cfg.usar_intencao_requerimento:
        extras.append(
            (
                "requer",
                (
                    "SELECT RH.codpes, RH.coddis, RH.codtur, R.codpgm, "
                    "R.tiprqm, R.dtacadrqm FROM REQUERHISTESC RH "
                    "INNER JOIN REQUERIMENTOGR R ON RH.codrqm = R.codrqm "
                    "INNER JOIN HABILPROGGR HP ON R.codpes = HP.codpes "
                    "AND R.codpgm = HP.codpgm "
                    f"INNER JOIN CURSOGR CS ON HP.codcur = CS.codcur "
                    f"WHERE CS.codclg = {cod}"
                ),
            )
        )

    for chave, query in tqdm(extras, desc="Carga aluno-aux", unit="tab"):
        c = cache / f"aux_{chave}_{cod}.pkl"
        if c.exists() and not forcar:
            dados[chave] = pd.read_pickle(c)
        else:
            dados[chave] = DB.fetch_all(query)
            dados[chave] = pd.DataFrame(dados[chave])
            dados[chave].to_pickle(c)
        df = dados[chave].copy()
        for col in ("coddis", "coddisreq", "codtur"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                if col != "coddisreq":
                    df[col] = df[col].str.upper()
        for col in ("dtainicrl", "dtafimcrl", "dtacadrqm"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        dados[chave] = df
    return dados


def carregar_dados_aluno(
    cfg: DatasetAlunoConfig, forcar: bool = False
) -> dict[str, pd.DataFrame]:
    """Reaproveita :func:`carregar_dados` (base + macro-sensores) e adiciona
    CURRICULOGR, REQUISITOGR e HISTESCOLARGR com notas."""
    dados = carregar_dados(cfg, forcar=forcar)
    dados = _cargar_aux(cfg, dados, forcar)
    dados["hist_aluno"] = _carregar_hist_aluno(cfg, forcar=forcar)
    # ``perhab`` por (codcur, codhab) — usado no Módulo D (conflito de turno).
    if "habilit" in dados and len(dados["habilit"]):
        hab = dados["habilit"]
        if "perhab" in hab.columns:
            dados["_perhab"] = hab[["codcur", "codhab", "perhab"]].drop_duplicates(
                ["codcur", "codhab"]
            )
        else:
            dados["_perhab"] = pd.DataFrame(columns=["codcur", "codhab", "perhab"])
    else:
        dados["_perhab"] = pd.DataFrame(columns=["codcur", "codhab", "perhab"])
    return dados


# ---------------------------------------------------------------------------
# Helpers temporais
# ---------------------------------------------------------------------------
def _sem_anterior_letivo(ano_sem: int) -> int:
    """``ano_sem`` (ano*10+sem, sem∈{1,2}) do semestre letivo imediatamente
    anterior (t-1): 1S ano N ← 2S ano N-1; 2S ano N ← 1S ano N."""
    ano, sem = ano_sem // 10, ano_sem % 10
    return (ano - 1) * 10 + 2 if sem == 1 else ano * 10 + 1


def _sem_casa(dtaing: pd.Timestamp, ano_alvo: int, sem_alvo: int) -> int:
    """Semestre de casa do aluno no semestre alvo (1-indexado)."""
    if pd.isna(dtaing):
        return 0
    ano_ing = dtaing.year
    sem_ing = 2 if dtaing.month > 6 else 1
    casa = (ano_alvo - ano_ing) * 2 + (sem_alvo - sem_ing) + 1
    return int(max(casa, 1))


# ---------------------------------------------------------------------------
# Bloco 1: alunos ativos + currículos elegíveis + necessidade curricular
# ---------------------------------------------------------------------------
def _alunos_ativos(
    cfg: DatasetAlunoConfig, dados: dict[str, pd.DataFrame], dta_corte: pd.Timestamp
) -> pd.DataFrame:
    """Alunos da unidade **ativos no Dia D** do semestre alvo (point-in-time).

    Define "ativo no Dia D" via **verdade temporal** extraída da HISTPROGGR —
    snapshot vetorial do último evento ``dtaoco <= dta_corte`` por
    ``(codpes, codpgm)``. Lógica de **exclusão** (não inclusão): o aluno é
    ativo no Dia D se o último ``stapgm`` **não for** um status morto em
    ``MORTO_STAPGM`` (`{'E','T','S'}`). A HISTPROGGR é um log de eventos —
    ``H`` (Histórico/Habilitação) e ``EH`` (encerramento de habilitação
    anterior ao trocar de ênfase) NÃO matam o programa, e o último registro
    da imensa maioria dos veteranos é ``H``; exigir ``∈ {A,R}`` (versão
    anterior) deletava os veteranos reais da base. Alunos sem nenhuma
    HISTPROGGR ≤ Dia D ficam no time (não há evidência de evasão).

    Camada de piso (hard backstop): ``dtaclcgru`` (colação) ≥ Dia D — quem já
    colou-grau antes do Dia D jamais pode estar matriculado naquele semestre,
    independente da HISTPROGGR.

    Bug histórico corrigido aqui: a condicional anterior
    ``(dtaclcgru isna | dtaclcgru > dta_corte)`` IGNORAVA evasão
    (jubilamento/desistência/encerramento sem colação), retendo ~15 anos de
    evadidos da **1ª graduação** como ativos — origem do sintoma "~85% de
    alunos com 0 matrículas reais". Importante: ``codpgm`` na PROGRAMAGR é o
    número de (re)ingresso da pessoa na USP (não tipo de programa) — alunos
    reingressantes ``codpgm >= 2`` são ativos legítimos e NÃO devem ser
    filtrados. O cache de ``habilprog`` já é restrito à graduação da unidade
    via ``INNER JOIN CURSOGR WHERE codclg = {cod}``. O cache de
    ``histprog_unidade`` deve incluir TODOS os ``codpgm`` (a query de
    extração NÃO filtra ``codpgm = 1``) — senão a evidência PIT dos
    reingressos não existe e eles caem no fallback.

    Regra de Ouro (data leakage): calouros FUVEST/SISU que ingressam APÓS o
    Dia D (~D+0, por carga) não entram — ``dtaing > dta_corte`` já os exclui.
    Incluí-los seria usar a data de ingresso futura para montar o universo.
    Eles constam do alvo T_pico (outcome realizado), mas o modelo não pode
    prever para quem ainda não existe no sistema no momento de raspagem.
    """
    hp = dados.get("habilprog")
    if hp is None or not len(hp):
        return pd.DataFrame(columns=["codpes", "codcur", "codhab", "dtaing"])
    a = hp.copy()
    a = a[a["codpes"].notna()]
    a["codpes"] = a["codpes"].astype(int)
    # Piso (hard backstop): filtros puramente "na HABILPROGGR".
    cand = a[
        (a["dtaing"] < dta_corte)
        & (a["dtaclcgru"].isna() | (a["dtaclcgru"] > dta_corte))
    ].copy()

    # NOTA: NÃO filtrar por ``codpgm == 1``. ``codpgm`` na PROGRAMAGR é o
    # número de (re)ingresso da pessoa na USP (1=1º ingresso, 2=reingresso,
    # ...), não o tipo de programa: alunos reingressantes (segunda graduação,
    # retorno após abandono) têm ``codpgm >= 2`` e são LEGITIMAMENTE ativos no
    # Dia D — empiricamente 100% das linhagens com ``codpgm >= 2`` no cache
    # têm último ``stapgm`` PIT ∈ {A,R} (diagnóstico 2018.1). Descartá-los
    # jogaria fora exatamente o elenco ativo de reentradas. O CACHE já está
    # restrito a graduação da unidade via ``INNER JOIN CURSOGR WHERE codclg =
    # {cod}`` (CURSOGR = cursos de graduação; pós-graduação vive em outra
    # família de tabelas, não aqui).

    if not len(cand):
        return pd.DataFrame(columns=["codpes", "codcur", "codhab", "dtaing"])

    # ---- Verda temporal point-in-time (HISTPROGGR) -----------------------
    # Snapshot por (codpes, codpgm): último stapgm com dtaoco <= Dia D.
    # Algoritmo espelhado em _macro_trancamento: pré-ordenação por dtaoco +
    # groupby.tail(1) obtêm "último evento de cada par" sem ler eventos
    # futuros (Regra de Ouro garantida por dtaoco <= dta_corte).
    hp_hist = dados.get("histprog_unidade")
    if hp_hist is not None and len(hp_hist):
        h = hp_hist.dropna(subset=["dtaoco"]).copy()
        if "stapgm" in h.columns:
            h["stapgm"] = h["stapgm"].astype(str).str.strip()
        h = h[h["dtaoco"] <= dta_corte]
        if len(h) and "stapgm" in h.columns:
            h = h.sort_values("dtaoco")
            # tail(1) dentro de cada (codpes, codpgm): último stapgm PIT.
            ult = (
                h.groupby(["codpes", "codpgm"], sort=False)
                .tail(1)[["codpes", "codpgm", "stapgm"]]
                .rename(columns={"stapgm": "_stapgm_pit"})
            )
            cand = cand.merge(ult, on=["codpes", "codpgm"], how="left")
            # Conserva quem NÃO tem HISTPROGGR <= Dia D (cobertura parcial da
            # HISTPROGGR no cache ≈ 45% dos codpes); os demais seguem o piso
            # dtaclcgru como único backstop. Quem TEM evidência só sai se o
            # último status for morto (E/T/S). Lógica por EXCLUSÃO: ``H``/``EH``
            # (habilitação/ênfase) e ``A``/``R`` mantêm o programa vivo — exigir
            # apenas {A,R} deletaria veteranos cujo último evento é ``H``.
            mask_ativo = cand["_stapgm_pit"].isna() | ~cand["_stapgm_pit"].isin(
                MORTO_STAPGM
            )
            # Métrica de leakage para inspeção (consolida N "fantasmas").
            n_ghost = int((cand["_stapgm_pit"].notna() & ~mask_ativo).sum())
            cand = cand[mask_ativo].drop(columns=["_stapgm_pit"])
            if n_ghost:
                sys.stderr.write(
                    f"[alunos_ativos] D{dta_corte.date()}: filtro "
                    f"point-in-time (HISTPROGGR) removeu {n_ghost:,} evadidos/"
                    f"trancados que passavam pelo filtro de dtaclcgru.\n"
                )

    return cand[["codpes", "codcur", "codhab", "dtaing"]].drop_duplicates(
        ["codpes", "codcur", "codhab"]
    )


def _curriculos_elegiveis(
    alunos: pd.DataFrame,
    curriculo: pd.DataFrame,
    dta_corte: pd.Timestamp,
) -> pd.DataFrame:
    """Heurística de União de Currículos Elegíveis: para cada aluno, todos os
    ``codcrl`` do seu (codcur, codhab) cuja ``dtafimcrl`` é nula ou posterior
    ao ingresso do aluno, e ``dtainicrl`` ≤ Dia D. Devolve (codpes, codcrl).

    Importante: ``alunos`` aqui deve manter **todas** as linhas (codcur,
    codhab) por codpes — é o plano tangível de currículos por onde o aluno
    pode vir a se formar (união). Por isso Deduplica por codcrl no final,
    não por codpes: dois currículos de habilitações distintas do mesmo aluno
    devem ser considerados."""
    if not len(alunos) or not len(curriculo):
        return pd.DataFrame(columns=["codpes", "codcrl"])
    crr = curriculo.copy()
    elig = alunos[["codpes", "codcur", "codhab", "dtaing"]].merge(
        crr, on=["codcur", "codhab"], how="inner"
    )
    mask = (
        (elig["dtafimcrl"].isna() | (elig["dtafimcrl"] > elig["dtaing"]))
        & (elig["dtainicrl"].isna() | (elig["dtainicrl"] <= dta_corte))
    )
    elig = elig[mask]
    return elig[["codpes", "codcrl"]].drop_duplicates()


def _necessidade_curricular(
    alunos: pd.DataFrame,
    elig: pd.DataFrame,
    grade: pd.DataFrame,
    curriculo: pd.DataFrame,
) -> pd.DataFrame:
    """Para cada (aluno, coddis) entre os currículos elegíveis do aluno:
    - ``status_obrigatoriedade_otimista``: 'O' se obrigatória em algum
      currículo; 'E/C' se Eletiva/Complementar; 'L' (Livre) caso contrário.
    - ``min_numsemidl``: menor semestre ideal da disciplina entre elegíveis.
    - ``max_carga_total``: maior ``cgahrtot`` entre os currículos elegíveis
      (denominador do ``perc_conclusao_aprovado``).
    """
    cg = grade.merge(elig, on="codcrl", how="inner")
    if not len(cg):
        return pd.DataFrame(
            columns=["codpes", "coddis", "status_obrigatoriedade_otimista",
                     "min_numsemidl", "max_carga_total"]
        )
    cg["numsemidl"] = pd.to_numeric(cg["numsemidl"], errors="coerce")
    cg["otimista"] = cg["tipobg"].map(
        {"O": 0, "E": 1, "C": 1, "P": 2, "L": 3, "F": 2}
    )

    # MAX cgahortot por (codpes, coddis): join curriculo (codcrl, cgahortot).
    if len(curriculo):
        cg_m = cg.merge(
            curriculo[["codcrl", "cgahortot"]], on="codcrl", how="left"
        )
        cg_m["cgahortot"] = pd.to_numeric(cg_m["cgahortot"], errors="coerce")
    else:
        cg_m = cg.copy()
        cg_m["cgahortot"] = np.nan

    ag = (
        cg_m.groupby(["codpes", "coddis"], sort=False)
        .agg(
            _min_otim=("otimista", "min"),
            min_numsemidl=("numsemidl", "min"),
            max_carga_total=("cgahortot", "max"),
        )
        .reset_index()
    )
    ag["status_obrigatoriedade_otimista"] = np.where(
        ag["_min_otim"] == 0, "O", np.where(ag["_min_otim"] == 1, "E/C", "L")
    )
    return ag.drop(columns="_min_otim")


# ---------------------------------------------------------------------------
# Bloco 2: histórico point-in-time agregado por aluno
# ---------------------------------------------------------------------------
def _filtrar_hist_passado(
    hist: pd.DataFrame, dta_corte: pd.Timestamp
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Devolve (registros_passado, concluido_passado) didaticamente:

    - ``registros_passado``: inscrições criadas (< dta_corte) — usadas para
      "cursou pré-req no t-1 sem nota" (Módulo B) e trancamentos (Módulo A).
    - ``concluido_passado``: inscrições com ``rstfim`` consolidado antes do
      Dia D (``dtaultalt`` < dta_corte quando houver) — contam aprovações/
      reprovações e créditos aprovados para o Módulo C.

    Tudo **estritamente anterior** ao ``dtainitur - dias_corte`` (Regra de Ouro).
    """
    h = hist.copy()
    registros = h[h["dtacrihst"] < dta_corte]
    concluido = registros[
        registros["rstfim"].notna()
        & (registros["dtaultalt"].isna() | (registros["dtaultalt"] < dta_corte))
    ]
    return registros, concluido


def _historico_agregado_aluno(
    concluido: pd.DataFrame,
    sem_alvo: int,
    disciplina_cre: pd.DataFrame,
) -> pd.DataFrame:
    """Agregados point-in-time por (codpes, coddis) e por codpes, vetorizados.

    Saídas (chave por codpes; colunas por coddis quando indicado):

    - ``qtd_reprovacoes_discip`` (por coddis)
    - ``flag_aprovado_discip``: 1 se já aprovado nesta coddis
    - ``flag_trancamento_previo`` (por coddis): 1 se stamtr='E' e rstfim='T'
    - ``creditos_aprovados``: soma creaul+cretrb aprovados (por codpes)
    - ``media_ponderada_suja_hist`` (por codpes): lote da regra do Graduacao
    - ``velocidade_historica_creditos`` (por codpes): média de créditos
      aprovados por semestre letivo passado
    - ``qtd_aprovadas`` / ``qtd_cursadas`` (por codpes) para ``taxa_sucesso``
    """
    out_dis = pd.DataFrame()
    if len(concluido):
        c = concluido.copy()
        rep = (
            c[c["rstfim"].isin(RSTFM_REPROVACAO)]
            .groupby(["codpes", "coddis"], sort=False)
            .size()
            .rename("qtd_reprovacoes_discip")
            .reset_index()
        )
        ap = (
            c[c["rstfim"].isin(RSTFM_APROVACAO)]
            .groupby(["codpes", "coddis"], sort=False)
            .size()
            .rename("_n_ap")
            .reset_index()
        )
        ap["flag_aprovado_discip"] = 1
        # Trancamento "nos 2 semestres letivos anteriores": aproximamos por
        # registros de t-2 e t-1 cuja codtur pertença a esses semestres.
        t1 = _sem_anterior_letivo(sem_alvo)
        t2 = _sem_anterior_letivo(t1)
        sems_prev = {t1, t2}
        # Bug-fix: trancamentos parciais têm rstfim == "T", mas o stamtr
        # varia ('C', 'E', 'I', 'M') -- exigir stamtr=='E' zera a feature.
        # Filtramos puramente por rstfim == "T" nos semestres t-1 e t-2.
        trac = concluido[
            (concluido["rstfim"] == "T")
            & (concluido["ano_sem"].isin(sems_prev))
        ]
        tr = (
            trac.groupby(["codpes", "coddis"], sort=False)
            .size()
            .rename("flag_trancamento_previo")
            .reset_index()
        )
        tr["flag_trancamento_previo"] = (tr["flag_trancamento_previo"] > 0).astype(int)
        out_dis = rep.merge(ap, on=["codpes", "coddis"], how="outer").merge(
            tr, on=["codpes", "coddis"], how="outer"
        )
        out_dis = out_dis.fillna(
            {
                "qtd_reprovacoes_discip": 0,
                "flag_aprovado_discip": 0,
                "flag_trancamento_previo": 0,
            }
        )
        out_dis["codpes"] = out_dis["codpes"].astype(int)
    else:
        out_dis = pd.DataFrame(
            columns=["codpes", "coddis", "qtd_reprovacoes_discip",
                      "flag_aprovado_discip", "flag_trancamento_previo"]
        )

    # Agregados por codpes (perfil histórico).
    perfil = pd.DataFrame()
    if len(concluido):
        c = concluido.merge(
            disciplina_cre[["coddis", "creaul", "cretrb"]], on="coddis", how="left"
        )
        c["creaul"] = pd.to_numeric(c["creaul"], errors="coerce").fillna(0)
        c["cretrb"] = pd.to_numeric(c["cretrb"], errors="coerce").fillna(0)
        c["_cre"] = c["creaul"] + c["cretrb"]
        c["_nota"] = c["notfim2"]
        falta_nota = c["_nota"].isna() | (c["_nota"] == "")
        c.loc[falta_nota, "_nota"] = c.loc[falta_nota, "notfim"]
        c["_nota"] = pd.to_numeric(c["_nota"], errors="coerce")
        c["_rstfim_hist"] = c["rstfim"]

        aprov = c[c["_rstfim_hist"].isin(RSTFM_APROVACAO)]
        cred_ap = (
            aprov.groupby("codpes", sort=False)["_cre"].sum().rename("creditos_aprovados")
        )
        # Média ponderada suja: rstfim ∈ {A,RN,RA,RF} com nota presente.
        suja = c[
            c["_rstfim_hist"].isin(RSTFM_APROVACAO + RSTFM_REPROVACAO)
            & c["_nota"].notna()
        ].copy()
        suja["_peso"] = suja["_cre"].where(suja["_cre"] > 0)
        suja = suja.dropna(subset=["_peso"])
        suja["_wnota"] = suja["_nota"] * suja["_peso"]
        mp = (
            (suja.groupby("codpes", sort=False)["_wnota"].sum()
             / suja.groupby("codpes", sort=False)["_peso"].sum())
            .rename("media_ponderada_suja_hist")
            .round(1)
        )
        # Velocidade: média de créditos aprovados por semestre letivo passado.
        vel = (
            aprov.groupby(["codpes", "ano_sem"], sort=False)["_cre"]
            .sum()
            .groupby("codpes", sort=False)
            .mean()
            .rename("velocidade_historica_creditos")
        )
        n_ap = aprov.groupby("codpes", sort=False).size().rename("qtd_aprovadas")
        n_cur = (
            c[c["_rstfim_hist"].notna()]
            .groupby("codpes", sort=False)
            .size()
            .rename("qtd_cursadas")
        )
        perfil = pd.concat(
            [cred_ap, mp, vel, n_ap, n_cur], axis=1
        ).reset_index()
        perfil["taxa_sucesso_historica"] = np.where(
            perfil["qtd_cursadas"] > 0,
            perfil["qtd_aprovadas"] / perfil["qtd_cursadas"],
            0.0,
        )
        perfil = perfil.fillna(
            {
                "creditos_aprovados": 0,
                "media_ponderada_suja_hist": 0.0,
                "velocidade_historica_creditos": 0.0,
                "qtd_aprovadas": 0,
                "qtd_cursadas": 0,
            }
        )
        perfil["codpes"] = perfil["codpes"].astype(int)
    else:
        perfil = pd.DataFrame(
            columns=["codpes", "creditos_aprovados", "media_ponderada_suja_hist",
                      "velocidade_historica_creditos", "qtd_aprovadas",
                      "qtd_cursadas", "taxa_sucesso_historica"]
        )
    return out_dis, perfil


# ---------------------------------------------------------------------------
# Bloco 3: matriz base (positivos + negativos plausíveis)
# ---------------------------------------------------------------------------
def _positivos_sem(
    cfg: DatasetAlunoConfig,
    hist_aluno: pd.DataFrame,
    codpes_ativos: set[int],
    turmas_sem: pd.DataFrame,
) -> pd.DataFrame:
    """Positivos (y=1): (codpes, coddis, codtur) cujo registro estava **vivo
    no dia do pico de ocupação** da turma (T_pico).

    Regra de Ouro (T_pico, ver :func:`reconstruir_alvo_nummtr_max`): para cada
    turma, ``D* = argmax ocupação(D)`` em ``D ∈ dias_pico`` ≡
    ``[dtainitur, dtainitur+21]`` (pós-início das aulas), onde
    ``ocupação(D) = |criados ≤ D| − |excluídos (stamtr E/R) com dtaultalt ≤ D|``.
    O aluno conta como positivo sse, nesse D*, seu registro estava vivo:
    ``dtacrihst ≤ D*`` E não (``stamtr ∈ {E,R}`` com ``dtaultalt ≤ D*``).

    Isto evita marcar y=1 para alunos que se inscreveram mas trancaram/
    excluíram antes do pico (devem ser y=0), e captura alunos presentes no
    instante de lotação máxima — alvo alinhado ao T_pico da infraestrutura
    de alocação (AGENTS.md)."""
    if not len(hist_aluno) or not len(turmas_sem):
        return pd.DataFrame(columns=["codpes", "coddis", "codtur"])
    h = hist_aluno.merge(
        turmas_sem[["coddis", "codtur", "dtainitur"]], on=["coddis", "codtur"], how="inner"
    )
    h = h.dropna(subset=["dtacrihst", "dtainitur"])
    h["codpes"] = pd.to_numeric(h["codpes"], errors="coerce")
    h = h.dropna(subset=["codpes"])
    h = h[h["codpes"].isin(codpes_ativos)]

    # Offset em dias desde dtainitur (única unidade comum a todas as turmas).
    cri_d = (h["dtacrihst"] - h["dtainitur"]).dt.days
    excl = h["stamtr"].isin(["E", "R"])
    exc_d = (h["dtaultalt"] - h["dtainitur"]).dt.days.where(excl)
    h = h.assign(cri_d=cri_d, exc_d=exc_d)

    # Para cada turma, D* = argmax ocupação(D) em dias pós-início (d >= 0).
    # Empregamos o mesmo dia-pico discreto (dias_pico) da infraestrutura de
    # alocação: idxmax em ocup_d{d} p/ d >= 0 dá D* em dias-pico.
    dias = [d for d in cfg.dias_pico if d >= 0]
    # ocupação por (turma, d) — Series MultiIndex perfeito para reindex.
    ocup_por_d: dict[int, pd.Series] = {}
    for d in dias:
        cri = h[h["cri_d"] <= d].groupby(["coddis", "codtur"]).size()
        exc_ate = (
            h[h["exc_d"].notna() & (h["exc_d"] <= d)]
            .groupby(["coddis", "codtur"]).size()
        )
        col = pd.DataFrame({"cri": cri, "exc": exc_ate}).fillna(0)
        ocup = (col["cri"] - col["exc"]).astype(int)
        ocup_por_d[d] = ocup
    # DataFrame idx= (coddis, codtur); cols = d.cada d ∈ dias_pico >= 0.
    ocup_df = pd.DataFrame(ocup_por_d)
    ocup_df = ocup_df.fillna(0).astype(int)
    if not len(ocup_df):
        return pd.DataFrame(columns=["codpes", "coddis", "codtur"])
    # D* por turma: argmax sobre os dias_pico pós-início (idx: dias_pico).
    key_cols = ocup_df.index.names
    d_star_series = ocup_df.idxmax(axis=1)  # valor: um de dias (>= 0)

    # Portão vivo-em-D* por par (aluno, turma). Broadcast do D* turma para
    # cada registro do aluno, depois filtra vivo.
    h = h.merge(
        d_star_series.rename("d_star").reset_index(),
        on=list(key_cols), how="left",
    )
    h = h.dropna(subset=["d_star"])
    vivo = h["cri_d"] <= h["d_star"]
    excl_ate_pico = h["stamtr"].isin(["E", "R"]) & h["exc_d"].notna() & (h["exc_d"] <= h["d_star"])
    vivo = vivo & ~excl_ate_pico
    h = h[vivo]
    return h[["codpes", "coddis", "codtur"]].drop_duplicates()


def _negativos_via_requerimento(
    cfg: DatasetAlunoConfig,
    dados: dict[str, pd.DataFrame],
    alunos: pd.DataFrame,
    turmas_sem: pd.DataFrame,
    aprovados: pd.DataFrame,
    dta_corte: pd.Timestamp,
) -> pd.DataFrame:
    """Candidatos negativos adicionais: (codpes, coddis, codtur) com
    requerimento de matrícula/intenção aberto próximo ao Dia D."""
    req = dados.get("requer")
    if req is None or not len(req) or not len(turmas_sem):
        return pd.DataFrame(columns=["codpes", "coddis", "codtur"])
    r = req.copy()
    if "dtacadrqm" not in r.columns:
        return pd.DataFrame(columns=["codpes", "coddis", "codtur"])
    r["codpes"] = pd.to_numeric(r["codpes"], errors="coerce")
    r = r.dropna(subset=["codpes"])
    ativos = set(alunos["codpes"].astype(int))
    r = r[r["codpes"].isin(ativos)]
    # Janela [dta_corte, dta_corte + janela] captura intenção no entorno do
    # Dia D (requerimentos costumam chegar dias antes das aulas).
    r = r[
        (r["dtacadrqm"] >= dta_corte - pd.Timedelta(days=cfg.janela_intencao_dias))
        & (r["dtacadrqm"] <= dta_corte + pd.Timedelta(days=cfg.janela_intencao_dias))
    ]
    if "tiprqm" in r.columns:
        # Mantemos requerimentos relacionados a matrícula/inscrição em disciplina.
        r = r[r["tiprqm"].astype(str).str.contains(
            "mat|inscr|discip", case=False, na=False
        )]
    r = r[["codpes", "coddis", "codtur"]].dropna(subset=["coddis"])
    r = r.merge(
        turmas_sem[["coddis", "codtur"]], on=["coddis", "codtur"], how="inner"
    )
    r["codpes"] = r["codpes"].astype(int)
    # Remove os já aprovados na disciplina (exclusão obrigatória).
    if len(aprovados):
        r = r.merge(
            aprovados[["codpes", "coddis"]].assign(_ap=1),
            on=["codpes", "coddis"], how="left",
        )
        r = r[r["_ap"].isna()].drop(columns="_ap")
    return r[["codpes", "coddis", "codtur"]].drop_duplicates()


# ---------------------------------------------------------------------------
# Negativos L (livres) balanceados — flag ``balancear_l``
# ---------------------------------------------------------------------------
def _negativos_l_balanceados(
    cfg: DatasetAlunoConfig,
    pos: pd.DataFrame,
    neg: pd.DataFrame,
    necess: pd.DataFrame,
    aprovados: pd.DataFrame,
    tur: pd.DataFrame,
    sem_alvo: int,
) -> pd.DataFrame:
    """Negativos L (livres) amostrados para balancear a classe no semestre.

    Decisões de design (ver análise pré-implementação no histórico):

    - **Pool**: alunos com ≥1 matrícula no semestre (qualquer tipo — derivado
      de ``pos``) × coddis L ofertadas = coddis **fora do currículo elegível**
      do aluno (não presentes em ``necess``), não aprovadas, ainda não em
      ``pos``/``neg``. Decision 1.A.
    - **Razão-alvo**: média das razões ``pos/neg`` de O e E/C, calculada
      **sobre matriculados** (mesmo pool do L), **por semestre**. Decisions
      2.1, 3.A, 4.A.
    - **Volume**: amostra ``to_sample = max(0, target_neg_l − existing_neg_l)``
      do pool, SEM ``max_neg_turmas_por_disc`` para L. Decision 5.B.
    - **Reprodutibilidade**: ``random_state`` derivado de ``codundclg`` e
      ``sem_alvo`` (estável entre runs).

    ``pos``/``neg`` aqui já contêm os negativos via REQUERIMENTOGR
    (``existing_neg_l``), que são contados no alvo para não re-amostrar nem
    duplicar. ``neg`` é o DataFrame consolidado após ``req_cand`` em
    :func:`_build_matriz_sem`.
    """
    empty = pd.DataFrame(
        columns=["codpes", "coddis", "codtur", "alvo_matriculado"]
    )
    if not len(pos):
        return empty

    def _s(s: pd.Series) -> pd.Series:
        return s.astype(str).str.strip().str.upper()

    # ---- Status lookup: (codpes, coddis) → status O/E/C/L ---------------
    status_lookup = (
        necess[["codpes", "coddis", "status_obrigatoriedade_otimista"]]
        .drop_duplicates(["codpes", "coddis"])
        .copy()
    )
    status_lookup["codpes"] = status_lookup["codpes"].astype(int)
    status_lookup["coddis"] = _s(status_lookup["coddis"])
    status_lookup = status_lookup.rename(
        columns={"status_obrigatoriedade_otimista": "st"}
    )

    def _with_status(df: pd.DataFrame) -> pd.DataFrame:
        d = df[["codpes", "coddis"]].copy()
        d["codpes"] = d["codpes"].astype(int)
        d["coddis"] = _s(d["coddis"])
        d = d.merge(status_lookup, on=["codpes", "coddis"], how="left")
        d["st"] = d["st"].fillna("L")
        return d

    # ---- Conta positivos por status (pos é só matriculados) ----------
    pos_s = _with_status(pos)
    pos_o = int((pos_s["st"] == "O").sum())
    pos_ec = int((pos_s["st"] == "E/C").sum())
    pos_l = int((pos_s["st"] == "L").sum())

    if pos_l == 0:
        return empty

    # ---- Conta negativos O/E/C entre matriculados (mesmo universo) -----
    codpes_matriculados = set(pos["codpes"].astype(int))
    neg_matr = neg[neg["codpes"].astype(int).isin(codpes_matriculados)]
    neg_matr_s = _with_status(neg_matr) if len(neg_matr) else pos_s.iloc[0:0].copy()
    neg_o = int((neg_matr_s["st"] == "O").sum())
    neg_ec = int((neg_matr_s["st"] == "E/C").sum())
    existing_neg_l = int((neg_matr_s["st"] == "L").sum())

    # ---- Razão-alvo: média das razões pos/neg de O e E/C --------------
    razoes = [r for r in (
        pos_o / neg_o if neg_o > 0 else None,
        pos_ec / neg_ec if neg_ec > 0 else None,
    ) if r is not None]
    if not razoes:
        return empty
    razao_alvo = float(np.mean(razoes))
    if not razao_alvo > 0:
        return empty

    target_neg_l = int(round(pos_l / razao_alvo))
    to_sample = max(0, target_neg_l - existing_neg_l)
    if to_sample <= 0:
        return empty

    # ---- Pool: matriculados × coddis L ofertadas (fora do currículo) ---
    # Anti-join contra necess (fora do currículo) e aprovados, via
    # merges com marcadores, tolerantes a divergências de dtype.
    matr = pd.DataFrame({"codpes": sorted(codpes_matriculados)})
    matr["codpes"] = matr["codpes"].astype(int)

    offered = tur[["coddis"]].drop_duplicates().copy()
    offered["coddis"] = _s(offered["coddis"])

    pool_cd = matr.merge(offered, how="cross")
    pool_cd["coddis"] = _s(pool_cd["coddis"])

    # Remove pares que estão no currículo elegível do aluno (status O/E/C/L
    # curricular). Restam apenas coddis FORA do currículo = L desta aluno.
    nec_cd = (
        necess[["codpes", "coddis"]].drop_duplicates(["codpes", "coddis"]).copy()
    )
    nec_cd["codpes"] = nec_cd["codpes"].astype(int)
    nec_cd["coddis"] = _s(nec_cd["coddis"])
    pool_cd = pool_cd.merge(
        nec_cd.assign(_n=1), on=["codpes", "coddis"], how="left"
    )
    pool_cd = pool_cd[pool_cd["_n"].isna()].drop(columns="_n")

    # Remove disciplinas já aprovadas pelo aluno.
    ap_cd = aprovados[["codpes", "coddis"]].drop_duplicates().copy()
    ap_cd["codpes"] = ap_cd["codpes"].astype(int)
    ap_cd["coddis"] = _s(ap_cd["coddis"])
    pool_cd = pool_cd.merge(
        ap_cd.assign(_a=1), on=["codpes", "coddis"], how="left"
    )
    pool_cd = pool_cd[pool_cd["_a"].isna()].drop(columns="_a")
    if not len(pool_cd):
        return empty

    # Expande para (codpes, coddis, codtur) via oferta de turmas.
    tur_cd = tur[["coddis", "codtur"]].drop_duplicates().copy()
    tur_cd["coddis"] = _s(tur_cd["coddis"])
    pool = pool_cd.merge(tur_cd, on="coddis", how="inner")
    pool["codtur"] = pool["codtur"].astype(str)
    if not len(pool):
        return empty

    # Remove (codpes, coddis, codtur) já presentes em pos OU neg (incl. req_cand).
    occupied = pd.concat(
        [pos[["codpes", "coddis", "codtur"]], neg[["codpes", "coddis", "codtur"]]],
        ignore_index=True,
    ).drop_duplicates()
    occupied["codpes"] = occupied["codpes"].astype(int)
    occupied["coddis"] = _s(occupied["coddis"])
    occupied["codtur"] = occupied["codtur"].astype(str)
    pool = pool.merge(
        occupied.assign(_o=1), on=["codpes", "coddis", "codtur"], how="left"
    )
    pool = pool[pool["_o"].isna()].drop(columns="_o")
    if not len(pool):
        return empty

    # ---- Amostragem ao alvo de razão (sem max_neg_turmas_por_disc p/ L) --
    n = min(to_sample, len(pool))
    if n < len(pool):
        seed = int(cfg.codundclg) * 1000003 + int(sem_alvo)
        pool = pool.sample(n=n, random_state=seed)

    # Enriquecer com as colunas de ``tur`` (mesmo formato de ``neg``).
    tur_full = tur[["coddis", "codtur", "sufixo", "ano_sem", "sem_tipo",
                    "dtainitur"]].drop_duplicates(["coddis", "codtur"]).copy()
    tur_full["coddis"] = _s(tur_full["coddis"])
    tur_full["codtur"] = tur_full["codtur"].astype(str)
    neg_l = pool[["codpes", "coddis", "codtur"]].merge(
        tur_full, on=["coddis", "codtur"], how="left"
    )
    neg_l["alvo_matriculado"] = 0
    return neg_l[["codpes", "coddis", "codtur", "sufixo", "ano_sem", "sem_tipo",
                 "dtainitur", "alvo_matriculado"]]


def _build_matriz_sem(
    cfg: DatasetAlunoConfig,
    dados: dict[str, pd.DataFrame],
    sem_alvo: int,
    turmas_sem: pd.DataFrame,
) -> pd.DataFrame:
    """Constrói a matriz base (positivos + negativos) para UM semestre alvo."""
    # Bug-fix: o unpacking ``ano_alvo, sem_alvo = sem_alvo // 10, sem_alvo % 10``
    # rebindava ``sem_alvo`` ao DÍGITO do semestre (1 ou 2), fazendo
    # _historico_agregado_aluno receber 2 em vez de 20242 — bug que zerava
    # ``flag_trancamento_previo`` (sems_prev = {-8, 1} nÃ£o casa ninguÃ©m).
    # Agora o dígito fica em ``sem_alvo_dig`` e o composto ``sem_alvo`` Ã©
    # preservado para downstream (HIST agrega por ano_sem*10+dÃ­gito).
    ano_alvo, sem_alvo_dig = sem_alvo // 10, sem_alvo % 10
    dta_corte = (
        turmas_sem["dtainitur"].min() - pd.Timedelta(days=cfg.dias_corte)
    )

    alunos = _alunos_ativos(cfg, dados, dta_corte)
    if not len(alunos):
        return pd.DataFrame()
    # A "Heurística de União de Currículos Elegíveis" usa TODAS as
    # habilitações-programa ativas do aluno (alunos_full). Mas para a
    # **matriz base** (positivos e negativos) o aluno conta uma única vez:
    # deduplica por codpes mantendo o programa mais recente (dtaing maior),
    # que é o programa vigente no Dia D. Sem isso, um aluno com 7 entradas
    # antigas gera 7 linhas iguais (alvo, turma) ~inflando alvo_matriculado
    # para 2-3× mais do que realmente este na turma.
    alunos_full = alunos.copy()
    alunos = (
        alunos.sort_values("dtaing", ascending=False)
        .drop_duplicates("codpes", keep="first")
        .copy()
    )
    alunos["sem_casa"] = alunos["dtaing"].map(
        lambda d: _sem_casa(d, ano_alvo, sem_alvo_dig)
    )
    alunos["codpes"] = alunos["codpes"].astype(int)

    elig = _curriculos_elegiveis(alunos_full, dados["curriculo"], dta_corte)
    necess = _necessidade_curricular(
        alunos, elig, dados["grade"], dados["curriculo"]
    )
    if not len(necess):
        return pd.DataFrame()

    registros, concluido = _filtrar_hist_passado(dados["hist_aluno"], dta_corte)
    out_dis, perfil = _historico_agregado_aluno(
        concluido, sem_alvo, dados["disciplina"]
    )

    aprovados = out_dis[out_dis["flag_aprovado_discip"] == 1][["codpes", "coddis"]]
    codpes_ativos = set(alunos["codpes"].astype(int))

    # ---- Positivos (y=1) --------------------------------------------------
    pos = _positivos_sem(cfg, dados["hist_aluno"], codpes_ativos, turmas_sem)
    pos = pos.merge(
        aprovados.assign(_ap=1), on=["codpes", "coddis"], how="left"
    )
    # Filtr de exclusão também para positivos: se aprovado antes do Dia D,
    # não deve haver nova matrícula — remoção de ruído de cadastro.
    pos = pos[pos["_ap"].isna()].drop(columns="_ap", errors="ignore")
    pos["alvo_matriculado"] = 1

    # Universo de alunos que efetivamente matricularam em ≥1 turma no
    # semestre (origem de y=1). Usado por ``excluir_fantasmas`` (filtro de
    # negativos) e por ``balancear_l`` (pool de negativos L).
    codpes_matriculados: set[int] = (
        set(pos["codpes"].astype(int)) if len(pos) else set()
    )

    # ---- Negativos (y=0): obrigatórias/eletivas pendentes, na janela ------
    # Junta necessidade (aluno, coddis) com créditos reprovados/trancamento.
    cand = necess.merge(
        out_dis, on=["codpes", "coddis"], how="left"
    ).merge(
        alunos[["codpes", "sem_casa", "dtaing"]], on="codpes", how="left"
    )
    cand = cand.fillna(
        {
            "qtd_reprovacoes_discip": 0,
            "flag_aprovado_discip": 0,
            "flag_trancamento_previo": 0,
        }
    )
    # Exclusão obrigatória: já aprovado antes do Dia D.
    cand = cand[cand["flag_aprovado_discip"] == 0]
    # Só gera negativos para O/E/C (Livres não entram via grade).
    cand = cand[cand["status_obrigatoriedade_otimista"].isin(["O", "E/C"])]
    # Horizonte temporal: semestre ideal ≤ sem_casa + horizonte.
    cand["min_numsemidl"] = pd.to_numeric(cand["min_numsemidl"], errors="coerce")
    cand = cand[cand["min_numsemidl"] <= cand["sem_casa"] + cfg.horizonte_sem]

    # Flag ``excluir_fantasmas``: mantém só negativos de alunos com ≥1
    # matrícula no semestre (``codpes_matriculados``). ``pos`` fica intacto.
    if cfg.excluir_fantasmas:
        cand = cand[cand["codpes"].isin(codpes_matriculados)]

    # Normativa: para o ``neg`` SÓ usamos as 3 chaves primárias
    # (codpes, coddis, codtur) -- TODAS as features curriculares/históricas
    # são enriquecidas pós-concat (passo de baixo) via merge LEFT contra
    # ``necess``/``out_dis``/``alunos_uni``. Antes o ``neg`` já trazia
    # ``status_obrigatoriedade_otimista`` de ``candid``, gerando colisão
    # de sufixos (``_x``/``_y``) no merge pós-concat e invertendo o
    # preenchimento de 'L'. Padronizar ``neg`` ao mesmo esquema de ``pos``
    # elimina a ambiguidade.

    # Cruza com turmas ofertadas no semestre (mesma coddis).
    tur = turmas_sem[["coddis", "codtur", "sufixo", "ano_sem", "sem_tipo",
                      "dtainitur"]].drop_duplicates(["coddis", "codtur"])
    neg = cand[["codpes", "coddis"]].drop_duplicates().merge(
        tur, on="coddis", how="inner"
    )

    # Limita o número de turmas por disciplina/aluno para conter explosão.
    if cfg.max_neg_turmas_por_disc and len(neg):
        neg = (
            neg.sort_values(["codpes", "coddis", "codtur"])
            .groupby(["codpes", "coddis"], sort=False, group_keys=False)
            .head(cfg.max_neg_turmas_por_disc)
        )

    # Remove negativos que são, de fato, positivos (matrículas reais).
    if len(pos):
        neg = neg.merge(
            pos[["codpes", "coddis", "codtur"]].assign(_pos=1),
            on=["codpes", "coddis", "codtur"], how="left",
        )
        neg = neg[neg["_pos"].isna()].drop(columns="_pos", errors="ignore")
    neg["alvo_matriculado"] = 0

    # ---- Negativos adicionais via requerimento (intenção) -----------------
    if cfg.usar_intencao_requerimento:
        req_cand = _negativos_via_requerimento(
            cfg, dados, alunos, turmas_sem, aprovados, dta_corte
        )
        if cfg.excluir_fantasmas and len(req_cand):
            req_cand = req_cand[req_cand["codpes"].isin(codpes_matriculados)]
        if len(req_cand):
            # Mesma padronização: só chaves primárias.
            req_cand = req_cand.merge(
                tur[["coddis", "codtur", "sufixo", "ano_sem", "sem_tipo",
                     "dtainitur"]],
                on=["coddis", "codtur"], how="left",
            )
            # Não duplica linhas já presentes em neg/pos.
            base_keys = pd.concat(
                [
                    neg[["codpes", "coddis", "codtur"]],
                    pos[["codpes", "coddis", "codtur"]],
                ]
            ).assign(_e=1)
            req_cand = req_cand.merge(
                base_keys, on=["codpes", "coddis", "codtur"], how="left"
            )
            req_cand = req_cand[req_cand["_e"].isna()].drop(columns="_e", errors="ignore")
            req_cand["alvo_matriculado"] = 0
            neg = pd.concat([neg, req_cand], ignore_index=True)

    # ---- Negativos L (livres) balanceados — flag ``balancear_l`` ----------
    # ``neg`` aqui já consolida grade + REQUERIMENTOGR; ``_negativos_l_balanceados``
    # conta os neg-L existentes no alvo de razão para não re-amostrar nem duplicar.
    if cfg.balancear_l and len(pos) and len(neg):
        neg_l = _negativos_l_balanceados(
            cfg, pos, neg, necess, aprovados, tur, sem_alvo
        )
        if len(neg_l):
            neg = pd.concat([neg, neg_l], ignore_index=True)

    base = pd.concat([pos, neg], ignore_index=True)
    if not len(base):
        return base
    base["ano_sem"] = sem_alvo
    base["sem_tipo"] = base["codtur"].str[4].map({"1": "1S", "2": "2S"})
    base["sufixo"] = base["codtur"].str[-2:].astype(int)

    # ---- Enriquecimento pós-concat: NEED, OUT_DIS, PERFIL -----------------
    # Bug-fix: antes, ``base.merge(candid[["codpes","coddis"]]...)`` só
    # marcava presença da necessidade no ``candid`` -- não copiava as colunas
    # de status (``status_obrigatoriedade_otimista``, ``min_numsemidl``,
    # ``max_carga_total``). Como ``pos`` vinha sem essas colunas e ``neg`` já
    # as trazia, o ``fillna('status_obrigatoriedade_otimista', 'L')`` abaixo
    # marcava TODOS os positivos como 'L'. Com um filtro Jupyter ``!= 'L'``
    # (para excluir as optativas livres), eliminava-se 100% dos y=1. Além
    # disso ``delta_semestre_ideal`` ficava NaN puro nos positivos → vazamento
    # reversível (AUC=1.0 mágico no modelo).
    # Solução: merge LEFT explícito de ``necess`` em ``base``, trazendo os
    # valores reais de status/min_numsemidl/max_carga_total para os positivos
    # em O/E/C; pares (aluno, coddis) fora de qualquer currículo elegível
    # (ex.: optativa livre por intenção/REQUERIMENTOGR) seguem como NaN → viram
    # 'L' (Optativa Livre) na imputação abaixo, agora por razão correta.

    # 1. Necessidade curricular (união de currículos elegíveis).
    base = base.merge(
        necess[["codpes", "coddis", "status_obrigatoriedade_otimista",
                "min_numsemidl", "max_carga_total"]],
        on=["codpes", "coddis"], how="left",
    )

    # 2. Histórico pontual por (aluno, disciplina): reprovacoes/trancamento.
    # Bug-fix dtype: ``base["coddis"]`` e ``out_dis["coddis"]`` podem estar em
    # dtypes string diferentes (object vs StringDtype) após os merges
    # anteriores ``necess`` (StringDtype a partir de GRADECURRICULAR.join) e
    # ``pos``/``neg`` (StringDtype via turmas_sem). Cast explícito garante o
    # casamento de chaves e evita flag_trancamento_previo virar 0 puro.
    _odn = out_dis[["codpes", "coddis", "qtd_reprovacoes_discip",
                    "flag_trancamento_previo"]].copy()
    _odn["coddis"] = _odn["coddis"].astype(str)
    _bn = base[["codpes", "coddis"]].copy()
    _bn["coddis"] = _bn["coddis"].astype(str)
    # dropna para não inflar outer-style merges.
    _odn["codpes"] = pd.to_numeric(_odn["codpes"], errors="coerce")
    _bn["codpes"] = pd.to_numeric(_bn["codpes"], errors="coerce")
    base = base.merge(
        _odn,
        on=["codpes", "coddis"], how="left",
    )

    # 3. Perfil curricular do aluno (sem_casa, dtaing, codcur, codhab).
    alunos_uni = alunos[["codpes", "sem_casa", "dtaing", "codcur", "codhab"]].drop_duplicates(["codpes"])
    base = base.merge(alunos_uni, on="codpes", how="left")

    # Imputação: colunas que legítimamente NÃO existem para um par
    # (aluno,coddis) fora de currículo elegível viram 'L' / NaN.
    for col, default in (
        ("status_obrigatoriedade_otimista", "L"),
        ("min_numsemidl", np.nan),
        ("qtd_reprovacoes_discip", 0),
        ("flag_trancamento_previo", 0),
        ("max_carga_total", np.nan),
    ):
        if col not in base.columns:
            base[col] = default
        else:
            base[col] = base[col].fillna(default)

    # 4. Perfil histórico agregado (media_ponderada, velocidade, taxa_sucesso).
    base = base.merge(perfil, on="codpes", how="left")
    # Imputação defensiva via loop (cria a coluna se ainda não existir em
    # ``base`` -- o ``fillna`` puro não cria colunas ausentes, e ``sem_casa``
    # chega só via ``alunos_uni`` do passo 3; refazer via loop garante a
    # presença para ``delta_semestre_ideal``).
    for col, default in (
        ("creditos_aprovados", 0),
        ("media_ponderada_suja_hist", 0.0),
        ("velocidade_historica_creditos", 0.0),
        ("qtd_aprovadas", 0),
        ("qtd_cursadas", 0),
        ("taxa_sucesso_historica", 0.0),
        ("sem_casa", np.nan),
        ("dtaing", pd.NaT),
    ):
        if col not in base.columns:
            base[col] = default
        else:
            base[col] = base[col].fillna(default)
    # delta_semestre_ideal = sem_casa - min_numsemidl (positivo = atraso)
    base["delta_semestre_ideal"] = base["sem_casa"] - base["min_numsemidl"]

    # perc_conclusao_aprovado
    base["perc_conclusao_aprovado"] = np.where(
        base["max_carga_total"].notna() & (base["max_carga_total"] > 0),
        base["creditos_aprovados"] / base["max_carga_total"],
        np.nan,
    )
    return base


# ---------------------------------------------------------------------------
# Módulo B: pré-requisitos (Feature Estrela de Outliers)
# ---------------------------------------------------------------------------
def _status_prerequisito_aluno(
    base: pd.DataFrame,
    cfg: DatasetAlunoConfig,
    dados: dict[str, pd.DataFrame],
    sem_alvo: int,
    dta_corte: pd.Timestamp,
) -> pd.DataFrame:
    """``status_prerequisito_aluno`` (categórica 0/1/2):

    - 0: Falta cumprir pré-requisito(s).
    - 1: Cumpriu oficialmente (rstfim='A' no passado).
    - 2: Cursou o pré-req no t-1, mas sem nota / sem consolidação até o Dia D.
    """
    req = dados.get("requisito")
    df = base.copy()
    df["status_prerequisito_aluno"] = 0
    if req is None or not len(req):
        # Sem REQUISITOGR disponível: assume cumprido (não punir indevidamente).
        df["status_prerequisito_aluno"] = 1
        return df

    # Pré-requisitos por coddis (todos os currículos da unidade — união).
    prereq = (
        req.dropna(subset=["coddis", "coddisreq"])
        .groupby("coddis", sort=False)["coddisreq"]
        .apply(lambda s: list(set(s)))
        .to_dict()
    )

    hist_aluno = dados["hist_aluno"]
    t1 = _sem_anterior_letivo(sem_alvo)
    # Histórico: concluídos passados (aprovados oficiais) e registros t-1.
    concluido = hist_aluno[
        (hist_aluno["dtacrihst"] < dta_corte)
        & hist_aluno["rstfim"].notna()
        & (hist_aluno["dtaultalt"].isna() | (hist_aluno["dtaultalt"] < dta_corte))
    ]
    ap_set = (
        concluido[concluido["rstfim"].isin(RSTFM_APROVACAO)]
        .groupby("codpes", sort=False)["coddis"]
        .apply(set)
        .to_dict()
    )
    # "Cursou no t-1 sem nota": registro criado antes do Dia D, mas sem
    # consolidação até o Dia D. Em dumps/extrações executadas hoje, o rstfim
    # de t-1 já foi preenchido "no futuro" (após o Dia D) — por isso a nota é
    # considerada pendente no Dia D se rstfim é nulo OU se a última alteração
    # do registro (dtaultalt) ocorreu em ou após dta_corte (barreira temporal
    # estrita point-in-time).
    sem_t1 = hist_aluno[hist_aluno["ano_sem"] == t1]
    mask_pendente_no_dia_d = (
        (sem_t1["dtacrihst"] < dta_corte)
        & (
            sem_t1["rstfim"].isna()
            | (sem_t1["dtaultalt"].notna() & (sem_t1["dtaultalt"] >= dta_corte))
        )
    )
    cursando_t1 = (
        sem_t1[mask_pendente_no_dia_d]
        .groupby("codpes", sort=False)["coddis"]
        .apply(set)
        .to_dict()
    )

    def _status(codpes: int, coddis: str) -> int:
        pres = prereq.get(coddis)
        if not pres:
            return 1  # sem pré-requisitos → cumprido
        aprov = ap_set.get(int(codpes), set())
        curs = cursando_t1.get(int(codpes), set())
        any_missing = False
        any_cursando = False
        for p in pres:
            if p in aprov:
                continue
            if p in curs:
                any_cursando = True
            else:
                any_missing = True
        if any_missing:
            return 0
        return 2 if any_cursando else 1

    df["status_prerequisito_aluno"] = [
        _status(c, d) for c, d in zip(df["codpes"], df["coddis"], strict=True)
    ]
    return df


# ---------------------------------------------------------------------------
# Módulo D: atrito / rejeição
# ---------------------------------------------------------------------------
def _modulo_d_atrito(
    base: pd.DataFrame,
    cfg: DatasetAlunoConfig,
    dados: dict[str, pd.DataFrame],
    turmas_sem: pd.DataFrame,
) -> pd.DataFrame:
    """``conflito_turno``, ``choque_horario_obrigatoria``, ``peso_carga_disciplina``."""
    df = base.copy()

    # peso_carga_disciplina
    disc = dados["disciplina"][["coddis", "creaul", "cretrb"]].drop_duplicates("coddis")
    disc["creaul"] = pd.to_numeric(disc["creaul"], errors="coerce").fillna(0)
    disc["cretrb"] = pd.to_numeric(disc["cretrb"], errors="coerce").fillna(0)
    disc["peso_carga_disciplina"] = disc["creaul"] + disc["cretrb"]
    df = df.merge(disc[["coddis", "peso_carga_disciplina"]], on="coddis", how="left")
    df["peso_carga_disciplina"] = df["peso_carga_disciplina"].fillna(0).astype(int)

    # conflito_turno: perhab do aluno (HABILITACAOGR) × flag_noturno da turma.
    if "codcur" in df.columns and "codhab" in df.columns and len(dados["_perhab"]):
        df = df.merge(dados["_perhab"], on=["codcur", "codhab"], how="left")
    if "perhab" not in df.columns:
        df["perhab"] = np.nan
    # flag_noturno da turma: rederiva de OCUPTURMA (já agregado em dados["ocup"]).
    ocup = dados.get("ocup")
    flag_not = pd.DataFrame()
    if ocup is not None and len(ocup):
        o = ocup[ocup["codtur"].isin(df["codtur"])]
        if len(o) and "horent" in o.columns:
            o = o.copy()
            o["horent_int"] = pd.to_numeric(
                o["horent"].astype(str).str[:2], errors="coerce"
            )
            o["flag_noturno"] = (o["horent_int"] >= 18).astype(int)
            flag_not = (
                o.groupby(["coddis", "codtur"], sort=False)["flag_noturno"]
                .max()
                .reset_index()
            )
    if len(flag_not):
        df = df.merge(flag_not, on=["coddis", "codtur"], how="left")
    else:
        df["flag_noturno"] = 0
    df["flag_noturno"] = df["flag_noturno"].fillna(0).astype(int)
    # Bug-fix: o Replicado (HABILITACAOGR.perhab) armazena turnos tanto
    # como siglas ('M','V','D','I','N') quanto por extenso. Aceitar ambas.
    perhab_str = df["perhab"].astype(str).str.strip().str.upper()
    # M, MATUTINO, V, VESPERTINO, D, DIURNO, I, INTEGRAL são
    # diurnos/integrais (conflitam com turma noturna).
    eh_diurno = perhab_str.isin(
        ["M", "MATUTINO", "V", "VESPERTINO", "D", "DIURNO", "I", "INTEGRAL"]
    )
    df["conflito_turno"] = np.where(
        (df["flag_noturno"] == 1) & eh_diurno, 1, 0
    )

    # choque_horario_obrigatoria: horário desta turma cruza com horário de
    # disciplina O pendente (do semestre ideal do aluno) ofertada no mesmo sem.
    df["choque_horario_obrigatoria"] = _choque_horario_obrigatoria(
        df, dados, turmas_sem
    )
    df["choque_horario_obrigatoria"] = (
        df["choque_horario_obrigatoria"].fillna(0).astype(int)
    )
    df.drop(columns=["perhab", "flag_noturno"], inplace=True, errors="ignore")
    return df


def _choque_horario_obrigatoria(
    df: pd.DataFrame,
    dados: dict[str, pd.DataFrame],
    turmas_sem: pd.DataFrame,
) -> pd.Series:
    """Heurística de choque de horário com obrigatória pendente: compara o
    bloco (diasem+horário) da turma-alvo com os blocos das turmas de
    disciplinas 'O' **pendentes** do semestre ideal do aluno, ofertadas no
    mesmo semestre. Totalmente vetorizado via merge por bloco."""
    res = pd.Series(0, index=df.index, dtype=int)
    ocup = dados.get("ocup")
    grade = dados.get("grade")
    if (
        ocup is None or not len(ocup)
        or grade is None or not len(grade)
        or not len(df) or "sem_casa" not in df.columns
    ):
        return res
    o = ocup[ocup["codtur"].isin(turmas_sem["codtur"])].copy()
    if not len(o):
        return res
    o["bloco"] = o["diasmnocp"].astype(str) + "_" + o["horent"].astype(str)
    blocos_turma = o[["coddis", "codtur", "bloco"]].drop_duplicates()

    # Obrigatórias ofertadas no semestre, com seu semestre ideal.
    obg = grade[grade["tipobg"] == "O"][["coddis", "numsemidl"]].drop_duplicates("coddis")
    obg_ofert = obg.merge(
        turmas_sem[["coddis", "codtur"]], on="coddis", how="inner"
    ).merge(blocos_turma, on=["coddis", "codtur"], how="inner")
    if not len(obg_ofert):
        return res
    obg_ofert["numsemidl"] = pd.to_numeric(obg_ofert["numsemidl"], errors="coerce")
    obg_ofert = obg_ofert.dropna(subset=["numsemidl"])
    if not len(obg_ofert):
        return res
    # Blocos de obrigatória ofertada por numsemidl (long: um por bloco).
    # Bug-fix: traz ``coddis`` da obrigatória (coddis_obg) para permitir
    # filtragem anti-auto-conflito -- uma disciplina não pode dar choque de
    # horário com ela mesma. Antes, sem essa coluna, qualquer matéria
    # obrigatória casava consigo própria na mesma turma (mesmo bloco) e
    # setava a flag, invertendo o sinal de Pearson para +0.51.
    obg_long = obg_ofert[["numsemidl", "bloco", "coddis"]].drop_duplicates()
    obg_long = obg_long.rename(columns={"coddis": "coddis_obg"})

    # Cada linha do df: explode seus blocos (via codtur), junta com sem_casa.
    # Carrega também o ``coddis`` da turma-alvo (coddis_alvo) para a filtragem
    # anti-auto-conflito.
    d = df[["codtur", "sem_casa", "coddis"]].copy()
    d["_idx"] = d.index
    d = d.dropna(subset=["sem_casa"])
    if not len(d):
        return res
    d["sem_casa"] = d["sem_casa"].astype(int)
    d = d.rename(columns={"coddis": "coddis_alvo"}).reindex(
        columns=["codtur", "coddis_alvo", "sem_casa", "_idx"]
    )
    link = blocos_turma[["codtur", "bloco"]].drop_duplicates()
    d = d.merge(link, on="codtur", how="inner")
    if not len(d):
        return res
    # Considera conflito com obrigatórias do sem ideal (sc) e do anterior
    # (sc-1) — matérias "na época" e uma de defasagem. Explodimos os dois
    # alvos de numsemidl por linha.
    targets = pd.concat(
        [
            d.assign(numsemidl=d["sem_casa"]),
            d.assign(numsemidl=d["sem_casa"] - 1),
        ], ignore_index=True,
    )
    hits = targets.merge(obg_long, on=["numsemidl", "bloco"], how="inner")
    if not len(hits):
        return res
    # Bug-fix anti-auto-conflito: exclui choques da disciplina com ela mesma.
    hits = hits[hits["coddis_obg"] != hits["coddis_alvo"]]
    if not len(hits):
        return res
    res.loc[hits["_idx"].unique()] = 1
    return res


# ---------------------------------------------------------------------------
# Módulo E/F: features da turma (reuso de dataset_alocacao)
# ---------------------------------------------------------------------------
def _montar_features_turma(
    cfg: DatasetAlunoConfig, dados: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Reaproveita o ferramental de :mod:`dataset_alocacao` para consolidar as
    features **da turma** (Módulos E e F), por ``(coddis, codtur, ano_sem)``."""
    from .dataset_macrosensores import features_macrosensores

    turmas_f = filtrar_turmas(cfg, dados["turmas"])
    if not len(turmas_f):
        return pd.DataFrame()
    est = reconstruir_estmtr(cfg, turmas_f, dados["hist"])
    t = features_base(cfg, turmas_f, dados)
    t = t.merge(est, on=["coddis", "codtur"], how="left")
    t["estmtr_val"] = t["estmtr_val"].fillna(0).astype(int)
    t = features_historico(cfg, t)
    t = features_demanda(cfg, t, dados["hist"], dados)
    t = features_professor_horario(cfg, t, dados)
    t = features_ingressantes(cfg, t, dados["grade"])
    t = features_sazonalidade(cfg, t)
    t = features_espaco_fase(cfg, t)
    t = features_rede_requisitos(cfg, t, dados["grade"], dados["hist"])
    t = features_concorrencia_horaria(cfg, t, dados)
    t = features_macrosensores(cfg, t, dados)

    # vagas_reais com np.nan se < 20 (não viciar em anomalias de cadastro).
    t["vagas_reais"] = t["vagas_reais"].where(t["vagas_reais"] >= 20, np.nan)

    cols_e = [
        "coddis", "codtur", "ano_sem", "vagas_reais",
        "qtd_turmas_abertas", "pressao_demanda", "hist_taxa_estouro",
        "hist_max_excesso", "net_pagerank", "net_out_degree",
        "pressao_represada", "flag_sexta", "qtd_dias_semana",
        "ind_sincronia_bloco", "flag_fora_de_epoca",
    ]
    # forca_docente = semestres_consecutivos_prof (fidelidade docente-disciplina).
    if "semestres_consecutivos_prof" in t.columns:
        t = t.rename(columns={"semestres_consecutivos_prof": "forca_docente"})
        cols_e.append("forca_docente")
    else:
        t["forca_docente"] = 0
        cols_e.append("forca_docente")

    cols_f = [c for c in t.columns if c.startswith("macro_")]
    cols_e = [c for c in cols_e if c in t.columns]
    return t[cols_e + cols_f].drop_duplicates(["coddis", "codtur", "ano_sem"])


# ---------------------------------------------------------------------------
# Anonimização LGPD
# ---------------------------------------------------------------------------
def _anonimizar_alunos(
    df: pd.DataFrame, mapa: dict[int, str], contador: list[int]
) -> pd.DataFrame:
    """Mapeia ``codpes`` → ``id_aluno`` (``ALU_0001`` ...) mantendo o mesmo ID
    para o mesmo ``codpes`` ao longo dos semestres fatiados."""
    out = df.copy()

    def _mk(v: Any) -> str:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return ""
        key = int(v)
        if key not in mapa:
            contador[0] += 1
            mapa[key] = f"ALU_{contador[0]:04d}"
        return mapa[key]

    out["id_aluno"] = out["codpes"].map(_mk)
    return out


# ---------------------------------------------------------------------------
# Colunas de vazamento (descarte final)
# ---------------------------------------------------------------------------
COLUNAS_VAZAMENTO_ALUNO = [
    "codpes", "nummtr", "nummtr_max", "estmtr", "stamtr", "rstfim",
    "dtacrihst", "dtaultalt", "dtaing", "codpgm", "estmtr_val",
    "dtainitur",
    "ocup_d+0", "ocup_d+7", "ocup_d+11", "ocup_d+14", "ocup_d+21",
]
# Colunas que re-identificam o aluno (codcur/codhab) ou são apenas
# intermediárias das features (já consumidas em delta_semestre_ideal /
# perc_conclusao_aprovado / taxa_sucesso_historica). Não devem virar feature.
COLUNAS_DESCARTE_ALUNO = [
    "codcur", "codhab", "min_numsemidl", "max_carga_total", "sem_casa",
    "creditos_aprovados", "qtd_aprovadas", "qtd_cursadas",
]


def _saida_com_flags(cfg: DatasetAlunoConfig) -> Path:
    """Caminho de saída com sufixo indicando as flags exploratórias ativas,
    para **nunca sobrescrever** o dataset baseline.

    Sufixos curtos (decisão 6): ``_sf`` (excluir_fantasmas), ``_bl``
    (balancear_l), ``_sf_bl`` (ambas). Com nenhuma flag ativa, retorna o
    caminho original sem alteração — o baseline fica bit-identico.
    """
    sufixo = ""
    if cfg.excluir_fantasmas:
        sufixo += "_sf"
    if cfg.balancear_l:
        sufixo += "_bl"
    if not sufixo:
        return cfg.saida
    return cfg.saida.with_name(f"{cfg.saida.stem}{sufixo}{cfg.saida.suffix}")


# ---------------------------------------------------------------------------
# Montagem final
# ---------------------------------------------------------------------------
def montar_dataset_aluno(
    cfg: DatasetAlunoConfig | None = None,
    forcar_extracao: bool = False,
) -> pd.DataFrame:
    """Constrói o DataFrame mestre (Aluno × Turma) com features + alvo.

    Sai em ``cfg.saida`` (CSV). Retorna o DataFrame.
    """
    cfg = cfg or DatasetAlunoConfig.from_env()
    saida_efetiva = _saida_com_flags(cfg)
    print(
        f"=== DatasetAlunoConfig ===\n  codundclg: {cfg.codundclg}\n  prefixos: {cfg.prefixos}\n"
        f"  anos: {cfg.ano_min}-{cfg.ano_max}\n  horizonte_sem: {cfg.horizonte_sem}\n"
        f"  excluir_fantasmas: {cfg.excluir_fantasmas}\n  balancear_l: {cfg.balancear_l}\n"
        f"  saida: {saida_efetiva}"
    )
    dados = carregar_dados_aluno(cfg, forcar=forcar_extracao)

    # Features da turma (Módulos E e F) — uma vez para todos os semestres.
    turmas_feat = _montar_features_turma(cfg, dados)
    print(f"Features de turma consolidadas: {len(turmas_feat)} linhas")

    # Laço por semestre alvo — streaming direto para o CSV para não segurar
    # ~20M linhas em RAM (catástrofe de memória no concat final).
    turmas_f = filtrar_turmas(cfg, dados["turmas"])
    sems = sorted(turmas_f["ano_sem"].unique())
    mapa_aluno: dict[int, str] = {}
    cont_aluno: list[int] = [0]

    col_id = ["id_aluno", "coddis", "codtur", "sufixo", "ano_sem", "sem_tipo"]
    descarte = set(COLUNAS_VAZAMENTO_ALUNO) | set(COLUNAS_DESCARTE_ALUNO)

    saida_efetiva.parent.mkdir(parents=True, exist_ok=True)
    n_linhas = 0
    n_pos = 0
    header_written = False
    final_cols: list[str] = []
    with tqdm(sems, desc="Semestres", unit="sem") as bar:
        for sem_alvo in bar:
            turmas_sem = turmas_f[turmas_f["ano_sem"] == sem_alvo]
            base = _build_matriz_sem(cfg, dados, int(sem_alvo), turmas_sem)
            if not len(base):
                continue
            base["ano_sem"] = int(sem_alvo)

            # Módulo B (pré-requisitos).
            dta_corte = base["dtainitur"].min() - pd.Timedelta(days=cfg.dias_corte)
            base = _status_prerequisito_aluno(
                base, cfg, dados, int(sem_alvo), dta_corte
            )

            # Módulo D (atrito).
            base = _modulo_d_atrito(base, cfg, dados, turmas_sem)

            # Módulos E/F (features da turma).
            if len(turmas_feat):
                base = base.merge(
                    turmas_feat, on=["coddis", "codtur", "ano_sem"], how="left"
                )

            # Anonimização LGPD (por semestre — mapa persistente).
            base = _anonimizar_alunos(base, mapa_aluno, cont_aluno)

            # Seleção de colunas + descarte de vazamento/intermediárias.
            if not final_cols:
                feat_cols = [
                    c for c in base.columns
                    if c not in col_id and c not in descarte
                    and c != "alvo_matriculado"
                ]
                final_cols = col_id + feat_cols + ["alvo_matriculado"]
                # Remove qualquer coluna de vazamento que tenha espiado
                # (defensivo — já não devem existir).
                final_cols = [c for c in final_cols if c not in descarte]
            keep = [c for c in final_cols if c in base.columns]
            base = base[keep]

            # Escreve chunk: header só na 1ª fatia, append senão.
            base.to_csv(saida_efetiva, mode='a' if header_written else 'w', index=False, header=(not header_written))
            header_written = True
            n_linhas += len(base)
            if "alvo_matriculado" in base.columns:
                n_pos += int(base["alvo_matriculado"].sum())

    if not header_written:
        print("[aviso] Nenhuma linha produzida — verifique cache/escopo.")
        return pd.DataFrame(columns=col_id + ["alvo_matriculado"])

    print(
        f"\nDataset salvo em {saida_efetiva} "
        f"({n_linhas} linhas, {len(final_cols)} colunas, pos={n_pos} "
        f"neg={n_linhas - n_pos})"
    )
    # Leitura leve para retorno (sample inicial) — opcional.
    df = pd.read_csv(saida_efetiva, nrows=1000)
    return df


def main(argv: Iterable[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Fornecedor de dataset aluno × turma")
    p.add_argument(
        "--codundclg", type=int, default=None,
        help="Colegiado/unidade (obrigatório se REPLICADO_CODUNDCLG ausente)",
    )
    p.add_argument("--prefixos", nargs="+", default=None)
    p.add_argument("--sufixo-min", type=int, default=None)
    p.add_argument("--ano-min", type=int, default=None)
    p.add_argument("--ano-max", type=int, default=None)
    p.add_argument("--horizonte-sem", type=int, default=None)
    p.add_argument("--janela-intencao", type=int, default=None)
    p.add_argument(
        "--max-neg-disc", type=int, default=None,
        help="Máx. de turmas negativas por (aluno, disciplina)",
    )
    p.add_argument(
        "--sem-intencao-requerimento", dest="usar_intencao_requerimento",
        action="store_false", help="Desativa candidatos via REQUERIMENTOGR",
    )
    p.add_argument("--forcar-extracao", action="store_true")
    p.add_argument("--saida", type=Path, default=None)
    p.add_argument(
        "--excluir-fantasmas", dest="excluir_fantasmas", action="store_true",
        help="Exclui alunos com zero matrículas no semestre dos negativos "
             "(sufixo _sf; selection-on-outcome para análise de inferência)",
    )
    p.add_argument(
        "--balancear-l", dest="balancear_l", action="store_true",
        help="Amostra negativos L entre alunos com ≥1 matrícula até a razão "
             "média O/E/C (sufixo _bl)",
    )
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
    if args.horizonte_sem is not None:
        overrides["horizonte_sem"] = args.horizonte_sem
    if args.janela_intencao is not None:
        overrides["janela_intencao_dias"] = args.janela_intencao
    if args.max_neg_disc is not None:
        overrides["max_neg_turmas_por_disc"] = args.max_neg_disc
    overrides["usar_intencao_requerimento"] = args.usar_intencao_requerimento
    overrides["excluir_fantasmas"] = args.excluir_fantasmas
    overrides["balancear_l"] = args.balancear_l
    if args.saida is not None:
        overrides["saida"] = args.saida

    cfg = DatasetAlunoConfig.from_env(**overrides)
    montar_dataset_aluno(cfg, forcar_extracao=args.forcar_extracao)
    return 0


__all__ = [
    "DatasetAlunoConfig",
    "carregar_dados_aluno",
    "montar_dataset_aluno",
    "main",
]

if __name__ == "__main__":
    sys.exit(main())
