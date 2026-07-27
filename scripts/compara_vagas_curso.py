"""Compara fontes de vagas por curso (codcur) no Replicado.

Fontes avaliadas:
  1. CURSOGR.totvag               -> total de vagas registrado no cadastro do curso
  2. HABILITACAOGR.numvaghab + ...-> soma das vagas das habilitacoes ativas do curso
  3. HABILVAGA.totvagofe          -> vagas oferecidas por ano (referencia o anoofe)
  4. HISTCURSOGR.totvag           -> total historico (cursos encerrados: dtafimcur set)

Foco: codclg lido de ``REPLICADO_CODUNDCLG`` (``.env``). Executar via tunnel SSH.
"""

import os
import sys

import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from replicado.connection import DB

load_dotenv()
_cod_env = os.getenv("REPLICADO_CODUNDCLG")
if not _cod_env or not _cod_env.strip():
    raise ValueError(
        "REPLICADO_CODUNDCLG não definido no .env —informe o código do "
        "colegiado/unidade (ex.: 45 para o IME)."
    )
CODCLG = int(_cod_env)


def fetch_habilitacao(codclg: int) -> pd.DataFrame:
    sql = f"""
    SELECT H.codcur, H.codhab, H.nomhab,
           H.numvaghab, H.numvaghabcpl, H.numvaghabcvn,
           H.dtaatvhab, H.dtadtvhab
    FROM HABILITACAOGR H
    INNER JOIN CURSOGR C ON C.codcur = H.codcur
    WHERE C.codclg = {codclg}
    """
    rows = DB.fetch_all(sql)
    df = pd.DataFrame(rows)
    for c in ["numvaghab", "numvaghabcpl", "numvaghabcvn"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["vag_hab_total"] = df["numvaghab"] + df["numvaghabcpl"] + df["numvaghabcvn"]
    df["ativa"] = df["dtaatvhab"].notna() & df["dtadtvhab"].isna()
    return df


def fetch_cursogr(codclg: int) -> pd.DataFrame:
    sql = f"SELECT codcur, nomcur, totvag, dtaatvcur, dtadtvcur FROM CURSOGR WHERE codclg = {codclg}"
    df = pd.DataFrame(DB.fetch_all(sql))
    df["totvag"] = pd.to_numeric(df["totvag"], errors="coerce")
    return df


def fetch_habilvaga(codclg: int) -> pd.DataFrame:
    sql = f"""
    SELECT HV.codcur, HV.codhab, HV.anoofe, HV.totvagofe
    FROM HABILVAGA HV
    INNER JOIN CURSOGR C ON C.codcur = HV.codcur
    WHERE C.codclg = {codclg}
    """
    df = pd.DataFrame(DB.fetch_all(sql))
    df["totvagofe"] = pd.to_numeric(df["totvagofe"], errors="coerce").fillna(0)
    df["anoofe"] = pd.to_numeric(df["anoofe"], errors="coerce")
    return df


def fetch_histcursogr(codclg: int) -> pd.DataFrame:
    sql = f"""
    SELECT HC.codcur, HC.nomcur, HC.totvag, HC.dtainicur, HC.dtafimcur
    FROM HISTCURSOGR HC
    INNER JOIN CURSOGR C ON C.codcur = HC.codcur
    WHERE C.codclg = {codclg}
    """
    df = pd.DataFrame(DB.fetch_all(sql))
    df["totvag"] = pd.to_numeric(df["totvag"], errors="coerce")
    return df


def main() -> None:
    load_dotenv()

    hab = fetch_habilitacao(CODCLG)
    cur = fetch_cursogr(CODCLG)
    hv = fetch_habilvaga(CODCLG)
    hc = fetch_histcursogr(CODCLG)

    print("=" * 90)
    print(f"CURSOS codclg={CODCLG} em CURSOGR: {len(cur)} linhas")
    print("=" * 90)

    # Soma das habilitacoes ativas por curso
    hab_ativa = (
        hab[hab["ativa"]]
        .groupby("codcur", as_index=False)
        .agg(vag_hab_ativa=("vag_hab_total", "sum"), n_hab_ativas=("codhab", "count"))
    )
    # Soma de TODAS as habilitacoes (ativo + encerrado)
    hab_all = hab.groupby("codcur", as_index=False).agg(
        vag_hab_todas=("vag_hab_total", "sum"), n_hab_total=("codhab", "count")
    )

    # habilitacao detalhe
    print("\n--- HABILITACAOGR (detalhe por codhab) ---")
    show = hab[
        [
            "codcur",
            "codhab",
            "nomhab",
            "numvaghab",
            "numvaghabcpl",
            "numvaghabcvn",
            "vag_hab_total",
            "ativa",
        ]
    ]
    print(show.to_string(index=False))

    # HABILVAGA: ano mais recente por curso
    ano_max = int(hv["anoofe"].max()) if not hv.empty else None
    hv_recente = (
        (
            hv[hv["anoofe"] == ano_max]
            .groupby("codcur", as_index=False)
            .agg(vag_ofe_recente=("totvagofe", "sum"), n_hab_ofe=("codhab", "count"))
        )
        if ano_max
        else pd.DataFrame(columns=["codcur", "vag_ofe_recente", "n_hab_ofe"])
    )
    # Serie historica por curso (soma por ano)
    print(f"\n--- HABILVAGA totvagofe por anoofe (ano_max={ano_max}) ---")
    serie = hv.groupby(["anoofe", "codcur"], as_index=False)["totvagofe"].sum()
    print(serie.to_string(index=False))

    # Comparativo lado a lado
    cmp = cur[["codcur", "nomcur", "totvag"]].copy()
    cmp = cmp.merge(hab_ativa, on="codcur", how="left")
    cmp = cmp.merge(hab_all, on="codcur", how="left")
    cmp = cmp.merge(hv_recente, on="codcur", how="left")
    cmp["vag_hab_ativa"] = cmp["vag_hab_ativa"].fillna(-1).astype(int)
    cmp["vag_hab_todas"] = cmp["vag_hab_todas"].fillna(-1).astype(int)
    cmp["vag_ofe_recente"] = cmp["vag_ofe_recente"].fillna(-1).astype(int)

    print("\n" + "=" * 90)
    print(
        "COMPARATIVO (totvag CURSOGR | sum habilitacoes ativas | todas | totvagofe ano recente)"
    )
    print("=" * 90)
    print(cmp.to_string(index=False))

    # Divergencias
    print("\n" + "=" * 90)
    print("DIVERGENCIAS (totvag != vag_hab_ativa)")
    print("=" * 90)
    div = cmp[(cmp["totvag"] != cmp["vag_hab_ativa"]) & (cmp["vag_hab_ativa"] >= 0)]
    print(div.to_string(index=False) if not div.empty else "(nenhuma)")

    print("\n" + "=" * 90)
    print("HISTCURSOGR (cursos historicos - geralmente encerrados)")
    print("=" * 90)
    print(hc.to_string(index=False) if not hc.empty else "(vazio)")

    # Saving
    out = "temp/compara_vagas_curso.csv"
    cmp.to_csv(out, index=False)
    print(f"\nComparativo salvo em {out}")
    hab_out = "temp/compara_vagas_habilitacao.csv"
    hab.to_csv(hab_out, index=False)
    print(f"Detalhe de habilitacoes salvo em {hab_out}")
    serie_out = "temp/compara_vagas_habilvaga_serie.csv"
    serie.to_csv(serie_out, index=False)
    print(f"Serie HABILVAGA salva em {serie_out}")


if __name__ == "__main__":
    main()
