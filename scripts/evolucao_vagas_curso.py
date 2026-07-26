"""Analisa a evolucao temporal de vagas por codcur (IME, codclg=45).

Perguntas:
  1. O numero de vagas mudou de 2010 para ca (HABILVAGA por anoofe)?
  2. Algum curso so mudou de nome ou de codcur (CURSOGR x HISTCURSOGR)?
  3. A soma dos codhab para um dado codcur eh relativamente constante no tempo?

As duas fontes sao confrontadas:
  - HABILVAGA.totvagofe (operacional, por anoofe + codcur + codhab)
  - HABILITACAOGR.{numvaghab,numvaghabcpl,numvaghabcvn} (cadastral, com
    dtaatvhab/dtadtvhab que delimitam janelas de vigencia)
"""

import os
import sys

import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from replicado.connection import DB

CODCLG = 45


def main() -> None:
    load_dotenv()

    # 1) HABILVAGA: serie operacional por (anoofe, codcur)
    hv = pd.DataFrame(
        DB.fetch_all(f"""
        SELECT HV.anoofe, HV.codcur, HV.codhab, HV.totvagofe
        FROM HABILVAGA HV
        INNER JOIN CURSOGR C ON C.codcur = HV.codcur
        WHERE C.codclg = {CODCLG}
    """)
    )
    hv["totvagofe"] = pd.to_numeric(hv["totvagofe"], errors="coerce").fillna(0)
    hv["anoofe"] = pd.to_numeric(hv["anoofe"], errors="coerce")
    hv = hv.dropna(subset=["anoofe"])
    hv["anoofe"] = hv["anoofe"].astype(int)

    # 2) HABILITACAOGR cadastral com datas de vigencia
    hab = pd.DataFrame(
        DB.fetch_all(f"""
        SELECT H.codcur, H.codhab, H.nomhab,
               H.numvaghab, H.numvaghabcpl, H.numvaghabcvn,
               H.dtaatvhab, H.dtadtvhab
        FROM HABILITACAOGR H
        INNER JOIN CURSOGR C ON C.codcur = H.codcur
        WHERE C.codclg = {CODCLG}
    """)
    )
    for c in ["numvaghab", "numvaghabcpl", "numvaghabcvn"]:
        hab[c] = pd.to_numeric(hab[c], errors="coerce").fillna(0)
    hab["vag_total"] = hab["numvaghab"] + hab["numvaghabcpl"] + hab["numvaghabcvn"]
    hab["dtaatvhab"] = pd.to_datetime(hab["dtaatvhab"], errors="coerce")
    hab["dtadtvhab"] = pd.to_datetime(hab["dtadtvhab"], errors="coerce")

    # 3) CURSOGR + HISTCURSOGR: deteccao de mudanca de nome / codcur
    cur = pd.DataFrame(
        DB.fetch_all(f"""
        SELECT codcur, nomcur, totvag, dtaatvcur, dtadtvcur
        FROM CURSOGR WHERE codclg = {CODCLG}
    """)
    )
    hc = pd.DataFrame(
        DB.fetch_all(f"""
        SELECT HC.codcur, HC.nomcur, HC.totvag, HC.dtainicur, HC.dtafimcur
        FROM HISTCURSOGR HC
        INNER JOIN CURSOGR C ON C.codcur = HC.codcur
        WHERE C.codclg = {CODCLG}
    """)
    )

    print("=" * 90)
    print("P2: cursos que so aparecem no HISTCURSOGR (possivel mudanca de codcur)")
    print("=" * 90)
    hc_only = hc[~hc["codcur"].isin(cur["codcur"])]
    print(hc_only.to_string(index=False) if len(hc_only) else "(nenhum)")
    # nomes divergentes entre CURSOGR e HISTCURSOGR para o mesmo codcur
    nomes = cur[["codcur", "nomcur"]].merge(
        hc[["codcur", "nomcur"]].rename(columns={"nomcur": "nomcur_hist"}),
        on="codcur",
        how="outer",
    )
    diver = nomes[
        (nomes["nomcur"].notna())
        & (nomes["nomcur_hist"].notna())
        & (nomes["nomcur"].str.lower() != nomes["nomcur_hist"].str.lower())
    ]
    print("\nNomes divergentes CURSOGR vs HISTCURSOGR (mesmo codcur):")
    print(diver.to_string(index=False) if len(diver) else "(nenhum)")

    # ============ P1: evolucao HABILVAGA por codcur (soma anual dos codhab) =====
    serie = (
        hv.groupby(["anoofe", "codcur"], as_index=False)["totvagofe"]
        .sum()
        .sort_values(["codcur", "anoofe"])
    )
    print("\n" + "=" * 90)
    print("P1: serie HABILVAGA totvagofe (soma de codhab) por anoofe")
    print("=" * 90)
    print(serie.to_string(index=False))

    # pivot por codcur: variacao ao longo do tempo (min/max/coef. var.)
    piv = serie.pivot(index="anoofe", columns="codcur", values="totvagofe")
    print("\n" + "=" * 90)
    print("Pivot anoofe x codcur (totvagofe):")
    print("=" * 90)
    print(piv.to_string())

    print("\nEstatisticas por codcur (constancia temporal):")
    stat = pd.DataFrame(
        {
            "min": piv.min(),
            "max": piv.max(),
            "media": piv.mean(),
            "desvio": piv.std(),
            "cv": piv.std() / piv.mean(),
            "n_anos": piv.count(),
            "n_valores_distintos": piv.nunique(),
        }
    ).fillna(0)
    print(stat.to_string())

    # ============ P3: soma dos codhab eh constante? (por anoofe) ============
    print("\n" + "=" * 90)
    print("P3: cada codcur tem o mesmo total em TODOS os anos? (HABILVAGA)")
    print("=" * 90)
    for codcur, g in serie.groupby("codcur"):
        vals = sorted(g["totvagofe"].unique())
        flag = "CONSTANTE" if len(vals) == 1 else f"VARIA ({len(vals)} valores)"
        print(
            f"  codcur {codcur}: {g['anoofe'].min()}-{g['anoofe'].max()}"
            f" | {flag} | valores={vals}"
        )

    # ============ HABILVAGA: granularidade codhab ao longo do tempo ===========
    print("\n" + "=" * 90)
    print("HABILVAGA: codhab usados por codcur ao longo do tempo")
    print("=" * 90)
    for codcur, g in hv.groupby("codcur"):
        print(f"\ncodcur {codcur}:")
        print(g.sort_values(["anoofe", "codhab"]).to_string(index=False))

    # ============ HABILITACAOGR cadastral: reconstroi multidao ativa ano-a-ano
    print("\n" + "=" * 90)
    print("HABILITACAOGR: vagas cadastrais ATIVAS por ano (reconstrucao via datas)")
    print("=" * 90)
    anos = range(2010, 2026)
    recon = {}
    for ano in anos:
        corte = pd.Timestamp(year=ano, month=7, day=1)
        ativa = hab[
            (hab["dtaatvhab"].isna() | (hab["dtaatvhab"] <= corte))
            & (hab["dtadtvhab"].isna() | (hab["dtadtvhab"] > corte))
        ]
        # SOMA soh do codhab principal (codhab = 0) — evita dupla contagem
        princ = ativa[ativa["codhab"] == 0]
        tot_princ = princ.groupby("codcur")["vag_total"].sum()
        # SOMA de TODOS os codhab ativos (para comparar com a interpretacao
        # "soma de tudo")
        tot_todos = ativa.groupby("codcur")["vag_total"].sum()
        recon[ano] = pd.DataFrame(
            {
                "vag_cad_princ": tot_princ,
                "vag_cad_todos": tot_todos,
            }
        )
    recon_df = pd.concat(recon, names=["ano", "codcur"]).reset_index()
    print("\nVagas cadastrais (codhab=0) por ano e codcur:")
    piv_princ = recon_df.pivot(index="ano", columns="codcur", values="vag_cad_princ")
    print(piv_princ.to_string())
    print("\nVagas cadastrais (soma TODOS codhab ativos) por ano e codcur:")
    piv_todos = recon_df.pivot(index="ano", columns="codcur", values="vag_cad_todos")
    print(piv_todos.to_string())

    # Comparacao final HABILVAGA vs HABILITACAOGR(codhab=0)
    print("\n" + "=" * 90)
    print("COMPARACAO: HABILVAGA (ano recente) vs HABILITACAOGR codhab=0 ativa")
    print("=" * 90)
    ano_ref = int(hv["anoofe"].max())
    hv_ref = (
        hv[hv["anoofe"] == ano_ref]
        .groupby("codcur", as_index=False)["totvagofe"]
        .sum()
        .rename(columns={"totvagofe": f"habilvaga_{ano_ref}"})
    )
    cad_ref = recon_df[recon_df["ano"] == ano_ref][
        ["codcur", "vag_cad_princ", "vag_cad_todos"]
    ].rename(columns={"vag_cad_princ": "cad_princ", "vag_cad_todos": "cad_todos"})
    comp = hv_ref.merge(cad_ref, on="codcur", how="outer")
    comp = comp.merge(cur[["codcur", "nomcur"]], on="codcur", how="left")
    print(comp.to_string(index=False))

    # Salva
    serie.to_csv("temp/vagas_serie_habilvaga.csv", index=False)
    recon_df.to_csv("temp/vagas_recon_habilitacao.csv", index=False)
    comp.to_csv("temp/vagas_comparativo.csv", index=False)
    print(
        "\nSaidas: temp/vagas_serie_habilvaga.csv,"
        " temp/vagas_recon_habilitacao.csv, temp/vagas_comparativo.csv"
    )


if __name__ == "__main__":
    main()
