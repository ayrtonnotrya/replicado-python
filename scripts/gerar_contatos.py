"""
Gera um CSV com número USP, nome (social quando disponível) e e-mail principal
de todas as pessoas ativas da unidade configurada (REPLICADO_CODUNDCLG):
alunos de graduação, pós-graduação, cultura/extensão e especiais, funcionários
e docentes.

Regras:
- "Ativo" = LOCALIZAPESSOA.sitatl = 'A' na unidade.
- Nome = nome social (PESSOA.nomcnhpes) quando stautlnomsoc = 'S', senão nompes.
- E-mail = principal (EMAILPESSOA.stamtr = 'S'); se ninguém estiver marcado
  como principal, usa um e-mail do domínio USP (stausp = 'S').

Execução: poetry run python scripts/gerar_contatos.py
"""
import logging
import os

import pandas as pd

from replicado.connection import DB

logging.basicConfig(level=logging.INFO)

# Categorias de vínculo (LOCALIZAPESSOA.tipvinext) que entram no CSV.
VINCULOS_ALUNO = [
    "Aluno de Graduação",
    "Aluno de Pós-Graduação",
    "Aluno de Cultura e Extensão",
    "Aluno Especial de Graduação",
    "Aluno Especial Pós-Graduação",
]
VINCULOS_FUNCIONARIO = ["Servidor"]
VINCULOS_DOCENTE = ["Docente"]

CATEGORIAS = VINCULOS_ALUNO + VINCULOS_FUNCIONARIO + VINCULOS_DOCENTE

SAIDA = os.getenv("REPLICADO_CONTATOS_SAIDA", "temp/contatos_ativos.csv")


def _chunk(it, size):
    it = list(it)
    for i in range(0, len(it), size):
        yield it[i : i + size]


def codpes_vinculos(codundclg: int) -> list[dict]:
    """Lista (codpes, tipvinext) dos vínculos ativos da unidade nas categorias."""
    if not CATEGORIAS:
        return []
    lista = "(" + ",".join("'" + c + "'" for c in CATEGORIAS) + ")"
    query = f"""
        SELECT DISTINCT codpes, tipvinext
        FROM LOCALIZAPESSOA
        WHERE codundclg = :codundclg
          AND sitatl = 'A'
          AND tipvinext IN {lista}
    """
    return DB.fetch_all(query, {"codundclg": codundclg})


def dados_nome(codpes_list: list[int]) -> dict[int, str]:
    """codpes -> nome tratado (social se autorizado, senão nome oficial)."""
    nomes: dict[int, str] = {}
    for chunck in _chunk(codpes_list, 900):
        ids = ",".join(str(c) for c in chunck)
        rows = DB.fetch_all(
            f"SELECT codpes, nompes, nomcnhpes, stautlnomsoc "
            f"FROM PESSOA WHERE codpes IN ({ids})"
        )
        for r in rows:
            if r["stautlnomsoc"] == "S" and r["nomcnhpes"]:
                nomes[r["codpes"]] = r["nomcnhpes"]
            else:
                nomes[r["codpes"]] = r["nompes"]
    return nomes


def dados_email(codpes_list: list[int]) -> dict[int, str]:
    """codpes -> e-mail principal; se não houver principal, um e-mail @usp.br."""
    principais: dict[int, str] = {}
    usp: dict[int, str] = {}
    for chunck in _chunk(codpes_list, 900):
        ids = ",".join(str(c) for c in chunck)
        rows = DB.fetch_all(
            f"SELECT codpes, codema, stamtr, stausp "
            f"FROM EMAILPESSOA WHERE codpes IN ({ids})"
        )
        for r in rows:
            cod = r["codpes"]
            if r["stamtr"] == "S" and cod not in principais:
                principais[cod] = r["codema"]
            if r["stausp"] == "S" and cod not in usp:
                usp[cod] = r["codema"]

    emails: dict[int, str] = {}
    for cod in codpes_list:
        emails[cod] = principais.get(cod) or usp.get(cod)
    return emails


def main():
    codundclg = int(os.getenv("REPLICADO_CODUNDCLG"))
    vinculos = codpes_vinculos(codundclg)

    por_pessoa: dict[int, list[str]] = {}
    for r in vinculos:
        por_pessoa.setdefault(r["codpes"], []).append(r["tipvinext"])

    codpes_list = list(por_pessoa.keys())
    logging.info("Pessoas ativas na unidade %s: %d", codundclg, len(codpes_list))

    nomes = dados_nome(codpes_list)
    emails = dados_email(codpes_list)

    registros = []
    for cod in codpes_list:
        registros.append(
            {
                "numero_usp": cod,
                "nome": nomes.get(cod),
                "email": emails.get(cod),
                "categorias": "; ".join(sorted(set(por_pessoa[cod]))),
            }
        )

    df = pd.DataFrame(registros)
    df = df.sort_values("numero_usp").reset_index(drop=True)
    df.to_csv(SAIDA, index=False, encoding="utf-8")

    logging.info("Salvo %d registros em %s", len(df), SAIDA)


if __name__ == "__main__":
    main()
