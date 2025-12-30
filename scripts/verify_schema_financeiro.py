import os
import sys

from dotenv import load_dotenv

# Add the project root to the python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from replicado.connection import DB


def verify_financeiro_schema_expanded() -> None:
    load_dotenv()

    # Dict of table: [fields]
    schema_to_check = {
        # Patrimônio / Bens
        "BEM": ["codbem", "coditmmat", "tipbem", "nomuslbem"],
        "BEMPATRIMONIADO": [
            "numpat",
            "codbem",
            "stabem",
            "codlocusp",
            "codunddsp",
            "sglcendsp",
            "tippatusp",
            "vlroribem",
            "dtacad",
            "estcsrbem",
            "staconutlbem",
        ],
        "BEMCONVENIO": ["numpat", "codcvn"],
        "BEMDOADO": ["numpat", "codorg", "nompes"],
        "DESCRBEM": ["codbem", "nomcaritmmat", "vlrcaritmmat"],
        # Estrutura / Centro de Despesa
        "CENTRODESPHIERARQUIA": [
            "codunddsp",
            "sglcendsp",
            "codhiecendsp",
            "etrhie",
            "nomcendsp",
            "dtadtv",
            "ordvrthie",
        ],
        "LOCALUSP": ["codlocusp", "codund", "tiplocusp", "stiloc", "idfloc"],
        # Almoxarifado / Estoque
        "ESTOQUE": ["codunddsp", "codbem", "qtdatl", "qtdmin", "qtdmax", "prcmed"],
        "CLASSIFITEMMAT": [
            "coditmmat",
            "tipitmmat",
            "nomgrpitmmat",
            "nomsgpitmmat",
            "dtafimmat",
        ],
        # Convênios / Organizações
        "CONVENIO": [
            "codcvn",
            "tipcvn",
            "nomcvn",
            "stacvn",
            "dtaasicvn",
            "dtadtvcvn",
            "vlrtotcvn",
        ],
        "CONVORGAN": ["codorg", "codcvn", "stafnd", "codptporg"],
        "ORGANIZACAO": ["codorg", "nomrazsoc", "idfcgcorg", "sglorg", "tiporg"],
    }

    print(
        f"Verificando {len(schema_to_check)} tabelas para o módulo 'financeiro' "
        "(Expandido)..."
    )

    results = {}

    for table, fields in schema_to_check.items():
        results[table] = {"exists": False, "missing_fields": []}
        try:
            # Check table
            DB.fetch_all(f"SELECT TOP 1 1 FROM {table}")
            results[table]["exists"] = True
            print(f"[OK] Tabela '{table}' encontrada.")

            # Check fields
            for field in fields:
                try:
                    DB.fetch_all(f"SELECT TOP 1 {field} FROM {table}")
                except Exception:
                    results[table]["missing_fields"].append(field)
                    print(
                        f"    [ERRO] Campo '{field}' NÃO encontrado na tabela "
                        f"'{table}'."
                    )
        except Exception:
            print(f"[ERRO] Tabela '{table}' NÃO encontrada.")

    print("\n" + "=" * 50)
    print("RESUMO DO DIAGNÓSTICO EXPANDIDO")
    print("=" * 50)

    found_count = sum(1 for t in results.values() if t["exists"])
    print(f"Tabelas encontradas: {found_count}/{len(schema_to_check)}")

    inconsistencies = []
    for table, status in results.items():
        if not status["exists"]:
            inconsistencies.append(f"Tabela {table} inexistente.")
        elif status["missing_fields"]:
            inconsistencies.append(
                f"Tabela {table} sem os campos: {', '.join(status['missing_fields'])}"
            )

    if inconsistencies:
        print("\nInconsistências Detectadas:")
        for inc in inconsistencies:
            print(f"- {inc}")
    else:
        print("\nNenhuma inconsistência detectada no esquema proposto!")


if __name__ == "__main__":
    verify_financeiro_schema_expanded()
