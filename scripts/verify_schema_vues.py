import os
import sys

from dotenv import load_dotenv

# Add the project root to the python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from replicado.connection import DB


def verify_vues_tables():
    load_dotenv()

    # List of tables from vues___visão_de_replicação_padrão_estrutura.json
    tables_to_check = [
        "AREAATUACAOORG",
        "AREAATUACAOSETOR",
        "CALENDLOCALIDADEUSP",
        "CAMPUS",
        "COLEGIADO",
        "CONTINENTE",
        "ENDORGAN",
        "ENDUSP",
        "ENDUSPCOLEG",
        "ENDUSPSETOR",
        "ESTADO",
        "IDIOMA",
        "LOCALIDADE",
        "LOCALUSP",
        "PAIS",
        "PAISCONTINENTE",
        "PAISIDIOMA",
        "SETOR",
        "SETORCOLEG",
        "TABCBO",
        "TIPOCOLEGIADO",
        "TIPOESTRUTURACOLEG",
        "TIPOFUNCAOCOLEG",
        "TIPOOCORCALEND",
        "TIPOORGANIZACAO",
        "TIPOSETOR",
        "TIPOUNIDADE",
        "TRANSFPAIS",
        "TRANSFUNID",
        "UNIDADE",
        "UNIDADECAMPUSCOMPL",
        "UNIDADEFISCAL",
        "UNIDCOLEG",
        "URLSETOR",
        "URLUNIDADE",
    ]

    print(f"Verifying {len(tables_to_check)} tables from 'vues' schema...")

    existing_tables = []
    missing_tables = []

    for table in tables_to_check:
        try:
            # Try a simple SELECT 1 to check if table exists
            query = f"SELECT TOP 1 1 FROM {table}"
            DB.fetch_all(query)
            existing_tables.append(table)
            print(f"[OK] Table '{table}' found.")
        except Exception:
            missing_tables.append(table)
            print(f"[MISSING] Table '{table}' NOT found.")

    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Total tables checked: {len(tables_to_check)}")
    print(f"Found: {len(existing_tables)}")
    print(f"Missing: {len(missing_tables)}")

    if missing_tables:
        print("\nMissing Tables:")
        for t in missing_tables:
            print(f"- {t}")


if __name__ == "__main__":
    verify_vues_tables()
