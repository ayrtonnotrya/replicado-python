import os
import sys

from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.getcwd())

load_dotenv()

from replicado.connection import DB


def check_table(table_name):
    print(f"--- Checking table: {table_name} ---")

    # Check if table exists
    check_query = (
        f"SELECT name FROM sysobjects WHERE name = '{table_name}' AND type = 'U'"
    )
    try:
        exists = DB.fetch_all(check_query)
        if not exists:
            print(f"TABLE NOT FOUND: {table_name}")
            return
    except Exception as e:
        print(f"Error checking existence of {table_name}: {e}")
        return

    # Get columns
    query = f"""
        SELECT c.name as column_name, t.name as data_type
        FROM syscolumns c
        JOIN systypes t ON c.usertype = t.usertype
        WHERE c.id = object_id('{table_name}')
        ORDER BY c.name
    """
    try:
        result = DB.fetch_all(query)
        print(f"Found {len(result)} columns.")

    except Exception as e:
        print(f"Error listing columns for {table_name}: {e}")


if __name__ == "__main__":
    tables_to_check = [
        "CONVENIO",
        "ADITIVOCONV",
        "TIPOESTADOCONV",
        "SUBTIPOCONVENIO",
        "TIPOMOEDA",
        "TIPOUNIDTEMPO",
        "RESPCONVSERV",
        "RESPCONVORGAN",
        "CONVUNIDDESP",
        "TIPORESPONSABCONV",
        "ORGANIZACAO",
    ]

    for t in tables_to_check:
        check_table(t)
