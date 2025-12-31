import os
import sys
from sqlalchemy import text

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from replicado.connection import DB

def list_tables():
    print("Listando tabelas e views disponíveis...")
    try:
        # Para SQL Server / Sybase
        query = "SELECT name, type FROM sysobjects WHERE type IN ('U', 'V') ORDER BY name"
        results = DB.fetch_all(query)
        for row in results:
            name = row.get('name')
            type_obj = row.get('type')
            if "CITACAO" in name.upper() or "QUALIS" in name.upper() or "PROD" in name.upper():
                print(f"ENCONTRADA ({type_obj}): {name}")
            # else:
            #    print(name)
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    list_tables()
