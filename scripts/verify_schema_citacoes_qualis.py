import os
import sys
from sqlalchemy import text

# Adiciona o diretório raiz ao path para importar o replicado
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from replicado.connection import DB

def verify_schema():
    
    tables_to_verify = {
        "CITACAOPESSOA": [
            "codpes", "idcfteifm", "idfpesfte", "qtdpdupes", "qtdrefpdupes", 
            "qtdtotcitpes", "qtddoccit", "clsinh", "clsi10", "nompesfteifm"
        ],
        "CITACAOPESSOAANUAL": [
            "codpes", "idcfteifm", "anoref", "qtdcitpes"
        ],
        "QUALISPERIODICO": [
            "anobseavl", "codareavlprd", "numisnprd", "titprd", "clsqliprd", "sitclsqli", "areavlprd"
        ],
        "QUALISAREAAVLPROD": [
            "anobseavl", "codareavlprd", "nomareavlprd"
        ],
        "QUALISAREAAVLAREACAPES": [
            "anobseavl", "codareavlprd", "codcur", "codcpeatl"
        ],
        "UNIDCOLEG": [
            "codclg", "sglclg", "codund", "dtainivinund", "dtafimvinund"
        ],
        "PESSOA": ["codpes", "nompes"],
        "CURSO": ["codcur", "codclg"]
    }

    print(f"{'Tabela':<30} | {'Coluna':<25} | {'Status':<10}")
    print("-" * 75)

    for table, columns in tables_to_verify.items():
        # Verifica a tabela
        try:
            DB.execute(f"SELECT TOP 1 * FROM {table}")
            print(f"{table:<30} | {'(tabela)':<25} | OK")
            
            # Verifica cada coluna
            for column in columns:
                try:
                    DB.execute(f"SELECT TOP 1 {column} FROM {table}")
                    print(f"{table:<30} | {column:<25} | OK")
                except Exception as e:
                    print(f"{table:<30} | {column:<25} | FALHA")
        except Exception as e:
            print(f"{table:<30} | {'(tabela)':<25} | FALHA")

if __name__ == "__main__":
    verify_schema()
