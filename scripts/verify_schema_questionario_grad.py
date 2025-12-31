import logging
import os
import sys
from sqlalchemy import text

# Adiciona o diretório raiz ao path para importar o replicado
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from replicado.connection import DB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_table(engine, table_name, columns):
    logger.info(f"Verificando tabela: {table_name}")
    try:
        # Tenta selecionar o TOP 1 de cada coluna
        cols_str = ", ".join(columns)
        query = text(f"SELECT TOP 1 {cols_str} FROM {table_name}")
        with engine.connect() as conn:
            conn.execute(query)
        logger.info(f"  [OK] Tabela {table_name} e colunas verificadas.")
        return True, []
    except Exception as e:
        logger.error(f"  [ERRO] Falha ao verificar {table_name}: {e}")
        # Tenta descobrir qual coluna falhou
        failed_cols = []
        for col in columns:
            try:
                query = text(f"SELECT TOP 1 {col} FROM {table_name}")
                with engine.connect() as conn:
                    conn.execute(query)
            except Exception:
                failed_cols.append(col)
        return False, failed_cols

def main():
    engine = DB.get_engine()
    
    schema = {
        "QUESTIONARIO": [
            "codqtn", "nompsq", "dscpsq", "staati", "dtainiqtn", "dtafimqtn", "dtacad", "codpescad"
        ],
        "QUESTOESPESQUISA": [
            "codqtn", "codqst", "dscqst", "eplqst", "stamlpsel", "dscqstigl", "dscqstepa", "dscqstfcs"
        ],
        "ALTERNATIVAQUESTAO": [
            "codqtn", "codqst", "numatnqst", "dscatn", "statxtcpl", "qtdptosoceco", "dscatnigl", "dscatnepa", "dscatnfcs"
        ],
        "RESPOSTASQUESTAO": [
            "codqtn", "codqst", "numatnqst", "codpes", "numseqsrv", "rpaatn", "txtcplrpa", "dtarpa"
        ]
    }
    
    errors = {}
    
    for table, columns in schema.items():
        success, failed = verify_table(engine, table, columns)
        if not success:
            errors[table] = failed
            
    if errors:
        print("\n--- Inconsistências de Esquema Detectadas ---")
        for table, failed in errors.items():
            if not failed:
                print(f"Tabela {table} não encontrada ou erro geral.")
            else:
                print(f"Tabela {table}: Colunas não encontradas: {', '.join(failed)}")
    else:
        print("\n--- Esquema Verificado com Sucesso! ---")

if __name__ == "__main__":
    main()
