import logging

from replicado.connection import DB

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def verify_table(table_name, columns):
    """
    Verifica se uma tabela e suas colunas existem.
    """
    logger.info(f"Verificando tabela: {table_name}")
    cols_str = ", ".join(columns)
    query = f"SELECT TOP 1 {cols_str} FROM {table_name}"
    try:
        DB.fetch(query)
        logger.info(f"  [OK] Tabela {table_name} e colunas verificadas.")
        return True
    except Exception as e:
        logger.error(f"  [FALHA] Tabela {table_name}: {e}")
        return False


def main():
    # Tabelas e colunas a verificar conforme proposta de métodos
    schema = {
        "BIOTERIO": ["codbtr", "tipbtr", "nombtr", "codundrsp", "sitbtr", "emabtr", "numtelfmt"],
        "BIOCARACTERISTICA": ["codbtr", "codtipbtr", "arefsctot", "arefsccuc"],
        "BIOFLUXOANIMAL": ["codbtr", "codtipani", "lngani", "qtdanirpdmes", "qtdanipduadq", "qtdanifrnmes", "qtdanimntmes"],
        "BIOPARTICIPACAO": ["codbtr", "codpcpbtr", "codtipptpbtr", "dtainiptp", "dtafimptp"],
        "BIOPARTICIPANTE": ["codpcpbtr", "nompcpbtr", "codpespcp"],
        "BIORECHUMANO": ["codbtr", "codpcpbtr", "codescrcs", "codfncrcs"],
        "BIOTIPOINFO": ["codtipifm", "dsctipifm", "clstipifm"],
        "PESSOA": ["codpes", "nompes"]
    }

    inconsistencies = []

    for table, columns in schema.items():
        if not verify_table(table, columns):
            inconsistencies.append(table)

    if inconsistencies:
        print("\nInconsistências encontradas nas tabelas:")
        for table in inconsistencies:
            print(f"- {table}")
    else:
        print("\nNenhuma inconsistência encontrada no esquema proposto.")


if __name__ == "__main__":
    main()
