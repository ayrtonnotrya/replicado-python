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
    # Tabelas e colunas a verificar
    schema = {
        "DIM_PESSOA_XMLUSP": ["idfpescpq", "imgarqxml", "codpes", "dtaultalt"],
        "CITACAOPESSOA": ["codpes", "idcfteifm", "idfpesfte", "qtdpdupes", "clsinh"],
        "CITACAOPESSOAANUAL": ["codpes", "idcfteifm", "anoref", "qtdcitpes"],
        "QUALISPERIODICO": ["anobseavl", "numisnprd", "titprd", "clsqliprd"],
        "ACIPROJETO": ["codprj", "anoprj", "titprjaco", "sitprj"],
        "PDPROJETO": ["codprj", "anoprj", "titprj", "sitprj"],
        "ICTPROJETO": ["codprj", "anoprj", "titprj", "sitprj"],
        "AREACONHECIMENTOCNPQ": ["codare", "nomare"],
        "PDPROGRAMA": ["codpgm", "nompgm"],
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
