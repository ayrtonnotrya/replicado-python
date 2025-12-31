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
    # Tabelas e colunas fundamentais para o módulo de pós-graduação
    schema = {
        "AGPROGRAMA": ["codare", "codpes", "numseqpgm", "dtaselpgm", "nivpgm", "vinalupgm", "dtadpopgm", "dtadfapgm", "dtahomtrbcpg"],
        "AGINSCRICAO": ["codare", "codpes", "dtainsare", "staselare"],
        "AREA": ["codare", "codcur", "codcpe"],
        "AREACAPES": ["codcpe", "nomarecpe", "codcpeatl"],
        "DISCIPLINA": ["sgldis", "numseqdis", "nomdis"],
        "OFERECIMENTO": ["sgldis", "numseqdis", "numofe", "dtainiofe", "dtafimofe"],
        "ESPACOTURMA": ["sgldis", "numseqdis", "numofe", "horiniofe", "horfimofe"],
        "QUALIFICACAO": ["codpes", "codare", "numseqpgm", "nivpgm", "numseqqua", "dtaqua", "cctqua"],
        "TRABALHOPROG": ["codpes", "codare", "numseqpgm", "tittrb"],
        "COORIENTACAO": ["codpes", "codare", "dtainsare", "codpesdctegr", "dtainicoi"],
        "TITCONCEDIDO": ["codpes", "codare", "numseqpgm", "dtatitcon"],
        "HISTPROGRAMA": ["codpes", "codare", "numseqpgm", "dtastapgm", "stapgm"],
        "CREDAREA": ["codare", "codpes", "dtavencre"],
        "R25CRECREDOC": ["codare", "codpes", "nivcre"],
        "TABMOTIVDESLIG": ["codmotdes", "desmotdes"],
        "TABMOTIVTRANCA": ["codmottrnc", "desmottrnc"],
        "IDIOMA": ["codlin", "dsclin"],
    }

    inconsistencies = []

    print("-" * 50)
    print("Iniciando Verificação de Esquema: Pós-Graduação")
    print("-" * 50)

    for table, columns in schema.items():
        if not verify_table(table, columns):
            inconsistencies.append(table)

    print("-" * 50)
    if inconsistencies:
        print("\nInconsistências encontradas nas seguintes tabelas:")
        for table in inconsistencies:
            print(f"- {table}")
    else:
        print("\nNenhuma inconsistência encontrada no esquema verificado!")
    print("-" * 50)

if __name__ == "__main__":
    main()
