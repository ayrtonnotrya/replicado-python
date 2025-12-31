import logging
import os
import sys
from typing import Any

# Adiciona o diretório raiz ao sys.path para importações do replicado
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replicado.connection import DB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_table_and_columns(table_name: str, columns: list[str]) -> dict[str, Any]:
    """
    Verifica se a tabela e as colunas existem tentando um SELECT TOP 1.
    """
    results = {
        "table": table_name,
        "exists": False,
        "columns": {},
        "error": None
    }
    
    # Check table existence
    try:
        query = f"SELECT TOP 1 * FROM {table_name}"
        DB.fetch_all(query)
        results["exists"] = True
    except Exception as e:
        results["error"] = str(e)
        return results

    # Check columns
    for col in columns:
        try:
            query = f"SELECT TOP 1 {col} FROM {table_name}"
            DB.fetch_all(query)
            results["columns"][col] = True
        except Exception:
            results["columns"][col] = False
            
    return results

def list_existing_tables() -> list[str]:
    """
    Lista todas as tabelas visíveis no banco de dados.
    """
    try:
        # SQL Server / Sybase logic
        query = "SELECT name FROM sysobjects WHERE xtype='U' ORDER BY name"
        results = DB.fetch_all(query)
        return [r["name"] for r in results]
    except Exception as e:
        logger.error(f"Erro ao listar tabelas: {e}")
        return []

def main():
    logger.info("Iniciando Verificação de Schema para o Módulo Pesquisa...")
    
    existing_tables = [t.upper() for t in list_existing_tables()]
    logger.info(f"Encontradas {len(existing_tables)} tabelas no banco de dados.")
    
    # Definição das tabelas e colunas principais baseadas no JSON de visão
    schema_to_verify = {
        "ICTPROJETO": ["anoprj", "codprj", "titprj", "codpesalu", "codpesort", "staatlprj", "dtainiprj", "dtafimprj"],
        "ICTPROJEDITALBOLSA": ["anoprj", "codprj", "codctgedi", "dtaccdbol", "dtafimbol"],
        "ICTPROJFOMENTO": ["anoprj", "codprj", "codpgmfcm", "nomagefom", "idfprofom"],
        "ICTPROJINSCRICAOEDITAL": ["anoedi", "codmdl", "codctgedi", "anoprj", "codprj", "sitinsprj"],
        "ICTQUALIFICAORIENTADOR": ["anoedi", "codmdl", "codctgedi", "codpesort", "notcpepgmpgr"],
        "ICTTIPOINFO": ["codtipifm", "dsctipifm", "clstipifm"],
        "PDPROJETO": ["anoprj", "codprj", "titprj", "staatlprj", "codpes_pd", "codund", "dtainiprj", "dtafimprj"],
        "PDPROGRAMA": ["anoprj", "codprj", "numseq_pd", "dtainipgm_pd", "dtafimpgm_pd", "stapgm_pd"],
        "PDPROGRAMAFOMENTO": ["anoprj", "codprj", "numseq_pd", "numseqfom", "nomagefom"],
        "PDPROGRAMAVINCEMPRESA": ["anoprj", "codprj", "numseq_pd", "nomrazsocepr"],
        "PDPROJETOSUPERVISOR": ["anoprj", "codprj", "numseqspv", "tipspv", "codpesspv"],
        "PDPROJSOLICITACAO": ["anoprj", "codprj", "numseqsol", "codtipsol", "sitpcesol"],
        "PDTIPOINFO": ["codtipifm", "dsctipifm", "clstipifm"],
        "ACIPROJETO": ["anoprj", "codprj", "titprj", "staatlprj", "dtainiprj", "dtafimprj"],
        "ACIPROJPARTICIPACAO": ["anoprj", "codprj", "numseqptp", "codpesptp"],
        "ACIPARTICIPANTE": ["codpesptp", "nomptp", "tipdocptp", "numdocptp"],
        "ALUNOPD": ["codpes", "codundclg", "sitatl", "dtainivin", "dtafimvin"],
        "AREACONHECIMENTOCNPQ": ["codarecnhcpq", "nomarecnhcpq"],
        "PROPESQAREA": ["codarepsq", "nomarepsq"],
        "PROPESQFOMENTO": ["codpgmfcm", "nompgmfcm", "sglpgmfcm"],
        "PROPESQFORMACAOMERITO": ["codformer", "dscformer"],
        "PROPESQTIPOMODALID": ["codmdl", "nommdl", "sglmdl"],
        # common fallbacks
        "VINCULOPESSOAUSP": ["codpes", "tipvin", "sitatl", "codund"],
        "LOCALIZAPESSOA": ["codpes", "tipvin", "sitatl", "codund"],
    }
    
    report = []
    report.append("# Relatório de Diagnóstico de Replicação - Módulo Pesquisa\n")
    report.append(f"Data da Verificação: {os.popen('date').read().strip()}\n")
    
    total_tables = len(schema_to_verify)
    found_tables = 0
    
    for table, columns in schema_to_verify.items():
        res = verify_table_and_columns(table, columns)
        
        status_icon = "✅" if res["exists"] else "❌"
        if res["exists"]:
            found_tables += 1
            logger.info(f"{status_icon} Tabela {table} encontrada.")
        else:
            logger.warning(f"{status_icon} Tabela {table} NÃO encontrada.")
            
        report.append(f"## {status_icon} {table}")
        if not res["exists"]:
            report.append(f"- **Tabela não encontrada ou inacessível.**")
        else:
            report.append("- **Tabela encontrada.**")
            report.append("- **Colunas verificadas:**")
            for col, col_exists in res["columns"].items():
                col_icon = "✅" if col_exists else "❌"
                report.append(f"  - {col_icon} `{col}`")
        report.append("")

    report.append("\n# Tabelas Existentes no Banco (Amostra - Top 50)\n")
    for t in existing_tables[:50]:
        report.append(f"- {t}")
    
    summary = f"\n# Resumo\n- Tabelas da Visão Verificadas: {total_tables}\n- Tabelas Encontradas: {found_tables}\n- Tabelas Faltantes: {total_tables - found_tables}"
    report.append(summary)
    
    # Save report
    os.makedirs("temp/relatorios", exist_ok=True)
    report_path = "temp/relatorios/pesquisa_analise_status.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report))
        
    logger.info(f"Verificação concluída. Relatório salvo em: {report_path}")
    print(summary)

if __name__ == "__main__":
    main()
