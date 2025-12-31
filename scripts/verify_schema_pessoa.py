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

    # Get real columns for diagnostics
    try:
        # SQL Server / Sybase logic to get column names
        query = f"SELECT name FROM syscolumns WHERE id = OBJECT_ID('{table_name}')"
        col_results = DB.fetch_all(query)
        results["actual_columns"] = [r["name"] for r in col_results]
    except Exception as e:
        results["actual_columns"] = [f"Erro ao obter colunas: {e}"]
            
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
    logger.info("Iniciando Verificação de Schema para o Módulo Pessoa...")
    
    existing_tables = [t.upper() for t in list_existing_tables()]
    logger.info(f"Encontradas {len(existing_tables)} tabelas no banco de dados.")
    
    # Definição das tabelas e colunas principais baseadas no JSON de visão
    # Colunas mapeadas a partir dos códigos no JSON
    schema_to_verify = {
        "PESSOA": ["codpes", "nompes", "nommaepes", "dtanas", "tipdocidf", "numdocidf", "sexpes", "codpas", "nomcnhpes", "numcpf"],
        "COMPLPESSOA": ["codpes", "codestciv", "codrac", "codidtgne", "codortsex"],
        "VINCULOPESSOAUSP": ["codpes", "numseqpes", "tipvin", "sitatl", "codund", "codset", "tipfnc", "nomcaa"],
        "SERVIDOR": ["codpes", "numseqsrv", "codund", "tipcon", "tipjor"],
        "ALUNOGR": ["codpes", "numseqpgm", "codcurgrd", "codhab"],
        "ALUNOPOS": ["codpes", "numseqpgm", "codcurpgr", "codare", "nivpgm"],
        "ESTAGIARIO": ["codpes", "numseqsrv", "codund", "dtainiest", "dtafimest"],
        "TITULOPES": ["codpes", "numseqtitpes", "titpes", "dtatitpes", "codesc", "nivesc", "grufor"],
        "PREMIOPES": ["codpes", "numseqpmopes", "nompmopes", "codorg", "dtarcbpmopes"],
        "EMAILPESSOA": ["codpes", "codema", "staeat"],
        "TELEFPESSOA": ["codpes", "numseqtel", "codddd", "numtel", "tiptelef"],
        "ENDPESSOA": ["codpes", "numseqend", "endpes", "numend", "nombai", "cidpes", "sgpessgl"],
        "COMPLPESSOASERV": ["codpes", "numseqsrv", "numpis", "numctps", "serctps"],
        "PARTICIPANTECOLEG": ["codpes", "codclg", "sglclg"],
        "VINCSATPROFSENIOR": ["codpes", "codund", "dtainicbd", "dtafimcbd"],
        "IDENTIDADEGENERO": ["codidtgne", "dscidtgne"],
        "ORIENTACAOSEXUAL": ["codortsex", "dscortsex"],
        "RACACOR": ["codrac", "dscrac"],
        "RESUSERVHISTFUNCIONAL": ["codpes", "numseqsrv", "dtareg"],
        "VINCSATCOLEGIADO": ["codpes", "codclg", "sglclg", "dtainivin"],
        "VINCSATDESIGNACAO": ["codpes", "codund", "codset", "dtainivin"],
        "VINCSATHABILITACAOGR": ["codpes", "codhab", "dtainihab"],
    }
    
    report = []
    report.append("# Relatório de Diagnóstico de Replicação - Módulo Pessoa\n")
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
            for col in columns:
                col_exists = res["columns"].get(col, False)
                col_icon = "✅" if col_exists else "❌"
                report.append(f"  - {col_icon} `{col}`")
            
            report.append("- **Colunas reais no banco:**")
            report.append(f"  `{', '.join(res.get('actual_columns', []))}`")
        report.append("")

    report.append("\n# Tabelas Existentes no Banco (Amostra - Top 50)\n")
    for t in existing_tables[:50]:
        report.append(f"- {t}")
    
    summary = f"\n# Resumo\n- Tabelas da Visão Verificadas: {total_tables}\n- Tabelas Encontradas: {found_tables}\n- Tabelas Faltantes: {total_tables - found_tables}"
    report.append(summary)
    
    # Save report
    os.makedirs("temp/relatorios", exist_ok=True)
    report_path = "temp/relatorios/pessoa_diagnostico_schema.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report))
        
    logger.info(f"Verificação concluída. Relatório salvo em: {report_path}")
    print(summary)

if __name__ == "__main__":
    main()
