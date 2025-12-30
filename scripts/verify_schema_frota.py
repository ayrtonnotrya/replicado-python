import os
import sys
from dotenv import load_dotenv

# Adiciona o diretório raiz ao path
sys.path.append(os.getcwd())

load_dotenv()

from replicado.connection import DB

def check_table(table_name: str) -> bool:
    print(f"\n--- Verificando tabela: {table_name} ---")
    
    # Verifica se a tabela existe
    check_query = f"SELECT name FROM sysobjects WHERE name = '{table_name}' AND type = 'U'"
    try:
        exists = DB.fetch_all(check_query)
        if not exists:
            print(f"❌ TABELA NÃO ENCONTRADA: {table_name}")
            return False
    except Exception as e:
        print(f"⚠️ Erro ao verificar existência de {table_name}: {e}")
        return False

    # Tenta um SELECT TOP 1 para garantir permissão e dados
    try:
        DB.fetch_all(f"SELECT TOP 1 * FROM {table_name}")
        print(f"✅ Acesso de leitura OK")
    except Exception as e:
        print(f"❌ Erro ao ler dados de {table_name}: {e}")

    # Lista colunas
    query = f"""
        SELECT c.name as column_name, t.name as data_type
        FROM syscolumns c
        JOIN systypes t ON c.usertype = t.usertype
        WHERE c.id = object_id('{table_name}')
        ORDER BY c.name
    """
    try:
        result = DB.fetch_all(query)
        print(f"📊 Colunas encontradas ({len(result)}):")
        cols = [row.get('column_name') for row in result]
        print(f"   {', '.join(cols[:10])}{'...' if len(cols) > 10 else ''}")
        return True
    except Exception as e:
        print(f"⚠️ Erro ao listar colunas para {table_name}: {e}")
        return False

def find_similar_tables(pattern: str) -> None:
    print(f"\n--- Buscando tabelas que coincidem com '{pattern}' ---")
    query = f"""
        SELECT name as table_name
        FROM sysobjects
        WHERE name LIKE '{pattern}' AND type = 'U'
        ORDER BY name
    """
    try:
        result = DB.fetch_all(query)
        if not result:
            print("Nenhuma tabela encontrada.")
            return
        print(f"Encontradas {len(result)} tabelas:")
        for row in result:
            print(f"  - {row.get('table_name')}")
    except Exception as e:
        print(f"⚠️ Erro ao buscar tabelas: {e}")

if __name__ == "__main__":
    tables_to_check = [
        "FROTA_VEICULO",
        "FROTA_GRUPOVEICULO",
        "FROTA_SUBTIPOVEICULO",
        "FROTA_IPVA",
        "FROTA_CONDUTOR",
        "FROTA_CATEGHABILITACAO",
        "FROTA_ABASTECIMENTO",
        "FROTA_TIPOCOMBUSTIVEL",
        "FROTA_ORDEMSERVICO",
        "FROTA_MANUTENCAO",
        "FROTA_ITEMMANUTENCAO",
        "FROTA_GRUPOMANUTENCAO",
        "FROTA_SUBGRUPOMANUT",
        "FROTA_TIPOMANUTENCAO",
        "FROTA_CONTROLETRAFEGO",
        "FROTA_HODOMETRO",
        "FROTA_TIPOSITUACAO",
        "FROTA_SUBTIPOSITUACAO",
        "FROTA_MANUTNOTAFISCAL",
        "FROTA_TIPOCONDESPECIAL",
        "FROTA_TIPOVEICULO",
        "FROTA_VEICULOSITUACAO",
        "TIPOCOR",
        "UNIDADE",
        "SETOR",
        "LOCALIDADE",
        "PESSOA"
    ]

    print("🚀 Iniciando Diagnóstico de Esquema do Módulo Frota")
    print("=" * 50)
    
    results = {"total": len(tables_to_check), "ok": 0, "fail": 0}
    
    for t in tables_to_check:
        if check_table(t):
            results["ok"] += 1
        else:
            results["fail"] += 1

    print("\n" + "=" * 50)
    print(f"🏁 Resumo: {results['ok']} tabelas OK, {results['fail']} falhas de {results['total']} verificadas.")

    print("\n🔍 Buscando por tabelas similares para diagnóstico...")
    find_similar_tables("%FROTA%")
    find_similar_tables("%VEIC%")
    find_similar_tables("%VCL%")
    find_similar_tables("%MOTOR%")
