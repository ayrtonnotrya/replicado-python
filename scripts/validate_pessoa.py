import logging
import os
import sys
from typing import Any

# Adiciona o diretório raiz ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replicado.pessoa import Pessoa
from replicado.connection import DB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_method(method_name: str, *args, **kwargs):
    logger.info(f"Testando método: {method_name} com args={args} kwargs={kwargs}")
    try:
        method = getattr(Pessoa, method_name)
        result = method(*args, **kwargs)
        if result:
            logger.info(f"✅ {method_name}: Sucesso. Retornou {type(result)} com {len(result) if isinstance(result, (list, dict)) else 'valor'}")
            return result
        else:
            logger.warning(f"⚠️ {method_name}: Retornou vazio/None.")
            return None
    except Exception as e:
        logger.error(f"❌ {method_name}: Erro - {e}")
        return None

def main():
    logger.info("Iniciando Validação do Módulo Pessoa...")
    
    # 1. Tenta encontrar codpes para testes
    # Alguém com nome social
    row_social = DB.fetch("SELECT TOP 1 codpes FROM PESSOA WHERE stautlnomsoc = 'S'")
    codpes_social = row_social['codpes'] if row_social else 10001
    test_method("obter_nome_social", codpes_social)
    
    # Alguém com diversidade (COMPLPESSOA)
    row_div = DB.fetch("SELECT TOP 1 codpes FROM COMPLPESSOA")
    codpes_div = row_div['codpes'] if row_div else 10001
    test_method("obter_diversidade", codpes_div)
    
    # Alguém com titulações
    row_tit = DB.fetch("SELECT TOP 1 codpes FROM TITULOPES")
    codpes_tit = row_tit['codpes'] if row_tit else 10001
    test_method("listar_titulacoes", codpes_tit)
    
    # Alguém com premiações
    row_pre = DB.fetch("SELECT TOP 1 codpes FROM PREMIOPES")
    codpes_pre = row_pre['codpes'] if row_pre else 10001
    test_method("listar_premiacoes", codpes_pre)
    
    # Professores Seniores
    row_sen = DB.fetch("SELECT TOP 1 codund FROM VINCSATPROFSENIOR")
    codund_sen = row_sen['codund'] if row_sen else None
    test_method("listar_professores_seniores", codund_sen)
    
    # Membros de Colegiado
    row_clg = DB.fetch("SELECT TOP 1 codclg FROM PARTICIPANTECOLEG")
    codclg = row_clg['codclg'] if row_clg else 1
    test_method("listar_membros_colegiado", codclg)
    
    # Dados Servidor Complementar
    row_srv = DB.fetch("SELECT TOP 1 codpes FROM COMPLPESSOASERV")
    codpes_srv = row_srv['codpes'] if row_srv else 10001
    test_method("obter_dados_servidor_complementar", codpes_srv)

    logger.info("Validação concluída.")

if __name__ == "__main__":
    main()
