import logging
import os
import sys
import time
from typing import Any

# Adiciona a raiz do projeto ao python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from replicado.connection import DB
from replicado.lattes import Lattes

# Configura log para ver as queries e cache
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_method(name: str, func, *args, **kwargs) -> Any:
    print(f"\nTeste [{name}]: ", end="")
    try:
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()

        if result:
            count = len(result) if isinstance(result, list) else 1
            print(f"SUCESSO ({count} registros) em {end - start:.4f}s")
            # Mostra o primeiro registro como amostra se for lista
            if isinstance(result, list) and len(result) > 0:
                print(f"   Amostra: {result[0]}")
            elif isinstance(result, dict):
                print(f"   Dados: {result}")
            else:
                print(f"   Valor: {result}")
        else:
            print(f"VAZIO/FALHA (em {end - start:.4f}s)")
        return result
    except Exception as e:
        print("ERRO CRÍTICO")
        print(f"   Causa: {e}")
        return None


def main():
    print("=== Validação do Módulo Lattes ===\n")

    # 1. Busca um codpes que tenha XML para testes
    docente = DB.fetch("SELECT TOP 1 codpes FROM DIM_PESSOA_XMLUSP")
    if not docente:
        print("Erro: Nenhum docente com Lattes (XML) encontrado no banco para testes.")
        return

    codpes = docente["codpes"]
    print(f"Usando codpes {codpes} para testes individuais.")

    # Execução dos testes
    print("\n--- Testes de Funcionalidade XML ---")
    test_method("obter_array", Lattes.obter_array, codpes)

    # Teste de Cache (repetindo a chamada)
    print("\n--- Teste de Cache (repetindo obter_array) ---")
    test_method("obter_array (CACHE)", Lattes.obter_array, codpes)

    test_method("listar_artigos", Lattes.listar_artigos, codpes)
    test_method("listar_livros_publicados", Lattes.listar_livros_publicados, codpes)
    test_method("listar_capitulos_livros", Lattes.listar_capitulos_livros, codpes)
    test_method("listar_trabalhos_anais", Lattes.listar_trabalhos_anais, codpes)
    test_method(
        "listar_outras_producoes_bibliograficas",
        Lattes.listar_outras_producoes_bibliograficas,
        codpes,
    )
    test_method("listar_teses (DOUTORADO)", Lattes.listar_teses, codpes, "DOUTORADO")
    test_method("listar_areas_conhecimento", Lattes.listar_areas_conhecimento, codpes)

    print("\n--- Testes de Funcionalidade Relacional (Novos Métodos) ---")
    print(
        "Nota: Estes métodos podem retornar VAZIO/ERRO se as tabelas não estiverem replicadas."
    )
    test_method("listar_artigos_com_qualis", Lattes.listar_artigos_com_qualis, codpes)
    test_method("obter_metricas_citacao", Lattes.obter_metricas_citacao, codpes)
    test_method("listar_citacoes_anual", Lattes.listar_citacoes_anual, codpes)
    test_method("listar_projetos_pesquisa", Lattes.listar_projetos_pesquisa, codpes)
    test_method(
        "obter_detalhes_pos_doutorado", Lattes.obter_detalhes_pos_doutorado, codpes
    )
    test_method(
        "retornar_genero_pesquisador", Lattes.retornar_genero_pesquisador, codpes
    )


if __name__ == "__main__":
    main()
