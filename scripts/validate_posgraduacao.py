import logging
import os
from replicado.posgraduacao import Posgraduacao

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_method(method_name, method_func, *args, **kwargs):
    logger.info(f"Testando método: {method_name}")
    try:
        result = method_func(*args, **kwargs)
        if isinstance(result, list):
            count = len(result)
            logger.info(f"  [OK] Retornou {count} resultados.")
            if count > 0:
                logger.info(f"  [EXEMPLO] {result[0]}")
        elif result is None:
            logger.info("  [INFO] Retornou None (esperado se não houver dados).")
        else:
            logger.info(f"  [OK] Retornou dados.")
            logger.info(f"  [DADO] {result}")
        return True
    except Exception as e:
        import traceback
        logger.error(f"  [FALHA] Erro em {method_name}: {e}")
        logger.error(traceback.format_exc())
        return False

def main():
    # Parâmetros para teste (ajustar se necessário conforme o banco)
    # Usando valores que retornaram dados em consultas exploratórias
    codpes_teste = 42421
    codare_teste = 45131

    tests = [
        ("listar_qualificacoes", Posgraduacao.listar_qualificacoes, codpes_teste),
        ("listar_coorientacoes", Posgraduacao.listar_coorientacoes, 5253648),
        ("obter_tese_dissertacao", Posgraduacao.obter_tese_dissertacao, codpes_teste),
        ("listar_inscricoes_area", Posgraduacao.listar_inscricoes_area, codare_teste),
        ("listar_atividades_area", Posgraduacao.listar_atividades_area, codare_teste),
        ("listar_atividades_aluno", Posgraduacao.listar_atividades_aluno, codpes_teste),
        ("listar_idiomas", Posgraduacao.listar_idiomas),
        ("listar_colegiados", Posgraduacao.listar_colegiados),
        ("listar_linhas_pesquisa", Posgraduacao.listar_linhas_pesquisa, codare_teste),
        ("obter_detalhes_inscricao", Posgraduacao.obter_detalhes_inscricao, codpes_teste, codare_teste),
    ]

    total = len(tests)
    passed = 0

    print("-" * 50)
    print("Iniciando Validação do Módulo: Pós-Graduação")
    print("-" * 50)

    for name, func, *args in tests:
        if test_method(name, func, *args):
            passed += 1

    print("-" * 50)
    print(f"Resultado Final: {passed}/{total} testes passaram.")
    print("-" * 50)

    if passed < total:
        exit(1)

if __name__ == "__main__":
    main()
