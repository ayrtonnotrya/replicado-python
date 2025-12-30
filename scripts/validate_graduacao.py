import os
import sys
import logging
from typing import Any

# Adiciona a raiz do projeto ao python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from replicado.graduacao import Graduacao
from replicado.connection import DB

# Configura log para ver as queries se necessário
logging.basicConfig(level=logging.INFO)

def test_method(name: str, func, *args, **kwargs) -> Any:
    print(f"\nTeste [{name}]: ", end="")
    try:
        result = func(*args, **kwargs)
        if result:
            count = len(result) if isinstance(result, list) else 1
            print(f"SUCESSO ({count} registros)")
            # Mostra o primeiro registro como amostra se for lista
            if isinstance(result, list) and len(result) > 0:
                print(f"   Amostra: {result[0]}")
            elif isinstance(result, dict):
                print(f"   Dados: {result}")
            else:
                print(f"   Valor: {result}")
        else:
            print("VAZIO (Sem dados encontrados com estes parâmetros)")
        return result
    except Exception as e:
        print(f"FALHA")
        print(f"   Erro: {e}")
        return None

def main():
    print("=== Validação do Módulo Graduacao ===\n")
    
    # 1. Busca um codpes de aluno ativo para testes
    aluno = DB.fetch("SELECT TOP 1 codpes FROM LOCALIZAPESSOA WHERE tipvin = 'ALUNOGR'")
    if not aluno:
        print("Erro: Nenhum aluno de graduação encontrado no banco para testes.")
        return
    
    codpes = aluno['codpes']
    print(f"Usando codpes {codpes} para testes individuais.")

    # Busca uma turma ativa para testes
    turma = DB.fetch("SELECT TOP 1 coddis, verdis, codtur FROM TURMAGR WHERE statur = 'A'")
    if not turma:
        print("Aviso: Nenhuma turma ativa encontrada para testes de turmas.")
        turma = {'coddis': 'XXX0000', 'verdis': 1, 'codtur': '2025101'}
    
    # Execução dos testes
    test_method("obter_nome_social", Graduacao.obter_nome_social, codpes)
    test_method("obter_vencimento_identidade", Graduacao.obter_vencimento_identidade, codpes)
    test_method("obter_vencimento_passaporte", Graduacao.obter_vencimento_passaporte, codpes)
    test_method("listar_ingressantes", Graduacao.listar_ingressantes, 2024)
    test_method("obter_notas_ingresso", Graduacao.obter_notas_ingresso, codpes)
    test_method("listar_trancamentos_aluno", Graduacao.listar_trancamentos_aluno, codpes)
    test_method("listar_equivalencias_externas", Graduacao.listar_equivalencias_externas, codpes)
    test_method("listar_ministrantes", Graduacao.listar_ministrantes, turma['coddis'], turma['verdis'], turma['codtur'])
    test_method("obter_horario_turma", Graduacao.obter_horario_turma, turma['coddis'], turma['verdis'], turma['codtur'])
    test_method("contar_vagas_turma", Graduacao.contar_vagas_turma, turma['coddis'], turma['verdis'], turma['codtur'])
    test_method("obter_turma_pratica_vinculada", Graduacao.obter_turma_pratica_vinculada, turma['coddis'], turma['verdis'], turma['codtur'])
    test_method("listar_requerimentos_aluno", Graduacao.listar_requerimentos_aluno, codpes)
    
    # Busca um requerimento real se existir
    req = DB.fetch("SELECT TOP 1 codrqm FROM REQUERIMENTOGR")
    if req:
        test_method("obter_detalhes_requerimento", Graduacao.obter_detalhes_requerimento, req['codrqm'])
    
    test_method("listar_alunos_especiais", Graduacao.listar_alunos_especiais, turma['coddis'], turma['verdis'], turma['codtur'])
    test_method("listar_disciplinas_por_prefixo", Graduacao.listar_disciplinas_por_prefixo, "MAC")
    
    # Busca um curso real para normas
    curso = DB.fetch("SELECT TOP 1 codcur, codhab FROM HABILITACAOGR")
    if curso:
        test_method("obter_normas_habilitacao", Graduacao.obter_normas_habilitacao, curso['codcur'], curso['codhab'])
    
    test_method("obter_data_limite_conclusao", Graduacao.obter_data_limite_conclusao, codpes)
    test_method("listar_alunos_por_status_programa", Graduacao.listar_alunos_por_status_programa, "A")
    test_method("listar_disciplinas_com_vagas_extracurriculares", Graduacao.listar_disciplinas_com_vagas_extracurriculares)

if __name__ == "__main__":
    main()
