import os
import sys
from pprint import pprint
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())

from datetime import datetime
from dotenv import load_dotenv
import logging

from replicado import Pessoa, Graduacao, Posgraduacao, Bempatrimoniado, Estrutura
from replicado.utils import clean_string, remove_accents, dia_semana, horario_formatado, data_mes

load_dotenv()

# --- CONFIGURAÇÃO DE LOGGING PARA O TESTE ---
# O usuário da biblioteca pode configurar o logger conforme desejar.
# Aqui habilitamos o nível DEBUG para ver as queries SQL.
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def run_tests():
    print("=== INICIANDO TESTES EXTENDIDOS COM DADOS REAIS ===")
    
    codundclg_env = os.getenv('REPLICADO_CODUNDCLG')
    if not codundclg_env:
        print("ERRO: REPLICADO_CODUNDCLG não definido")
        return

    codundclg = int(codundclg_env.split(',')[0]) if ',' in codundclg_env else int(codundclg_env)
    
    # --- UTILS ---
    print("\n--- TESTE: UTILS ---")
    print(f"Clean string: '{clean_string('  teste  ')}'")
    print(f"Remove accents: {remove_accents('Ávidos')}")
    print(f"Dia semana 2SG: {dia_semana('2SG')}")
    print(f"Horario 0830: {horario_formatado('0830')}")
    print(f"Data mes (datetime): {data_mes(datetime.now())}")
    print(f"Data mes (str): {data_mes('2023-01-01 10:00:00')}")

    # --- ESTRUTURA ---
    print("\n--- TESTE: ESTRUTURA ---")
    unidades = Estrutura.listar_unidades()
    if unidades:
        u = unidades[0]
        codund = u['codund']
        print(f"Unidade: {u['nomund']} ({codund})")
        # Obter unidade
        print("Obter unidade:", Estrutura.obter_unidade(codund).get('nomund'))
        # Listar locais unidade
        locais = Estrutura.listar_locais_unidade(codund)
        print(f"Locais na unidade {codund}: {len(locais)}")
        if locais:
            # Obter local
            codlocusp = locais[0]['codlocusp']
            print("Obter local:", Estrutura.obter_local(codlocusp).get('nomlocusp'))
            # Procurar local
            part_cod = str(codlocusp)[:3]
            print(f"Procurar local part {part_cod}: {len(Estrutura.procurar_local(part_cod, codund))}")

    setores = Estrutura.listar_setores(codundclg)
    if setores:
        codset = setores[0]['codset']
        # Dump setor
        print(f"Dump setor {codset}:", Estrutura.dump(codset).get('nomset'))
        
        chefia = Estrutura.get_chefia_setor(codset, substitutos=True)
        chefia_no_sub = Estrutura.get_chefia_setor(codset, substitutos=False)
        print(f"Chefia (all/no_sub): {len(chefia)}/{len(chefia_no_sub)}")

    # --- GRADUAÇÃO ---
    print("\n--- TESTE: GRADUAÇÃO ---")
    print("Contar ativos:", Graduacao.contar_ativos())
    
    alunos_grad = Graduacao.listar_ativos()
    if alunos_grad:
        aluno = alunos_grad[0]
        codpes = aluno['codpes']
        print(f"Aluno Grad: {aluno['nompes']} ({codpes})")
        
        # Testar metodos dependentes de codpes
        print("Obter curso ativo:", Graduacao.obter_curso_ativo(codpes).get('nomcur', 'N/A'))
        print("Verifica:", Graduacao.verifica(codpes, codundclg))
        
        # Programas/Cursos
        curso = Graduacao.obter_curso_ativo(codpes)
        if curso:
            codcur = curso.get('codcur')
            codhab = curso.get('codhab')
            if codcur:
                print("Programa:", Graduacao.programa(codpes).get('nomcur', 'N/A'))
                print("Nome curso:", Graduacao.nome_curso(codcur))
                print("Nome habilitação:", Graduacao.nome_habilitacao(codhab, codcur))
                
                # Disciplinas
                disciplinas = Graduacao.listar_disciplinas()
                print(f"Disciplinas da unidade: {len(disciplinas)}")
                if disciplinas and len(disciplinas) > 0: # Walrus if python 3.8+
                    coddis = disciplinas[0]['coddis']
                    print(f"Nome disciplina {coddis}:", Graduacao.nome_disciplina(coddis))
                    print(f"Créditos disciplina {coddis}:", Graduacao.creditos_disciplina(coddis))
                
                # Disciplinas Aluno
                disc_aluno = Graduacao.listar_disciplinas_aluno(codpes)
                print(f"Disciplinas histórico aluno: {len(disc_aluno)}")
                
                # Médias
                print(f"Média Ponderada (Suja): {Graduacao.obter_media_ponderada_suja(codpes)}")
                print(f"Média Ponderada (Limpa): {Graduacao.obter_media_ponderada_limpa(codpes)}")
                
                # Grade Horária
                print(f"Grade Horária atual: {len(Graduacao.obter_grade_horaria(codpes))}")
                
                # Intercâmbio
                try:
                    print(f"Intercâmbios (geral): {len(Graduacao.listar_intercambios())}")
                    print(f"Intercâmbio aluno: {len(Graduacao.obter_intercambio_por_codpes(codpes))}")
                except Exception as e:
                    print(f"Erro em Intercâmbios (Tabela faltante?): {e}")
                
                # Verificações
                print(f"Graduado na unidade? {Graduacao.verificar_pessoa_graduada_unidade(codpes)}")
                print(f"Coordenador? {Graduacao.verificar_coordenador_curso_grad(codpes)}")
                
                # Departamentos
                print(f"Departamentos Ensino: {len(Graduacao.listar_departamentos_de_ensino())}")
                
                # Setor Aluno
                print(f"Setor do Aluno: {Graduacao.setor_aluno(codpes, codundclg)}")
                
        # Obter cursos habilitacoes
        cursos_hab = Graduacao.obter_cursos_habilitacoes(codundclg)
        print(f"Cursos/Habilitações na unidade: {len(cursos_hab)}")

    # --- PESSOA ---
    print("\n--- TESTE: PESSOA ---")
    # Usar codpes do aluno de graduação
    if 'codpes' in locals() and codpes:
        print(f"Dump: {Pessoa.dump(codpes).get('nompes')}")
        print("Crachá:", Pessoa.cracha(codpes))
        print("Listar Crachás:", len(Pessoa.listar_crachas(codpes)))
        print("Emails:", Pessoa.emails(codpes))
        print("Email princ:", Pessoa.email(codpes))
        print("Telefones:", Pessoa.telefones(codpes))
        print("Obter Endereço:", Pessoa.obter_endereco(codpes))
        
        # Obter Nome (single/list)
        print("Nome (str):", Pessoa.obter_nome(codpes))
        print("Nome (list):", Pessoa.obter_nome([codpes]))
        
        # Procurar por nome
        nome_busca = aluno['nompes'].split()[0]
        print(f"Busca '{nome_busca}':")
        print(" - Fonético:", len(Pessoa.procurar_por_nome(nome_busca, fonetico=True)))
        print(" - Normal:", len(Pessoa.procurar_por_nome(nome_busca, fonetico=False)))
        print(" - Inativos:", len(Pessoa.procurar_por_nome(nome_busca, ativos=False)))
        
        # Vinculos
        vinculos = Pessoa.listar_vinculos_ativos(codpes)
        print("Vínculos ativos:", len(vinculos))
        if vinculos:
             tipvinext = vinculos[0]['tipvinext']
             print(f"Total vínculo '{tipvinext}':", Pessoa.total_vinculo(tipvinext, codundclg))

    # --- PÓS-GRADUAÇÃO ---
    print("\n--- TESTE: PÓS-GRADUAÇÃO ---")
    print("Contar ativos:", Posgraduacao.contar_ativos())
    
    # Programas
    programas = Posgraduacao.programas(codundclg)
    print(f"Programas: {len(programas)}")
    
    if programas:
        prog = programas[0]
        codare = prog['codare']
        codcur = prog['codcur']
        print(f"Explorando Programa: {prog['nomcur']} (Area {codare})")
        
        # Filtered programas
        Posgraduacao.programas(codundclg, codcur=codcur)
        Posgraduacao.programas(codundclg, codare=codare)
        
        # Orientadores
        orientadores = Posgraduacao.orientadores(codare)
        print(f"Orientadores: {len(orientadores)}")
        if orientadores:
             try:
                codpes_ori = int(orientadores[0]['codpes'])
                print(f"Orientandos do prof {codpes_ori}:", len(Posgraduacao.listar_orientandos_ativos(codpes_ori)))
             except ValueError:
                 pass

        # Catalogo
        catalogo = Posgraduacao.catalogo_disciplinas(codare)
        print(f"Catálogo disciplinas: {len(catalogo)}")
        
        # Oferecimentos atuais
        oferecimentos = Posgraduacao.disciplinas_oferecimento(codare)
        print(f"Oferecimentos ativos: {len(oferecimentos)}")
        
        if oferecimentos:
            ofer = oferecimentos[0]
            sgldis = ofer['sgldis']
            numofe = ofer['numofe']
            numseqdis = ofer['numseqdis']
            
            print(f"Disciplina {sgldis}:", Posgraduacao.disciplina(sgldis).get('nomdis'))
            print("Oferecimento detalhe:", Posgraduacao.oferecimento(sgldis, numofe).get('dtainiofe'))
            print("Espaço turma:", len(Posgraduacao.espacoturma(sgldis, numseqdis, numofe)))
            print("Ministrantes:", len(Posgraduacao.ministrante(sgldis, numseqdis, numofe)))
            
            # Idioma (using codlinofe from oferecimento if available)
            ofer_detalhe = Posgraduacao.oferecimento(sgldis, numofe)
            if 'codlinofe' in ofer_detalhe:
                 print(f"Idioma: {ofer_detalhe['codlinofe']}")

    # Alunos ativos pos
    alunos_pos = Posgraduacao.ativos(codundclg)
    if alunos_pos:
        aluno_pos = alunos_pos[0]
        codpes_pos = aluno_pos['codpes']
        print("Verifica Pós:", Posgraduacao.verifica(codpes_pos, codundclg))
        print("Obter Vínculo Pós:", Posgraduacao.obter_vinculo_ativo(codpes_pos).get('nomcur'))
        
        # Testar orientador
        # Use codpes_ori from earlier orientadores list if available, 
        # or find one now to avoid hardcoded IDs.
        if 'codpes_ori' not in locals():
            orientadores_ativos = Posgraduacao.orientadores(codundclg)
            if orientadores_ativos:
                codpes_ori = int(orientadores_ativos[0]['codpes'])

        if 'codpes_ori' in locals():
            print(f"Orientandos Ativos (alias) do prof {codpes_ori}: {len(Posgraduacao.obter_orientandos_ativos(codpes_ori))}")
            print(f"Orientandos Concluídos do prof {codpes_ori}: {len(Posgraduacao.listar_orientandos_concluidos(codpes_ori))}")
             
        # Obter Defesas (do aluno?)
        # Search for a student with defenses (might be empty for active student)
        print(f"Defesas do aluno {codpes_pos}: {len(Posgraduacao.obter_defesas(codpes_pos))}")

    # Listar programas
    print(f"Listar Programas (novo): {len(Posgraduacao.listar_programas())}")
    
    # Egressos (testar com uma área válida)
    if programas:
         codare_test = programas[0]['codare']
         print(f"Egressos da área {codare_test}: {len(Posgraduacao.egressos_area(codare_test))}")
         print(f"Egressos por ano: {Posgraduacao.contar_egressos_area_agrupado_por_ano(codare_test)}")
         
    # Totais
    print(f"Total ME: {Posgraduacao.total_pos_nivel_programa('ME', codundclg)}")
    print(f"Total DO: {Posgraduacao.total_pos_nivel_programa('DO', codundclg)}")
    
    # Areas Programas
    # print(f"Areas Programas: {Posgraduacao.areas_programas(codundclg)}") 
    # Listar Disciplinas Pos
    print(f"Listar Disciplinas Pós (novo): {len(Posgraduacao.listar_disciplinas())}")

    # Defesas
    # Defesas is range based. Let's try current year.
    print("Defesas ano atual:", len(Posgraduacao.listar_defesas()))


    # --- BEMPATRIMONIADO ---
    print("\n--- TESTE: BEMPATRIMONIADO ---")
    bens = Bempatrimoniado.ativos(limite=5)
    print(f"Bens ativos (limit 5): {len(bens)}")
    if bens:
        bem = bens[0]
        numpat = bem['numpat']
        print(f"Bem {numpat}: Is Informatica? {Bempatrimoniado.is_informatica(str(numpat))}")
        
    # Test filters
    # Bempatrimoniado.ativos(buscas={'desbem': 'computador'}, limite=1) # desbem might fail in WHERE depending on type
    Bempatrimoniado.ativos(filtros={'numpat': numpat}, limite=1)
    
    # NEW PESSOA TESTS
    print("\n--- TESTE: PESSOA (NOVOS MÉTODOS) ---")
    print(f"Servidores (todos): {len(Pessoa.listar_servidores())}")
    print(f"Estagiários: {len(Pessoa.listar_estagiarios(codundclg))}")
    print(f"Designados (todos): {len(Pessoa.listar_designados(0))}")
    print(f"Designados (servidor): {len(Pessoa.listar_designados(1))}")
    print(f"Docentes (ativos): {len(Pessoa.listar_docentes())}")

    print("\n=== FIM TESTES EXTENDIDOS ===")


if __name__ == "__main__":
    run_tests()
