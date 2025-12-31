import argparse
import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.getcwd())

from replicado import (
    AEX,
    CartaoUSP,
    CEU,
    Bempatrimoniado,
    Beneficio,
    Convenio,
    Estrutura,
    Financeiro,
    Graduacao,
    Lattes,
    Pesquisa,
    Pessoa,
    Posgraduacao,
)
from replicado.connection import DB
from replicado.utils import (
    clean_string,
    data_mes,
    dia_semana,
    horario_formatado,
    remove_accents,
)

load_dotenv()

# --- CONFIGURAÇÃO DE LOGGING PARA O TESTE ---
logging.basicConfig(
    level=logging.ERROR,  # Default to ERROR to keep output clean, can be changed via args
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("manual_test")


class TestReport:
    def __init__(self):
        self.results = []

    def add(self, modulo, status, level, detalhes=""):
        self.results.append(
            {"modulo": modulo, "status": status, "level": level, "detalhes": detalhes}
        )

    def print_summary(self, target_level):
        print("\n" + "=" * 70)
        print(f"              RESUMO DOS TESTES MANUAIS (NÍVEL {target_level})")
        print("=" * 70)
        print(f"{'MÓDULO':<20} {'NÍVEL':<8} {'STATUS':<12} {'DETALHES'}")
        print("-" * 70)
        for res in self.results:
            print(
                f"{res['modulo']:<20} {res['level']:<8} {res['status']:<12} {res['detalhes']}"
            )
        print("=" * 70)


# --- SEÇÕES DE TESTE ---


def test_utils(ctx, report):
    print("\n--- TESTE: UTILS ---")
    ops = [
        f"Clean string: '{clean_string('  teste  ')}'",
        f"Remove accents: {remove_accents('Ávidos')}",
        f"Dia semana 2SG: {dia_semana('2SG')}",
        f"Horario 0830: {horario_formatado('0830')}",
        f"Data mes (datetime): {data_mes(datetime.now())}",
        f"Data mes (str): {data_mes('2023-01-01 10:00:00')}",
    ]
    for op in ops:
        print(op)
    report.add("UTILS", "✅ SUCESSO", 1, f"{len(ops)} operações")


def test_connection(ctx, report):
    print("\n--- TESTE: CONEXÃO ---")
    engine = DB.get_engine()
    print(f"Engine: {engine}")
    # Test a raw fetch
    res = DB.fetch("SELECT TOP 1 getdate() as agora")
    print(f"DB Time: {res['agora']}")
    report.add(
        "CONEXÃO", "✅ SUCESSO", 1, str(engine.url.render_as_string(hide_password=True))
    )


def test_estrutura(ctx, report):
    print("\n--- TESTE: ESTRUTURA ---")
    codund = ctx["codundclg"]
    u = Estrutura.obter_unidade(codund)
    
    if u:
        print(f"Unidade: {u['nomund']} ({codund})")
        print("Obter unidade:", u.get("nomund"))
        locais = Estrutura.listar_locais_unidade(codund)
        print(f"Locais na unidade {codund}: {len(locais)}")
        detalhes = f"{u['nomund']}, {len(locais)} locais"

        if locais:
            codlocusp = locais[0]["codlocusp"]
            print(
                f"Obter local {codlocusp}:",
                Estrutura.obter_local(codlocusp).get("nomlocusp"),
            )
            print(f"Procurar local '1': {len(Estrutura.procurar_local('1', codund))}")

        setores = Estrutura.listar_setores(ctx["codundclg"])
        if setores:
            codset = setores[0]["codset"]
            print(f"Setor {codset}: {Estrutura.dump(codset).get('nomset')}")
            chefia = Estrutura.get_chefia_setor(codset)
            print(f"Chefia do setor {codset}: {len(chefia)} pessoas")
            detalhes += f", {len(setores)} setores"
            print(f"Contato do setor {codset}: {Estrutura.obter_contato_setor(codset)}")
            print(f"Servidores do setor {codset}: {len(Estrutura.listar_servidores_setor(codset))}")

        print(f"Colegiados da unidade: {len(Estrutura.listar_colegiados(ctx['codundclg']))}")
        print(f"Dados Fiscais (CNPJ): {Estrutura.obter_dados_fiscais(ctx['codundclg']).get('numcnpj')}")
        print(f"Chefias da unidade: {len(Estrutura.listar_chefias_unidade(ctx['codundclg']))}")
        print(f"Departamentos da unidade: {len(Estrutura.listar_departamentos(ctx['codundclg']))}")

    report.add("ESTRUTURA", "✅ SUCESSO", 1, detalhes)


def test_pessoa_basic(ctx, report):
    print("\n--- TESTE: PESSOA (BÁSICO) ---")
    # Tenta achar um codpes ativo qualquer
    query = "SELECT TOP 1 codpes FROM LOCALIZAPESSOA WHERE sitatl='A'"
    res = DB.fetch(query)
    if res:
        ctx["codpes"] = res["codpes"]
        dump = Pessoa.dump(ctx["codpes"])
        print(f"Pessoa encontrada: {dump.get('nompes')} ({ctx['codpes']})")
        print(f"Email: {Pessoa.email(ctx['codpes'])}")
        print(f"Vínculos Ativos: {len(Pessoa.listar_vinculos_ativos(ctx['codpes']))}")
        report.add("PESSOA_BASIC", "✅ SUCESSO", 1, f"Codpes: {ctx['codpes']}")
    else:
        report.add("PESSOA_BASIC", "⚠️ AVISO", 1, "Nenhuma pessoa encontrada")


def test_graduacao(ctx, report):
    print("\n--- TESTE: GRADUAÇÃO ---")
    ativos = Graduacao.contar_ativos()
    print(f"Ativos: {ativos}")
    print(f"Ativos Masculinos: {Graduacao.contar_ativos_por_genero('M')}")
    print(f"Ativos Femininos: {Graduacao.contar_ativos_por_genero('F')}")

    lista = Graduacao.listar_ativos()
    if lista:
        aluno = lista[0]
        ctx["codpes"] = aluno["codpes"]
        print(f"Testando com Aluno: {aluno['nompes']} ({aluno['codpes']})")
        print(f"Verifica: {Graduacao.verifica(aluno['codpes'], ctx['codundclg'])}")

        curso_ativo = Graduacao.obter_curso_ativo(aluno["codpes"])
        if curso_ativo:
            codcur = curso_ativo.get("codcur")
            codhab = curso_ativo.get("codhab")
            print(f"Curso: {curso_ativo.get('nomcur')} ({codcur})")
            print(f"Nome Curso (by id): {Graduacao.nome_curso(codcur)}")
            if codcur and codhab:
                print(f"Nome Habilitação: {Graduacao.nome_habilitacao(codhab, codcur)}")
                print(
                    f"Disciplinas Currículo: {len(Graduacao.disciplinas_curriculo(codcur, codhab))}"
                )

        print(
            f"Programa: {Graduacao.programa(aluno['codpes']).get('nomcur') if Graduacao.programa(aluno['codpes']) else 'N/A'}"
        )
        print(
            f"Setor Aluno: {Graduacao.setor_aluno(aluno['codpes'], ctx['codundclg'])}"
        )
        print(
            f"Média Ponderada (L/S): {Graduacao.obter_media_ponderada_limpa(aluno['codpes'])} / {Graduacao.obter_media_ponderada_suja(aluno['codpes'])}"
        )
        print(f"Grade Horária: {len(Graduacao.obter_grade_horaria(aluno['codpes']))}")
        print(f"Disciplinas Aluno: {len(Graduacao.listar_disciplinas_aluno(aluno['codpes']))}")
        print(f"Notas Ingresso: {len(Graduacao.obter_notas_ingresso(aluno['codpes']))}")
        print(f"Trancamentos: {len(Graduacao.listar_trancamentos_aluno(aluno['codpes']))}")
        print(f"Equivalências Externas: {len(Graduacao.listar_equivalencias_externas(aluno['codpes']))}")
        print(f"Disciplinas Concluídas: {len(Graduacao.disciplinas_concluidas(aluno['codpes'], ctx['codundclg']))}")
        print(f"Créditos Exterior: {Graduacao.creditos_disciplinas_concluidas_aproveitamento_estudos_exterior(aluno['codpes'], ctx['codundclg'])}")
        print(f"Requerimentos Aluno: {len(Graduacao.listar_requerimentos_aluno(aluno['codpes']))}")

        if curso_ativo:
            print(f"Disciplinas Equivalentes Currículo: {len(Graduacao.disciplinas_equivalentes_curriculo(codcur, codhab))}")
            print(f"Normas Habilitação: {len(Graduacao.obter_normas_habilitacao(codcur, codhab))}")

        disciplinas = Graduacao.listar_disciplinas()
        print(f"Disciplinas Unidade: {len(disciplinas)}")
        if disciplinas:
            coddis = disciplinas[0]["coddis"]
            print(f"Nome Disciplina {coddis}: {Graduacao.nome_disciplina(coddis)}")
            print(f"Créditos {coddis}: {Graduacao.creditos_disciplina(coddis)}")

        print(f"Nome Social: {Graduacao.obter_nome_social(aluno['codpes'])}")
        print(f"Vencimento Identidade: {Graduacao.obter_vencimento_identidade(aluno['codpes'])}")
        print(f"Vencimento Passaporte: {Graduacao.obter_vencimento_passaporte(aluno['codpes'])}")

        print(f"Códigos Cursos Unidade: {len(Graduacao.obter_codigos_cursos())}")
        print(f"Disciplinas com Vagas Extras: {len(Graduacao.listar_disciplinas_com_vagas_extracurriculares())}")
        print(f"Alunos Ativos (Status A): {len(Graduacao.listar_alunos_por_status_programa('A'))}")

        ano_atual = datetime.now().year
        print(f"Ingressantes {ano_atual}: {len(Graduacao.listar_ingressantes(ano_atual))}")

        report.add("GRADUAÇÃO", "✅ SUCESSO", 2, f"{ativos} ativos")
    else:
        report.add("GRADUAÇÃO", "⚠️ AVISO", 2, "Nenhum aluno encontrado")


def test_posgraduacao(ctx, report):
    print("\n--- TESTE: PÓS-GRADUAÇÃO ---")
    ativos = Posgraduacao.contar_ativos()
    print(f"Ativos: {ativos}")
    print(f"Disciplinas Pós: {len(Posgraduacao.listar_disciplinas())}")
    print(f"Defesas: {len(Posgraduacao.listar_defesas())}")

    programas = Posgraduacao.programas(ctx["codundclg"])
    print(f"Programas: {len(programas)}")
    if programas:
        prog = programas[0]
        codare = prog["codare"]
        print(f"Área {codare}: {prog['nomcur']}")
        print(f"Orientadores da área: {len(Posgraduacao.orientadores(codare))}")
        print(f"Alunos ativos na área: {len(Posgraduacao.ativos(ctx['codundclg']))}")

        alunos = Posgraduacao.ativos(ctx["codundclg"])
        if alunos:
            aluno = alunos[0]
            print(
                f"Vínculo Aluno: {Posgraduacao.obter_vinculo_ativo(aluno['codpes']).get('nomcur')}"
            )
            print(f"Bancas Aluno: {len(Posgraduacao.listar_membros_banca(aluno['codpes']))}")
            print(f"Qualificações: {len(Posgraduacao.listar_qualificacoes(aluno['codpes']))}")
            print(f"Coorientações: {len(Posgraduacao.listar_coorientacoes(aluno['codpes']))}")
            print(f"Trabalho Conclusão: {Posgraduacao.obter_tese_dissertacao(aluno['codpes']).get('tittes') if Posgraduacao.obter_tese_dissertacao(aluno['codpes']) else 'N/A'}")
            print(f"Atividades Aluno: {len(Posgraduacao.listar_atividades_aluno(aluno['codpes']))}")

            print(
                f"Egressos da área {codare}: {len(Posgraduacao.egressos_area(codare))}"
            )
            print(f"Disciplinas Oferecimento: {len(Posgraduacao.disciplinas_oferecimento(codare))}")
            print(f"Catálogo Disciplinas: {len(Posgraduacao.catalogo_disciplinas(codare))}")
            print(f"Inscrições Área: {len(Posgraduacao.listar_inscricoes_area(codare))}")
            print(f"Linhas Pesquisa: {len(Posgraduacao.listar_linhas_pesquisa(codare))}")

        report.add(
            "PÓS-GRADUAÇÃO", "✅ SUCESSO", 2, f"{ativos} ativos, {len(programas)} progs"
        )
    else:
        report.add("PÓS-GRADUAÇÃO", "⚠️ AVISO", 2, "Nenhum programa encontrado")


def test_lattes(ctx, report):
    print("\n--- TESTE: LATTES ---")
    
    codpes = ctx.get("codpes")
    
    # Valida se o codpes atual tem Lattes. Se não, busca um novo especificamente para este teste.
    if not codpes or not Lattes.id(codpes):
        print(f"Codpes atual ({codpes}) sem Lattes. Buscando candidato com XML Lattes...")
        try:
             # Busca alguém que tenha arquivo XML
             res = DB.fetch_all("SELECT TOP 1 codpes FROM DIM_PESSOA_XMLUSP WHERE imgarqxml IS NOT NULL")
             if res:
                 codpes = res[0]['codpes']
                 print(f"Novo candidato Lattes selecionado: {codpes}")
        except Exception as e:
             print(f"Erro ao buscar candidato Lattes: {e}")

    if codpes:
        id_lattes = Lattes.id(codpes)
        print(f"ID Lattes para {codpes}: {id_lattes}")
        print(f"Data Atualização: {Lattes.retornar_data_ultima_atualizacao(codpes)}")
        resumo = Lattes.retornar_resumo_cv(codpes)
        print(f"Resumo (prefix): {str(resumo)[:50]}...")

        # Métodos de listagem (retornam False se falhar ou vazio se não houver registros)
        def safe_len(data):
            return len(data) if isinstance(data, list) else 0

        print(f"Artigos/Qualis: {safe_len(Lattes.listar_artigos_com_qualis(codpes))}")
        print(f"Capítulos: {safe_len(Lattes.listar_capitulos_livros(codpes))}")
        print(f"Trabalhos Anais: {safe_len(Lattes.listar_trabalhos_anais(codpes))}")
        print(f"Trabalhos Técnicos: {safe_len(Lattes.listar_trabalhos_tecnicos(codpes))}")
        print(f"Apresentação Trabalho: {safe_len(Lattes.listar_apresentacao_trabalho(codpes))}")
        print(f"Organização Evento: {safe_len(Lattes.listar_organizacao_evento(codpes))}")
        print(f"Banca Mestrado: {safe_len(Lattes.retornar_banca_mestrado(codpes))}")
        print(f"Banca Doutorado: {safe_len(Lattes.retornar_banca_doutorado(codpes))}")
        print(f"Métricas Citação: {Lattes.obter_metricas_citacao(codpes)}")
        print(f"Citações Anual: {safe_len(Lattes.listar_citacoes_anual(codpes))}")
        print(f"Projetos Pesquisa: {len(Lattes.listar_projetos_pesquisa(codpes))}")
        print(f"Áreas Conhecimento: {len(Lattes.listar_areas_conhecimento(codpes))}")
        print(f"Gênero Lattes: {Lattes.retornar_genero_pesquisador(codpes)}")
        print(f"Linhas Pesquisa: {safe_len(Lattes.listar_linhas_pesquisa(codpes))}")

        report.add("LATTES", "✅ SUCESSO", 2, f"ID: {id_lattes}")
    else:
        report.add("LATTES", "⚠️ AVISO", 2, "Sem codpes para testar")


def test_pesquisa(ctx, report):
    print("\n--- TESTE: PESQUISA ---")
    ic = Pesquisa.listar_iniciacao_cientifica(somente_ativos=True)
    print(f"ICs Ativas: {len(ic)}")
    colabs = Pesquisa.listar_pesquisadores_colaboradores_ativos()
    print(f"Colaboradores Ativos: {len(colabs)}")
    pd = Pesquisa.listar_pesquisa_pos_doutorandos()
    print(f"Pós-doutorandos: {len(pd)}")
    print(f"Contagem PD por ano: {Pesquisa.contar_pd_por_ano()}")
    print(f"Contagem PD últimos 12 meses: {Pesquisa.contar_pd_por_ultimos_12_meses()}")
    report.add("PESQUISA", "✅ SUCESSO", 2, f"{len(ic)} ICs, {len(pd)} PDs")


def test_pessoa_full(ctx, report):
    print("\n--- TESTE: PESSOA (FULL) ---")
    
    # Lógica de seleção com Retry e Heurística de Qualidade
    if "codpes" not in ctx:
        print("Selecionando pessoa para testes (Priorizando quem tem Lattes XML)...")
        # Busca candidatos que tenham XML do Lattes
        query_candidates = """
            SELECT TOP 30 L.codpes 
            FROM LOCALIZAPESSOA L
            INNER JOIN DIM_PESSOA_XMLUSP X ON L.codpes = X.codpes
            WHERE L.sitatl='A' 
            AND X.imgarqxml IS NOT NULL
        """
        try:
             candidates = DB.fetch_all(query_candidates)
        except Exception:
             # Fallback se a query falhar (ex: tabela DIM não acessível)
             candidates = DB.fetch_all("SELECT TOP 30 codpes FROM LOCALIZAPESSOA WHERE sitatl='A'")


        selected_codpes = None
        attempts = 0
        max_attempts = 3
        
        for cand in candidates:
            if attempts >= max_attempts:
                break
                
            c = cand['codpes']
            # Heurística: Tem Lattes? (Indica perfil acadêmico mais rico para testes)
            if Lattes.id(c):
                selected_codpes = c
                print(f"DEBUG: Candidato {c} possui Lattes. Selecionado.")
                break
            else:
                # Se não tem Lattes, usamos como fallback se não acharmos ninguém melhor
                if selected_codpes is None:
                    selected_codpes = c
            
            attempts += 1
            
        if selected_codpes:
            ctx['codpes'] = selected_codpes
            print(f"Pessoa selecionada: {selected_codpes}")
        else:
            print("AVISO: Nenhuma pessoa ativa encontrada.")

    if "codpes" in ctx:
        codpes = ctx["codpes"]
        print(f"Testando FULL para {codpes}")
        print(f"Emails: {Pessoa.emails(codpes)}")
        print(f"Telefones: {Pessoa.telefones(codpes)}")
        print(f"Endereço: {Pessoa.obter_endereco(codpes)}")
        print(f"Crachás: {len(Pessoa.listar_crachas(codpes))}")
        print(f"Vínculos Ativos: {len(Pessoa.listar_vinculos_ativos(codpes))}")

        servidores = Pessoa.listar_servidores()
        docentes = Pessoa.listar_docentes()
        estagiarios = Pessoa.listar_estagiarios(ctx["codundclg"])
        print(
            f"Global: {len(servidores)} serv, {len(docentes)} doc, {len(estagiarios)} estag"
        )

        nome = Pessoa.dump(codpes).get("nompes")
        if nome:
            print(
                f"Procurar por nome '{nome[:10]}': {len(Pessoa.procurar_por_nome(nome[:10]))}"
            )

        print(f"Obter nome: {Pessoa.obter_nome(codpes)}")
        print(f"Diversidade: {Pessoa.obter_diversidade(codpes)}")
        print(f"Total Alunos Grad Ativos Unidade: {Pessoa.total_vinculo('ALUNOGR', ctx['codundclg'])}")
        print(f"Designados: {len(Pessoa.listar_designados())}")
        print(f"AEX: {len(Pessoa.listar_aex(codpes))}")
        print(f"Cursos Extensão: {len(Pessoa.listar_cursos_extensao(codpes))}")
        print(f"Nome Social: {Pessoa.obter_nome_social(codpes)}")
        print(f"Titulações: {len(Pessoa.listar_titulacoes(codpes))}")
        print(f"Premiações: {len(Pessoa.listar_premiacoes(codpes))}")
        print(f"Professores Seniores: {len(Pessoa.listar_professores_seniores(ctx['codundclg']))}")
        print(f"Membros Colegiado 1 (exemplo): {len(Pessoa.listar_membros_colegiado(1))}")
        print(f"Dados Complementares: {Pessoa.obter_dados_servidor_complementar(codpes)}")

        report.add("PESSOA_FULL", "✅ SUCESSO", 2, f"{len(servidores)} serv totais")
    else:
        report.add("PESSOA_FULL", "⚠️ AVISO", 2, "Sem codpes")


def test_beneficio(ctx, report):
    print("\n--- TESTE: BENEFÍCIO ---")
    beneficios = Beneficio.listar_beneficios()
    print(f"Benefícios ativos: {len(beneficios)}")
    print(f"Monitores Pró-Aluno: {len(Beneficio.listar_monitores_pro_aluno('1'))}")
    report.add("BENEFÍCIO", "✅ SUCESSO", 3, f"{len(beneficios)} ativos")


def test_ceu(ctx, report):
    print("\n--- TESTE: CEU ---")
    cursos = CEU.listar_cursos()
    print(f"Cursos CEU (ano atual): {len(cursos)}")
    print(f"Cursos Ativos (Inscrições): {len(CEU.listar_cursos_ativos())}")
    if cursos:
        c = cursos[0]
        print(f"Detalhes Curso {c['codcurceu']}: {CEU.detalhes_curso(c['codcurceu'], c['codedicurceu']).get('nomcurceu')}")
    report.add("CEU", "✅ SUCESSO", 3, f"{len(cursos)} cursos")


def test_convenio(ctx, report):
    print("\n--- TESTE: CONVÊNIO ---")
    convenios = Convenio.listar_convenios_academicos_internacionais()
    print(f"Convênios Internacionais Ativos: {len(convenios)}")
    if convenios:
        cv = convenios[0]
        print(f"Coordenadores Convênio {cv['codcvn']}: {len(Convenio.listar_coordenadores_convenio(cv['codcvn']))}")
        print(f"Organizações Convênio {cv['codcvn']}: {len(Convenio.listar_organizacoes_convenio(cv['codcvn']))}")
    report.add("CONVÊNIO", "✅ SUCESSO", 3, f"{len(convenios)} ativos")


def test_financeiro(ctx, report):
    print("\n--- TESTE: FINANCEIRO ---")
    centros = Financeiro.listar_centros_despesas()
    print(f"Centros de Despesas: {len(centros)}")
    if centros:
        codunddsp = centros[0]["codunddsp"]
        print(f"Estoque Unidade {codunddsp}: {len(Financeiro.listar_estoque_unidade(codunddsp))}")
        print(f"Sugestão Reposição {codunddsp}: {len(Financeiro.sugerir_reposicao(codunddsp))}")
        print(f"Hierarquia Financeira {codunddsp}: {len(Financeiro.obter_hierarquia_financeira(codunddsp))}")
        print(f"Convênios Financeiros {ctx['codundclg']}: {len(Financeiro.listar_convenios_financeiros(ctx['codundclg']))}")
        
    print(f"Bens por Responsável (ctx): {len(Financeiro.listar_bens_por_responsavel(ctx.get('codpes', 0)))}")
    print(f"Doações Recebidas {ctx['codundclg']}: {len(Financeiro.listar_doacoes_recebidas(ctx['codundclg']))}")
    print(f"Status Bens {ctx['codundclg']}: {Financeiro.contar_bens_por_status(ctx['codundclg'])}")
    print(f"Buscar Local USP 'SALA': {len(Financeiro.buscar_local_usp('SALA'))}")
    
    report.add("FINANCEIRO", "✅ SUCESSO", 3, f"{len(centros)} centros")


def test_bempatrimoniado(ctx, report):
    print("\n--- TESTE: BEMPATRIMONIADO ---")
    bens = Bempatrimoniado.ativos(limite=5)
    print(f"Exemplos de bens: {len(bens)}")
    if bens:
        numpat = bens[0]["numpat"]
        print(f"Verifica Bem {numpat}: {Bempatrimoniado.verifica(numpat)}")
        print(f"É Informática {numpat}: {Bempatrimoniado.is_informatica(numpat)}")
        
        detalhes = Financeiro.obter_detalhes_bem(numpat)
        if detalhes:
            print(f"Detalhes Bem Financeiro: {detalhes.get('tipbem')}")
            codbem = detalhes.get('codbem')
            if codbem:
                print(f"Preço Médio Bem {codbem}: {Financeiro.obter_preco_medio(codbem)}")
                print(f"Atributos Material {codbem}: {len(Financeiro.listar_atributos_material(codbem))}")
                print(f"Detalhar Item {codbem}: {Financeiro.detalhar_item_material(codbem).get('nomgrpitmmat') if Financeiro.detalhar_item_material(codbem) else 'N/A'}")

    report.add("BEMPATRIMONIADO", "✅ SUCESSO", 3, f"{len(bens)} listados")


def test_cartao(ctx, report):
    print("\n--- TESTE: CARTÃO USP ---")
    if "codpes" in ctx:
        codpes = ctx["codpes"]
        print(f"Verificar Acesso {codpes}: {CartaoUSP.verificar_acesso(codpes)}")
        print(f"Crachá Ativo {codpes}: {bool(CartaoUSP.buscar_cracha_ativo(codpes))}")
        print(f"Solicitações {codpes}: {len(CartaoUSP.listar_solicitacoes(codpes))}")
    report.add("CARTÃO USP", "✅ SUCESSO", 3, "Validado")


def test_aex(ctx, report):
    print("\n--- TESTE: AEX ---")
    atividades = AEX.listar_atividades()
    print(f"Catálogo AEX Unidade: {len(atividades)}")
    if atividades:
        a = atividades[0]
        print(f"Busca por Código {a['codaex']}: {AEX.buscar_por_codigo(a['codaex']).get('titaex')}")
        print(f"Inscritos na Atividade: {len(AEX.listar_inscritos(a['codaex'], a['veraex']))}")
    report.add("AEX", "✅ SUCESSO", 3, f"{len(atividades)} atividades")





# --- EXECUTOR PRINCIPAL ---


def run_tests():
    parser = argparse.ArgumentParser(description="Testes Manuais do Replicado")
    parser.add_argument(
        "--level",
        type=int,
        default=3,
        choices=[1, 2, 3],
        help="Nível de profundidade (1-3)",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Habilita logs de debug (queries SQL)"
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger("replicado.connection").setLevel(logging.DEBUG)
        logging.getLogger("manual_test").setLevel(logging.DEBUG)

    level = args.level
    print(f"=== INICIANDO TESTES MANUAIS - NÍVEL {level} ===")

    codundclg_env = os.getenv("REPLICADO_CODUNDCLG")
    if not codundclg_env:
        print("ERRO: REPLICADO_CODUNDCLG não definido")
        return

    ctx = {
        "codundclg": (
            int(codundclg_env.split(",")[0])
            if "," in codundclg_env
            else int(codundclg_env)
        ),
        "level": level,
    }
    report = TestReport()

    # Mapeamento de testes por nível
    # Mapeamento de testes por nível
    # (Nome, Função) - Todos rodam, a função decide a profundidade baseada em ctx['level']
    test_list = [
        ("UTILS", test_utils),
        ("CONEXÃO", test_connection),
        ("ESTRUTURA", test_estrutura),
        ("PESSOA", test_pessoa_full), # Unificando testes de pessoal
        ("GRADUAÇÃO", test_graduacao),
        ("PÓS-GRADUAÇÃO", test_posgraduacao),
        ("LATTES", test_lattes),
        ("PESQUISA", test_pesquisa),
        ("BENEFÍCIO", test_beneficio),
        ("CEU", test_ceu),
        ("CONVÊNIO", test_convenio),
        ("FINANCEIRO", test_financeiro),
        ("BEMPATRIMONIADO", test_bempatrimoniado),
        ("CARTÃO USP", test_cartao),
        ("AEX", test_aex),
    ]

    for name, func in test_list:
        try:
            func(ctx, report)
        except Exception as e:
            print(f"❌ FALHA EM {name}: {e}")
            report.add(name, "❌ ERRO", level, str(e))

    report.print_summary(level)
    print("\n=== FIM DOS TESTES ===")


if __name__ == "__main__":
    run_tests()
