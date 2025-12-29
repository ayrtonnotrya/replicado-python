import argparse
import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.getcwd())

from replicado import (
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
    unidades = Estrutura.listar_unidades()
    detalhes = "Nenhuma unidade"
    if unidades:
        u = unidades[0]
        codund = u["codund"]
        print(f"Unidade: {u['nomund']} ({codund})")
        print("Obter unidade:", Estrutura.obter_unidade(codund).get("nomund"))
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
        print(
            f"Disciplinas Aluno: {len(Graduacao.listar_disciplinas_aluno(aluno['codpes']))}"
        )

        disciplinas = Graduacao.listar_disciplinas()
        print(f"Disciplinas Unidade: {len(disciplinas)}")
        if disciplinas:
            coddis = disciplinas[0]["coddis"]
            print(f"Nome Disciplina {coddis}: {Graduacao.nome_disciplina(coddis)}")
            print(f"Créditos {coddis}: {Graduacao.creditos_disciplina(coddis)}")

        print(
            f"Cursos/Habilitações: {len(Graduacao.obter_cursos_habilitacoes(ctx['codundclg']))}"
        )
        print(
            f"Departamentos Ensino: {len(Graduacao.listar_departamentos_de_ensino())}"
        )
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
            print(
                f"Egressos da área {codare}: {len(Posgraduacao.egressos_area(codare))}"
            )

        report.add(
            "PÓS-GRADUAÇÃO", "✅ SUCESSO", 2, f"{ativos} ativos, {len(programas)} progs"
        )
    else:
        report.add("PÓS-GRADUAÇÃO", "⚠️ AVISO", 2, "Nenhum programa encontrado")


def test_lattes(ctx, report):
    print("\n--- TESTE: LATTES ---")
    if "codpes" in ctx:
        codpes = ctx["codpes"]
        id_lattes = Lattes.id(codpes)
        print(f"ID Lattes para {codpes}: {id_lattes}")
        print(f"Data Atualização: {Lattes.retornar_data_ultima_atualizacao(codpes)}")
        resumo = Lattes.retornar_resumo_cv(codpes)
        print(f"Resumo (prefix): {str(resumo)[:50]}...")

        # Métodos de listagem (retornam False se falhar ou vazio se não houver registros)
        def safe_len(data):
            return len(data) if isinstance(data, list) else 0

        print(f"Artigos: {safe_len(Lattes.listar_artigos(codpes))}")
        print(f"Livros: {safe_len(Lattes.listar_livros_publicados(codpes))}")
        print(f"Prêmios: {safe_len(Lattes.listar_premios(codpes))}")
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
    report.add("PESQUISA", "✅ SUCESSO", 2, f"{len(ic)} ICs, {len(pd)} PDs")


def test_pessoa_full(ctx, report):
    print("\n--- TESTE: PESSOA (FULL) ---")
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

        report.add("PESSOA_FULL", "✅ SUCESSO", 2, f"{len(servidores)} serv totais")
    else:
        report.add("PESSOA_FULL", "⚠️ AVISO", 2, "Sem codpes")


def test_beneficio(ctx, report):
    print("\n--- TESTE: BENEFÍCIO ---")
    beneficios = Beneficio.listar_beneficios()
    print(f"Benefícios ativos: {len(beneficios)}")
    report.add("BENEFÍCIO", "✅ SUCESSO", 3, f"{len(beneficios)} ativos")


def test_ceu(ctx, report):
    print("\n--- TESTE: CEU ---")
    cursos = CEU.listar_cursos()
    print(f"Cursos CEU (ano atual): {len(cursos)}")
    report.add("CEU", "✅ SUCESSO", 3, f"{len(cursos)} cursos")


def test_convenio(ctx, report):
    print("\n--- TESTE: CONVÊNIO ---")
    convenios = Convenio.listar_convenios_academicos_internacionais()
    print(f"Convênios Internacionais Ativos: {len(convenios)}")
    report.add("CONVÊNIO", "✅ SUCESSO", 3, f"{len(convenios)} ativos")


def test_financeiro(ctx, report):
    print("\n--- TESTE: FINANCEIRO ---")
    centros = Financeiro.listar_centros_despesas()
    print(f"Centros de Despesas: {len(centros)}")
    report.add("FINANCEIRO", "✅ SUCESSO", 3, f"{len(centros)} centros")


def test_bempatrimoniado(ctx, report):
    print("\n--- TESTE: BEMPATRIMONIADO ---")
    bens = Bempatrimoniado.ativos(limite=10)
    print(f"Exemplos de bens: {len(bens)}")
    report.add("BEMPATRIMONIADO", "✅ SUCESSO", 3, f"{len(bens)} listados")


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
        )
    }
    report = TestReport()

    # Mapeamento de testes por nível
    # (Nome, Nível Mínimo, Função)
    test_list = [
        ("UTILS", 1, test_utils),
        ("CONEXÃO", 1, test_connection),
        ("ESTRUTURA", 1, test_estrutura),
        ("PESSOA_BASIC", 1, test_pessoa_basic),
        ("GRADUAÇÃO", 2, test_graduacao),
        ("PÓS-GRADUAÇÃO", 2, test_posgraduacao),
        ("LATTES", 2, test_lattes),
        ("PESQUISA", 2, test_pesquisa),
        ("PESSOA_FULL", 2, test_pessoa_full),
        ("BENEFÍCIO", 3, test_beneficio),
        ("CEU", 3, test_ceu),
        ("CONVÊNIO", 3, test_convenio),
        ("FINANCEIRO", 3, test_financeiro),
        ("BEMPATRIMONIADO", 3, test_bempatrimoniado),
    ]

    for name, min_level, func in test_list:
        if level >= min_level:
            try:
                func(ctx, report)
            except Exception as e:
                print(f"❌ FALHA EM {name}: {e}")
                report.add(name, "❌ ERRO", min_level, str(e))

    report.print_summary(level)
    print("\n=== FIM DOS TESTES ===")


if __name__ == "__main__":
    run_tests()
