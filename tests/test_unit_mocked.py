import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from replicado.bempatrimoniado import Bempatrimoniado
from replicado.connection import DB
from replicado.estrutura import Estrutura
from replicado.graduacao import Graduacao
from replicado.pessoa import Pessoa
from replicado.posgraduacao import Posgraduacao
from replicado.utils import (
    clean_string,
    data_mes,
    dia_semana,
    horario_formatado,
    remove_accents,
)


# --- UTILS TESTS ---
def test_clean_string() -> None:
    assert clean_string("  teste  ") == "teste"
    assert clean_string(None) is None
    assert clean_string("teste") == "teste"
    assert clean_string(123) == 123


def test_remove_accents() -> None:
    assert remove_accents("Avidos") == "Avidos"
    assert remove_accents("Árvore") == "Arvore"
    assert remove_accents("coração") == "coracao"


def test_dia_semana() -> None:
    assert dia_semana("2SG") == "segunda-feira"
    assert dia_semana(None) == ""
    assert dia_semana("XYZ") == ""


def test_horario_formatado() -> None:
    assert horario_formatado("0830") == "08:30"
    assert horario_formatado("08:30") == "08:30"
    assert horario_formatado(None) is None


def test_data_mes() -> None:
    assert data_mes(datetime(2023, 1, 1)) == "01/01/2023"
    assert data_mes("2023-01-01") == "01/01/2023"
    assert data_mes("2023-01-01 10:00") == "01/01/2023"
    assert data_mes("invalid") == "invalid"
    assert data_mes(None) is None


# --- CONNECTION TESTS ---
def test_connection_missing_env() -> None:
    # Force reset singleton
    DB._engine = None
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="Variáveis de ambiente de conexão"):
            DB.get_engine()


@patch("replicado.connection.create_engine")
def test_connection_success(mock_create_engine) -> None:
    DB._engine = None
    env_vars = {
        "REPLICADO_HOST": "host",
        "REPLICADO_PORT": "1433",
        "REPLICADO_DATABASE": "db",
        "REPLICADO_USERNAME": "user",
        "REPLICADO_PASSWORD": "pass",
    }
    with patch.dict(os.environ, env_vars):
        engine = DB.get_engine()
        assert engine is not None
        assert mock_create_engine.called
        # Check get_session
        sess = DB.get_session()
        assert sess is not None


def test_cria_filtro_busca() -> None:
    filtros = {"codpes": 12345}
    buscas = {"nompes": "Silva"}
    tipos = {"codpes": "int"}

    where, params = DB.cria_filtro_busca(filtros, buscas, tipos)
    assert "codpes = CONVERT(int, :codpes)" in where
    assert "nompes LIKE :nompes" in where

    # Test DB.execute/fetch/fetch_all generic passthrough if possible to mock
    # They depend on get_engine.
    with patch("replicado.connection.DB.get_engine") as mock_engine_getter:
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_engine_getter.return_value = mock_engine

        # execute
        DB.execute("SELECT 1")
        assert mock_conn.execute.called

        # fetch
        mock_conn.execute.return_value.fetchone.return_value = MagicMock(
            _mapping={"a": " b "}
        )
        res = DB.fetch("SELECT 1")
        assert res["a"] == "b"

        # fetch_all
        mock_conn.execute.return_value._mapping.items.return_value = {
            "a": " b "
        }.items()  # Mock row mapping items
        # Actually in SQLAlchemy 2.0+ it's result._mapping
        row = MagicMock()
        row._mapping = {"k": " v "}
        mock_conn.execute.return_value = [row]
        results = DB.fetch_all("SELECT 1")
        assert results[0]["k"] == "v"

    # Filter/Search branches
    # buscas only
    w, p = DB.cria_filtro_busca({}, {"a": "b"}, {})
    assert "WHERE ( a LIKE :a )" in w

    # filtros and buscas
    w, p = DB.cria_filtro_busca({"f": 1}, {"a": "b"}, {})
    assert "WHERE ( f = :f ) AND ( a LIKE :a )" in w

    # multiple buscas
    w, p = DB.cria_filtro_busca({}, {"a": "b", "c": "d"}, {})
    assert "OR" in w and "c LIKE :c" in w

    # filtros only
    w, p = DB.cria_filtro_busca({"f": 1}, {}, {})
    assert "WHERE ( f = :f )" in w


# --- GRADUACAO TESTS ---
@patch("replicado.connection.DB.fetch")
@patch("replicado.connection.DB.fetch_all")
def test_graduacao_methods(mock_fetch_all, mock_fetch) -> None:
    # Setup mocks
    mock_fetch.return_value = {"nompes": "Teste", "codpes": 123}
    mock_fetch_all.return_value = [
        {
            "codpes": 123,
            "nompes": "Teste",
            "tipvin": "ALUNOGR",
            "sitatl": "A",
            "codundclg": "45",
        }
    ]

    # verifica
    assert Graduacao.verifica(123, 45) is True
    mock_fetch_all.return_value = []
    assert Graduacao.verifica(123, 45) is False

    # listar_ativos
    mock_fetch_all.return_value = [
        {"codpes": 1, "nompes": "A"}
    ]  # Valid data for loop/return
    Graduacao.listar_ativos(codcur=1, ano_ingresso=2020, parte_nome="A")

    # contar_ativos
    mock_fetch.return_value = {"total": 10}
    assert Graduacao.contar_ativos() == 10

    # obter_curso_ativo
    mock_fetch.return_value = {"nomcur": "Matematica"}
    assert Graduacao.obter_curso_ativo(123)["nomcur"] == "Matematica"

    # programa
    Graduacao.programa(123)

    # nome_curso
    mock_fetch.return_value = {"nomcur": "Computacao"}
    assert Graduacao.nome_curso(55) == "Computacao"

    # nome_habilitacao
    mock_fetch.return_value = {"nomhab": "Bacharelado"}
    assert Graduacao.nome_habilitacao(10, 20) == "Bacharelado"

    # disciplinas_unidade
    mock_fetch_all.return_value = [{"coddis": "M1"}]
    Graduacao.listar_disciplinas()

    # creditos_disciplina
    mock_fetch.return_value = {"creaul": 4}
    Graduacao.creditos_disciplina("MAT1")

    # disciplinas_concluidas
    # Needs programa data and obter_curso_ativo data
    # We can mock Graduacao.programa and Graduacao.obter_curso_ativo or mock DB calls
    # Sequence of DB calls in disciplinas_concluidas:
    # 1. programa -> fetch
    # 2. (inside) obter_curso_ativo -> fetch (if called?)
    # Actually disciplinas_concluidas logic:
    #   programa_data = self.programa(codpes)
    #   codpgm = programa_data['codpgm']
    #   ingresso = self.obter_curso_ativo(codpes)
    #   dtainivin = ingresso['dtainivin']
    #   ... fetch_all(query, params)

    mock_fetch.side_effect = [
        {"codpgm": 1},  # programa
        {"dtainivin": "2020-01-01 00:00:00"},  # obter_curso_ativo
        {"nompes": "X"},  # dummy next
    ]
    mock_fetch_all.return_value = [{"coddis": "M1"}]
    Graduacao.disciplinas_concluidas(123, 45)
    mock_fetch.side_effect = None

    # creditos_concluidos
    mock_fetch.side_effect = [{"codpgm": 1}, {"dtainivin": "2020-01-01 00:00:00"}]
    mock_fetch_all.return_value = [{"creaul": 4, "cretrb": 2}]
    Graduacao.creditos_disciplinas_concluidas_aproveitamento_estudos_exterior(123, 45)
    mock_fetch.side_effect = None

    # media_ponderada
    mock_fetch_all.return_value = [
        {"creaul": 4, "cretrb": 2, "notfim2": "8.5", "notfim": "8.5"}
    ]
    Graduacao.obter_media_ponderada(123)

    # verificar_coordenador
    mock_fetch.return_value = {"qtde_cursos": 1}
    assert Graduacao.verificar_coordenador_curso_grad(123) is True

    # listar_intercambios
    mock_fetch_all.return_value = []
    Graduacao.listar_intercambios()

    # Missing methods for full coverage

    mock_fetch_all.return_value = [{"nomcur": "C", "nomhab": "H"}]
    Graduacao.obter_cursos_habilitacoes(45)

    mock_fetch_all.return_value = [
        {"coddis": "D1", "creaul": 4, "cretrb": 0, "notfim": "10"}
    ]
    Graduacao.listar_disciplinas_aluno(123)

    mock_fetch_all.return_value = [{"coddis": "D1", "verdis": 1}]
    Graduacao.disciplinas_curriculo(1, 1)

    mock_fetch_all.return_value = [{"coddis": "D1"}]
    Graduacao.disciplinas_equivalentes_curriculo(1, 1)

    mock_fetch_all.return_value = [{"coddis": "D1"}]
    Graduacao.disciplinas_equivalentes_curriculo(1, 1)

    # setor_aluno
    mock_fetch.side_effect = [{"codcur": 1, "codhab": 1}, {"nomabvset": "Setor X"}]
    Graduacao.setor_aluno(123, 45)
    mock_fetch.side_effect = None

    mock_fetch.return_value = {"total": 5}
    Graduacao.contar_ativos_por_genero("M")

    mock_fetch_all.return_value = [{"codcur": 10}]
    mock_fetch.return_value = {"total": 1}
    Graduacao.verificar_pessoa_graduada_unidade(123)
    Graduacao.verificar_ex_aluno_grad(123, 45)

    # setor_aluno edge case (not found)
    mock_fetch.side_effect = None
    mock_fetch.return_value = None  # obter_curso_ativo returns None/Empty
    res = Graduacao.setor_aluno(123, 45)
    assert res["nomabvset"] == "DEPARTAMENTO NÃO ENCONTRADO"

    # Graduacao extra gaps
    # obter_disciplinas with empty list
    assert Graduacao.obter_disciplinas([]) == []

    # disciplinas_concluidas with no programa
    mock_fetch.return_value = None
    assert Graduacao.disciplinas_concluidas(123, 45) == []

    # creditos_disciplinas_concluidas_aproveitamento_estudos_exterior with no programa
    assert (
        Graduacao.creditos_disciplinas_concluidas_aproveitamento_estudos_exterior(
            123, 45
        )
        == []
    )

    # contar_ativos_por_genero with codcur
    mock_fetch.return_value = {"total": 1}
    Graduacao.contar_ativos_por_genero("F", codcur=10)

    # verificar_pessoa_graduada_unidade with no codigos
    with patch("replicado.graduacao.Graduacao.obter_codigos_cursos") as m_cods:
        m_cods.return_value = []
        assert Graduacao.verificar_pessoa_graduada_unidade(123) is False

    # obter_grade_horaria
    Graduacao.obter_grade_horaria(123)

    # listar_disciplinas_grade_curricular
    Graduacao.listar_disciplinas_grade_curricular(10, 20)

    # obter_intercambio_por_codpes
    Graduacao.obter_intercambio_por_codpes(123)

    # listar_disciplinas_aluno with codpgm and NULL in rstfim
    Graduacao.listar_disciplinas_aluno(123, codpgm=1, rstfim=["A", "NULL"])

    # obter_media_ponderada branching
    mock_fetch_all.return_value = [
        {"creaul": 4, "cretrb": 0, "notfim2": None, "notfim": "10"},  # notfim2 is None
        {
            "creaul": 4,
            "cretrb": 0,
            "notfim2": "abc",
            "notfim": "10",
        },  # ValueError on float conversion
    ]
    Graduacao.obter_media_ponderada(123)


# --- POSGRADUACAO TESTS ---
@patch("replicado.connection.DB.fetch")
@patch("replicado.connection.DB.fetch_all")
def test_posgraduacao_methods(mock_fetch_all, mock_fetch) -> None:
    # verifica
    mock_fetch_all.return_value = [
        {"codpes": 123, "tipvin": "ALUNOPOS", "sitatl": "A", "codundclg": "45"}
    ]
    assert Posgraduacao.verifica(123, 45) is True

    # ativos
    Posgraduacao.ativos(45)

    # contar_ativos
    mock_fetch.return_value = {"total": 50}
    assert Posgraduacao.contar_ativos(codare=10) == 50

    # programas
    mock_fetch_all.return_value = [{"codare": 1, "nomare": "Area 1"}]
    Posgraduacao.programas(45, codcur=10, codare=20)

    # orientadores
    mock_fetch_all.return_value = [{"codpes": 1, "nompes": "O"}]
    Posgraduacao.orientadores(10)

    # disciplinas
    mock_fetch_all.return_value = [{"sgldis": "S", "nomdis": "N"}]
    Posgraduacao.disciplinas_oferecimento(10)

    # ministrante
    mock_fetch_all.return_value = [{"codpes": 1, "nompes": "M"}]
    Posgraduacao.ministrante("M", 1, 1)

    # lista_orientandos_ativos
    mock_fetch_all.return_value = [{"codpes": 999}]
    # Needs internal mock or fetch returns right data
    # Mock result for obter_vinculo_ativo
    mock_fetch.side_effect = [
        {"codpes": 999},  # orientandos call
        {
            "nomcur": "X",
            "nivpgm": "ME",
            "dtainivin": "2010",
            "nomare": "Y",
        },  # obter_vinculo inner call
    ]
    # Actually fetch_all is called first
    # This is complex to mock purely with return values if nested calls exist.
    # Better to mock the method directly.
    with patch("replicado.posgraduacao.Posgraduacao.obter_vinculo_ativo") as m_vinc:
        m_vinc.return_value = {
            "nomcur": "X",
            "nivpgm": "ME",
            "dtainivin": "2010",
            "nomare": "Y",
        }
        Posgraduacao.listar_orientandos_ativos(888)
        Posgraduacao.listar_orientandos_concluidos(888)

    mock_fetch.side_effect = None  # Reset

    # obter_defesas
    mock_fetch_all.return_value = [{"nompes": "A", "dtadfapgm": "2023"}]
    Posgraduacao.obter_defesas(123)

    # listar_disciplinas
    mock_fetch_all.return_value = [{"sgldis": "S", "nomdis": "N"}]
    Posgraduacao.listar_disciplinas()

    # egressos
    mock_fetch_all.return_value = [{"codpes": 1}, {"codpes": 2}]
    Posgraduacao.egressos_area(10)

    mock_fetch_all.return_value = [{"ano": 2020, "quantidade": 10}]
    Posgraduacao.contar_egressos_area_agrupado_por_ano(10)

    # total_pos
    mock_fetch.return_value = {"total": 5}
    Posgraduacao.total_pos_nivel_programa("ME", 45)

    # areas_programas
    # Sequence: 1. programas, 2. areas of course
    mock_fetch_all.side_effect = [
        [{"codcur": 1, "nomcur": "X", "codare": 1, "nomare": "Y"}],
        [{"codare": 10}],
    ]
    mock_fetch.return_value = {"codare": 10, "nomare": "Y"}
    Posgraduacao.areas_programas(45)
    mock_fetch_all.side_effect = None

    # listar_defesas
    Posgraduacao.listar_defesas()

    # obter_vinculo_ativo
    mock_fetch.return_value = {"nomcur": "X"}
    Posgraduacao.obter_vinculo_ativo(123)

    # Posgraduacao extra gaps
    # programas without codundclgi (from env)
    with patch.dict(os.environ, {"REPLICADO_CODUNDCLG": "45"}):
        Posgraduacao.programas()

    # areas_programas with env comma
    with patch.dict(os.environ, {"REPLICADO_CODUNDCLG": "45,88"}):
        mock_fetch_all.side_effect = [[{"codcur": 1}], [{"codare": 1}]]
        mock_fetch.return_value = {"codare": 1, "nomare": "A"}
        Posgraduacao.areas_programas()
        mock_fetch_all.side_effect = None

    # alunos_programa without codare
    with patch("replicado.posgraduacao.Posgraduacao.areas_programas") as m_areas:
        m_areas.return_value = {10: [{"codare": 1}]}
        Posgraduacao.alunos_programa(45, 10)

    # idioma_disciplina without cod
    assert Posgraduacao.idioma_disciplina(None) is None

    # contar_ativos_por_genero with codare
    mock_fetch.return_value = {"total": 1}
    Posgraduacao.contar_ativos_por_genero("F", codare=10)

    # listar_membros_banca with codare and numseqpgm
    Posgraduacao.listar_membros_banca(123, codare=1, numseqpgm=2)

    # aliases
    mock_fetch_all.return_value = []
    Posgraduacao.obter_orientandos_ativos(123)
    Posgraduacao.obter_orientandos_concluidos(123)

    # listar_disciplinas with no env
    with patch.dict(os.environ, {}, clear=True):
        assert Posgraduacao.listar_disciplinas() == []


# --- PESSOA TESTS ---
@patch("replicado.connection.DB.fetch")
@patch("replicado.connection.DB.fetch_all")
def test_pessoa_methods(mock_fetch_all, mock_fetch) -> None:
    # Comprehensive mock data
    mock_data = {
        "codpes": 123,
        "nompes": "Teste",
        "codema": "a@a.com",
        "codddd": "11",
        "numtel": "99999999",
        "nompesttd": "Nome Completo",
        "tipvin": "ALUNOGR",
        "tipvinext": "Servidor",
        "sitatl": "A",
        "codundclg": 45,
        "codset": 1,
        "nomset": "Setor X",
    }
    mock_fetch.return_value = mock_data
    mock_fetch_all.return_value = [mock_data]

    Pessoa.dump(123)
    Pessoa.cracha(123)
    Pessoa.listar_crachas(123)
    Pessoa.emails(123)
    Pessoa.email(123)
    Pessoa.telefones(123)
    Pessoa.obter_endereco(123)

    # procurar_por_nome
    Pessoa.procurar_por_nome("A", fonetico=True, ativos=True)
    Pessoa.procurar_por_nome("A", fonetico=False, ativos=False)

    # obter_nome
    Pessoa.obter_nome(123)
    Pessoa.obter_nome([123, 456])

    # listar_vinculos
    Pessoa.listar_vinculos_ativos(123)
    mock_fetch.return_value = {"total": 10}
    Pessoa.total_vinculo("A", 45)

    # listas novas
    Pessoa.listar_servidores()
    Pessoa.listar_estagiarios(45)
    Pessoa.listar_designados(0)
    Pessoa.listar_designados(1)
    Pessoa.listar_designados(2)
    Pessoa.listar_docentes()

    # Missing branches
    Pessoa.procurar_por_nome("A", tipvin="S", tipvinext="D", codundclgs="45")
    Pessoa.obter_nome([])
    mock_fetch.return_value = None
    assert Pessoa.obter_nome(99999) is None

    # Email branch
    mock_fetch.return_value = None
    assert Pessoa.email(123) is None


# --- ESTRUTURA TESTS ---
@patch("replicado.connection.DB.fetch")
@patch("replicado.connection.DB.fetch_all")
def test_estrutura_methods(mock_fetch_all, mock_fetch) -> None:
    mock_fetch.return_value = {"nomund": "U"}
    mock_fetch_all.return_value = []

    Estrutura.obter_unidade(45)
    Estrutura.listar_locais_unidade(45)
    Estrutura.obter_local(1)
    Estrutura.procurar_local("A")
    Estrutura.dump(1)
    Estrutura.get_chefia_setor(1, substitutos=True)
    Estrutura.get_chefia_setor(1, substitutos=False)
    Estrutura.listar_setores()
    Estrutura.listar_setores(45)

    Estrutura.listar_locais_unidade()
    Estrutura.procurar_local("A", codund=0)
    Estrutura.procurar_local("A", codund=-1)
    Estrutura.procurar_local("A", codund=45)

    Estrutura.listar_unidades()


# --- BEMPATRIMONIADO TESTS ---
@patch("replicado.connection.DB.fetch")
@patch("replicado.connection.DB.fetch_all")
def test_bempatrimoniado_methods(mock_fetch_all, mock_fetch) -> None:
    mock_fetch.return_value = {"numpat": "123", "stabem": "Ativo"}
    mock_fetch_all.return_value = []

    Bempatrimoniado.dump("123")

    # verifica
    assert Bempatrimoniado.verifica("123") is True
    mock_fetch.return_value = {"numpat": "123", "stabem": "Baixado"}
    assert Bempatrimoniado.verifica("123") is False

    Bempatrimoniado.ativos(filtros={"numpat": 1}, buscas={"desbem": "PC"})

    # is_informatica
    mock_fetch.return_value = {"coditmmat": 12513}
    assert Bempatrimoniado.is_informatica("1") is True

    mock_fetch.return_value = {"coditmmat": 99999}
    assert Bempatrimoniado.is_informatica("1") is False

    mock_fetch.return_value = None
    assert Bempatrimoniado.is_informatica("1") is False

    # Exception safe
    mock_fetch.return_value = {"coditmmat": "invalid"}
    assert Bempatrimoniado.is_informatica("1") is False


@patch("replicado.connection.DB.fetch")
@patch("replicado.connection.DB.fetch_all")
def test_coverage_gap_fillers(mock_fetch_all, mock_fetch) -> None:
    # Graduacao
    mock_fetch.return_value = {"nomdis": "D"}
    Graduacao.nome_disciplina("D1")
    mock_fetch.return_value = None
    Graduacao.nome_disciplina("X")

    mock_fetch_all.return_value = [{"coddis": "D1"}, {"coddis": "D2"}]
    Graduacao.obter_disciplinas(["D1", "D2"])

    mock_fetch_all.return_value = [
        {"coddis": "D1", "creaul": 4, "cretrb": 2, "notfim": "10"}
    ]
    Graduacao.listar_disciplinas_aluno(123)

    # Posgraduacao
    mock_fetch_all.return_value = [{"codare": 1, "nomare": "A"}]
    Posgraduacao.programas(45, codcur=1)

    mock_fetch_all.return_value = [{"codpes": 1, "nompes": "O"}]
    Posgraduacao.orientadores(10)

    mock_fetch_all.return_value = [{"sgldis": "S", "nomdis": "N"}]
    Posgraduacao.disciplinas_oferecimento(10)

    mock_fetch_all.return_value = [{"nompes": "A", "dtadfapgm": "2020"}]
    Posgraduacao.obter_defesas(123)

    mock_fetch_all.return_value = [{"sgldis": "S", "nomdis": "N"}]
    Posgraduacao.listar_disciplinas()

    # Graduacao extra gaps
    mock_fetch_all.return_value = [{"coddis": "D1"}]
    Graduacao.listar_disciplinas_aluno_ano_semestre(123, 2023)
    Graduacao.listar_departamentos_de_ensino()

    # Recycled mock for media logic (needs creaul, etc)
    mock_fetch_all.return_value = [
        {"creaul": 4, "cretrb": 2, "notfim2": "8.5", "notfim": "8.5"}
    ]
    Graduacao.obter_media_ponderada_limpa(123)
    Graduacao.obter_media_ponderada_suja(123)

    # Posgraduacao extra gaps
    mock_fetch_all.return_value = [{"sgldis": "S"}]
    Posgraduacao.catalogo_disciplinas(1)

    mock_fetch.return_value = {"nomdis": "N"}
    Posgraduacao.disciplina("S")

    mock_fetch.return_value = {"numofe": 1, "sgldis": "S", "numseqdis": 1}
    mock_fetch_all.return_value = [{"coddis": "D"}]  # inner call to espacoturma
    Posgraduacao.oferecimento("S", 1)

    mock_fetch_all.return_value = [{"coddis": "D"}]
    Posgraduacao.espacoturma("S", 1, 1)

    mock_fetch_all.return_value = [{"codpes": 1}]
    Posgraduacao.alunos_programa(45, 10, 20)

    mock_fetch.return_value = {"dsclin": "Portugues"}
    Posgraduacao.idioma_disciplina("P")

    mock_fetch.return_value = {"total": 10}
    Posgraduacao.contar_ativos_por_genero("M")

    mock_fetch.return_value = {"codpes": 123}
    Posgraduacao.verificar_ex_aluno_pos(123, 45)

    mock_fetch_all.return_value = [{"nompes": "Banca"}]
    Posgraduacao.listar_membros_banca(123)

    mock_fetch_all.return_value = [{"codpes": 1}]
    Posgraduacao.listar_alunos_ativos_programa(1)

    mock_fetch_all.return_value = [{"nomcur": "Prog"}]
    Posgraduacao.listar_programas()
