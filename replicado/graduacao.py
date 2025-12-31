import logging
import os
from datetime import datetime
from typing import Any

from replicado.connection import DB

nlogger = logging.getLogger(__name__)


class Graduacao:
    """
    Classe para métodos relacionados à Graduação.
    """

    @staticmethod
    def obter_nome_social(codpes: int) -> str | None:
        """
        Retorna o nome social registrado na tabela PESSOA.

        Args:
            codpes (int): Número USP.

        Returns:
            Optional[str]: Nome social ou None se não houver.
        """
        query = "SELECT nomcnhpes FROM PESSOA WHERE codpes = :codpes AND stautlnomsoc = 'S'"
        result = DB.fetch(query, {"codpes": codpes})
        return result["nomcnhpes"].strip() if result and result["nomcnhpes"] else None

    @staticmethod
    def obter_vencimento_identidade(codpes: int) -> str | None:
        """
        Retorna a data de validade do documento de identificação.

        Args:
            codpes (int): Número USP.

        Returns:
            Optional[str]: Data de validade (string formatada) ou None.
        """
        query = "SELECT dtafimvalidf FROM PESSOA WHERE codpes = :codpes"
        result = DB.fetch(query, {"codpes": codpes})
        if result and result["dtafimvalidf"]:
            return result["dtafimvalidf"].strftime("%d/%m/%Y")
        return None

    @staticmethod
    def obter_vencimento_passaporte(codpes: int) -> str | None:
        """
        Retorna a data de validade do passaporte se disponível.

        Args:
            codpes (int): Número USP.

        Returns:
            Optional[str]: Data de validade ou None.
        """
        # No esquema Sybase, dados de passaporte podem estar na tabela de documentos complementares
        # ou como um tipo específico em PESSOA se tipdocidf for 'Passap'
        query = """
            SELECT dtafimvalidf FROM PESSOA
            WHERE codpes = :codpes AND tipdocidf = 'Passap'
        """
        result = DB.fetch(query, {"codpes": codpes})
        if result and result["dtafimvalidf"]:
            return result["dtafimvalidf"].strftime("%d/%m/%Y")
        return None

    @staticmethod
    def listar_ingressantes(ano_ingresso: int) -> list[dict[str, Any]]:
        """
        Lista alunos que ingressaram em um determinado ano.

        Args:
            ano_ingresso (int): Ano de ingresso.

        Returns:
            List[Dict[str, Any]]: Lista de ingressantes.
        """
        query = """
            SELECT p.codpes, p.nompes, pg.dtaing
            FROM PESSOA p
            INNER JOIN PROGRAMAGR pg ON p.codpes = pg.codpes
            WHERE YEAR(pg.dtaing) = :ano_ingresso
            ORDER BY p.nompes ASC
        """
        result = DB.fetch_all(query, {"ano_ingresso": ano_ingresso})
        for row in result:
            row["nompes"] = row["nompes"].strip()
        return result

    @staticmethod
    def obter_notas_ingresso(codpes: int) -> list[dict[str, Any]]:
        """
        Detalha as notas de vestibular/SISU do aluno.

        Args:
            codpes (int): Número USP.

        Returns:
            List[Dict[str, Any]]: Lista de notas.
        """
        query = """
            SELECT n.codtipmiaing, t.tipmiaing, n.noting
            FROM NOTASINGRESSOGR n
            INNER JOIN TIPOMATERIAING t ON n.codtipmiaing = t.codtipmiaing
            WHERE n.codpes = :codpes
        """
        result = DB.fetch_all(query, {"codpes": codpes})
        for row in result:
            row["tipmiaing"] = row["tipmiaing"].strip()
        return result

    @staticmethod
    def listar_trancamentos_aluno(codpes: int) -> list[dict[str, Any]]:
        """
        Lista os períodos de trancamento do aluno.

        Args:
            codpes (int): Número USP.

        Returns:
            List[Dict[str, Any]]: Lista de períodos de trancamento.
        """
        query = """
            SELECT h.dtaoco, h.stapgm, h.motstapgm as tiptrc
            FROM HISTPROGGR h
            WHERE h.codpes = :codpes AND (h.stapgm = 'T' OR h.stapgm = 'P')
            ORDER BY h.dtaoco DESC
        """
        result = DB.fetch_all(query, {"codpes": codpes})
        for row in result:
            if row["tiptrc"]:
                row["tiptrc"] = row["tiptrc"].strip()
        return result

    @staticmethod
    def verifica(codpes: int, codundclgi: int) -> bool:
        """
        Verifica se a pessoa é aluna de graduação ativa na unidade indicada.

        Args:
            codpes (int): Número USP.
            codundclgi (int): Código da Unidade e Colegiado.

        Returns:
            bool: True se for aluno ativo na unidade.
        """
        query = "SELECT * FROM LOCALIZAPESSOA WHERE codpes = :codpes"
        result = DB.fetch_all(query, {"codpes": codpes})

        for row in result:
            if (
                row["tipvin"] == "ALUNOGR"
                and row["sitatl"] == "A"
                and int(row["codundclg"]) == codundclgi
            ):
                return True
        return False

    @staticmethod
    def listar_ativos(
        codcur: int | None = None,
        ano_ingresso: int | None = None,
        parte_nome: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Lista alunos de graduação ativos na unidade.

        Args:
            codcur (int, optional): Filtro por código do curso.
            ano_ingresso (int, optional): Filtro por ano de ingresso.
            parte_nome (str, optional): Filtro por parte do nome (busca simples).

        Returns:
            List[Dict[str, Any]]: Lista de alunos.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")

        query_filter = ""
        params = {}

        if parte_nome:
            query_filter = " AND L.nompes LIKE :parteNome "
            params["parteNome"] = f"%{parte_nome}%"

        if codcur:
            query_filter += " AND C.codcur = :codcur"
            params["codcur"] = codcur

        if ano_ingresso:
            query_filter += " AND YEAR(V.dtainivin) = :anoIngresso "
            params["anoIngresso"] = ano_ingresso

        # Usando f-string para o IN clause do codundclg pois é configuração segura do ambiente
        query = f"""
        SELECT L.codpes, L.nompes, L.codema, C.codcur, C.nomcur, H.codhab, H.nomhab, V.dtainivin
        FROM LOCALIZAPESSOA L
        INNER JOIN VINCULOPESSOAUSP V ON (L.codpes = V.codpes) AND (L.codundclg = V.codclg)
        INNER JOIN CURSOGR C ON (V.codcurgrd = C.codcur)
        INNER JOIN HABILITACAOGR H ON (H.codhab = V.codhab)
        WHERE L.tipvin = 'ALUNOGR'
            AND L.codundclg IN ({codundclg})
            AND (V.codcurgrd = H.codcur AND V.codhab = H.codhab)
            {query_filter}
        ORDER BY L.nompes ASC
        """

        return DB.fetch_all(query, params)

    @staticmethod
    def contar_ativos() -> int:
        """
        Retorna contagem de alunos ativos na unidade.

        Returns:
            int: Quantidade de alunos.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")
        query = f"""
            SELECT count(*) as total
            FROM LOCALIZAPESSOA
            WHERE tipvin = 'ALUNOGR'
            AND codundclg IN ({codundclg})
        """
        result = DB.fetch(query)
        return result["total"] if result else 0

    @staticmethod
    def obter_curso_ativo(codpes: int) -> dict[str, Any]:
        """
        Retorna dados do curso de um aluno ativo na unidade.

        Args:
            codpes (int): Número USP.

        Returns:
            Dict[str, Any]: Dados do curso ou dicionário vazio.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")

        # Query baseada em Graduacao.obterCursoAtivo.sql
        sql = f"""
            SELECT L.codpes, L.nompes, C.codcur, C.nomcur, H.codhab, H.nomhab, V.dtainivin, V.codcurgrd
            FROM LOCALIZAPESSOA L
            INNER JOIN VINCULOPESSOAUSP V ON (L.codpes = V.codpes) AND (L.codundclg = V.codclg)
            INNER JOIN CURSOGR C ON (V.codcurgrd = C.codcur)
            INNER JOIN HABILITACAOGR H ON (H.codhab = V.codhab)
            WHERE (L.codpes = :codpes)
                AND (L.tipvin = 'ALUNOGR' AND L.codundclg IN ({codundclg}))
                AND (V.codcurgrd = H.codcur AND V.codhab = H.codhab)
        """
        result = DB.fetch(sql, {"codpes": codpes})
        return result if result else {}

    @staticmethod
    def listar_equivalencias_externas(codpes: int) -> list[dict[str, Any]]:
        """
        Lista as disciplinas aproveitadas de outras instituições.

        Args:
            codpes (int): Número USP.

        Returns:
            List[Dict[str, Any]]: Lista de equivalências externas.
        """
        query = """
            SELECT h.coddis, h.verdis, r.jusrqm as nominstext, d.creaul, d.cretrb
            FROM EQUIVEXTERNAGR h
            INNER JOIN DISCIPLINAGR d ON h.coddis = d.coddis AND h.verdis = d.verdis
            INNER JOIN REQUERIMENTOGR r ON h.codrqm = r.codrqm
            WHERE h.codpes = :codpes
            ORDER BY h.coddis ASC
        """
        result = DB.fetch_all(query, {"codpes": codpes})
        for row in result:
            row["coddis"] = row["coddis"].strip()
            row["nominstext"] = row["nominstext"].strip() if row["nominstext"] else None
        return result

    @staticmethod
    def listar_ministrantes(coddis: str, verdis: int, codtur: str) -> list[dict[str, Any]]:
        """
        Retorna os docentes responsáveis por uma turma específica.

        Args:
            coddis (str): Código da disciplina.
            verdis (int): Versão da disciplina.
            codtur (str): Código da turma.

        Returns:
            List[Dict[str, Any]]: Lista de ministrantes.
        """
        query = """
            SELECT p.codpes, p.nompes, m.dtainiaul, m.dtafimaul
            FROM MINISTRANTE m
            INNER JOIN PESSOA p ON m.codpes = p.codpes
            WHERE m.coddis = :coddis AND m.verdis = :verdis AND m.codtur = :codtur
        """
        params = {"coddis": coddis, "verdis": verdis, "codtur": codtur}
        result = DB.fetch_all(query, params)
        for row in result:
            row["nompes"] = row["nompes"].strip()
        return result

    @staticmethod
    def obter_horario_turma(coddis: str, verdis: int, codtur: str) -> list[dict[str, Any]]:
        """
        Retorna horários e locais de aula de uma turma.

        Args:
            coddis (str): Código da disciplina.
            verdis (int): Versão da disciplina.
            codtur (str): Código da turma.

        Returns:
            List[Dict[str, Any]]: Lista de horários e locais.
        """
        query = """
            SELECT o.diasmnocp, p.horent, p.horsai, o.codlocusp as codesp
            FROM OCUPTURMA o
            INNER JOIN PERIODOHORARIO p ON o.codperhor = p.codperhor
            WHERE o.coddis = :coddis AND o.verdis = :verdis AND o.codtur = :codtur
        """
        params = {"coddis": coddis, "verdis": verdis, "codtur": codtur}
        result = DB.fetch_all(query, params)
        for row in result:
            row["codesp"] = str(row["codesp"]).strip() if row["codesp"] else None
        return result

    @staticmethod
    def contar_vagas_turma(coddis: str, verdis: int, codtur: str) -> dict[str, Any]:
        """
        Retorna o detalhamento de vagas de uma turma.

        Args:
            coddis (str): Código da disciplina.
            verdis (int): Versão da disciplina.
            codtur (str): Código da turma.

        Returns:
            Dict[str, Any]: Detalhes das vagas.
        """
        query = """
            SELECT numvagtur, numvagopt, numvagecr, numins, nummtr, numpmtobg
            FROM TURMAGR
            WHERE coddis = :coddis AND verdis = :verdis AND codtur = :codtur
        """
        params = {"coddis": coddis, "verdis": verdis, "codtur": codtur}
        result = DB.fetch(query, params)
        return result if result else {}

    @staticmethod
    def obter_turma_pratica_vinculada(coddis: str, verdis: int, codtur: str) -> list[dict[str, Any]]:
        """
        Associa turmas teóricas às suas práticas vinculadas.

        Args:
            coddis (str): Código da disciplina teórica.
            verdis (int): Versão da disciplina teórica.
            codtur (str): Código da turma teórica.

        Returns:
            List[Dict[str, Any]]: Lista de turmas práticas vinculadas.
        """
        query = """
            SELECT coddispra, verdispra, codturpra
            FROM TURPRATICA
            WHERE coddisteo = :coddis AND verdisteo = :verdis AND codturteo = :codtur
        """
        params = {"coddis": coddis, "verdis": verdis, "codtur": codtur}
        result = DB.fetch_all(query, params)
        for row in result:
            row["coddispra"] = row["coddispra"].strip()
            row["codturpra"] = row["codturpra"].strip()
        return result

    @staticmethod
    def programa(codpes: int) -> dict[str, Any] | None:
        """
        Retorna dados do programa de graduação do aluno.

        Args:
            codpes (int): Número USP.

        Returns:
            Optional[Dict[str, Any]]: Dados do programa.
        """
        query = """
            SELECT TOP 1 * FROM HISTPROGGR
            WHERE (HISTPROGGR.codpes = :codpes)
            AND (HISTPROGGR.stapgm = 'H' OR HISTPROGGR.stapgm = 'R')
            ORDER BY HISTPROGGR.dtaoco DESC
        """
        return DB.fetch(query, {"codpes": codpes})

    @staticmethod
    def nome_curso(codcur: int) -> str | None:
        """
        Retorna o nome do curso.

        Args:
            codcur (int): Código do curso.

        Returns:
            Optional[str]: Nome do curso.
        """
        query = "SELECT TOP 1 nomcur FROM CURSOGR WHERE codcur = :codcur"
        result = DB.fetch(query, {"codcur": codcur})
        return result["nomcur"] if result else None

    @staticmethod
    def nome_habilitacao(codhab: int, codcur: int) -> str | None:
        """
        Retorna o nome da habilitação.

        Args:
            codhab (int): Código da habilitação.
            codcur (int): Código do curso.

        Returns:
            Optional[str]: Nome da habilitação.
        """
        query = """
            SELECT TOP 1 nomhab FROM HABILITACAOGR
            WHERE codhab = :codhab AND codcur = :codcur
        """
        result = DB.fetch(query, {"codhab": codhab, "codcur": codcur})
        return result["nomhab"] if result else None

    @staticmethod
    def obter_cursos_habilitacoes(codundclgi: int) -> list[dict[str, Any]]:
        """
        Obtém cursos e habilitações de uma unidade.

        Args:
            codundclgi (int): Código da unidade.

        Returns:
            List[Dict[str, Any]]: Lista de cursos e habilitações.
        """
        query = """
            SELECT CURSOGR.*, HABILITACAOGR.* FROM CURSOGR, HABILITACAOGR
            WHERE (CURSOGR.codclg = :codundclgi) AND (CURSOGR.codcur = HABILITACAOGR.codcur)
            AND ( (CURSOGR.dtaatvcur IS NOT NULL) AND (CURSOGR.dtadtvcur IS NULL) )
            AND ( (HABILITACAOGR.dtaatvhab IS NOT NULL) AND (HABILITACAOGR.dtadtvhab IS NULL) )
            ORDER BY CURSOGR.nomcur, HABILITACAOGR.nomhab ASC
        """
        return DB.fetch_all(query, {"codundclgi": codundclgi})

    @staticmethod
    def listar_disciplinas() -> list[dict[str, Any]]:
        """
        Lista disciplinas de graduação ativas na unidade.

        Returns:
            List[Dict[str, Any]]: Lista de disciplinas.
        """
        codundclgs = os.getenv("REPLICADO_CODUNDCLG")

        # Baseado em Graduacao.listarDisciplinas.sql
        sql = f"""
            SELECT D1.*
            FROM DISCIPLINAGR AS D1
            WHERE (D1.verdis = (SELECT MAX(D2.verdis) FROM DISCIPLINAGR AS D2 WHERE (D2.coddis = D1.coddis)))
            AND D1.coddis IN (SELECT coddis FROM DISCIPGRCODIGO WHERE DISCIPGRCODIGO.codclg IN ({codundclgs}))
            AND D1.dtadtvdis IS NULL
            AND D1.dtaatvdis IS NOT NULL
            ORDER BY D1.nomdis ASC
        """
        return DB.fetch_all(sql)

    @staticmethod
    def nome_disciplina(coddis: str) -> str | None:
        """
        Retorna o nome da disciplina.

        Args:
            coddis (str): Código da disciplina.

        Returns:
            Optional[str]: Nome da disciplina.
        """
        query = """
            SELECT D1.nomdis FROM DISCIPLINAGR AS D1
            WHERE (D1.verdis = (
                SELECT MAX(D2.verdis) FROM DISCIPLINAGR AS D2 WHERE (D2.coddis = D1.coddis)
            )) AND (D1.coddis = :coddis)
        """
        result = DB.fetch(query, {"coddis": coddis})
        return result["nomdis"] if result else None

    @staticmethod
    def obter_disciplinas(arr_coddis: list[str]) -> list[dict[str, Any]]:
        """
        Método para obter as disciplinas de graduação oferecidas na unidade.
        """
        if not arr_coddis:
            return []

        params = {}
        or_clauses = []
        for i, sgldis in enumerate(arr_coddis):
            key = f"sgldis_{i}"
            or_clauses.append(f"(D1.coddis LIKE :{key})")
            params[key] = f"{sgldis}%"

        where_clause = " OR ".join(or_clauses)

        query = f"""
            SELECT D1.* FROM DISCIPLINAGR AS D1
            WHERE (D1.verdis = (
                SELECT MAX(D2.verdis) FROM DISCIPLINAGR AS D2 WHERE (D2.coddis = D1.coddis)
            )) AND ({where_clause})
            ORDER BY D1.coddis ASC
        """
        return DB.fetch_all(query, params)

    @staticmethod
    def disciplinas_concluidas(codpes: int, codundclgi: int) -> list[dict[str, Any]]:
        """
        Método para trazer as disciplinas, status e créditos concluídos.
        """
        programa_data = Graduacao.programa(codpes)
        if not programa_data:
            return []

        # Original PHP logic:
        # $programa = $programa['codpgm'];
        # $ingresso = self::curso($codpes, $codundclgi);
        # $ingresso = substr($ingresso['dtainivin'], 0, 4);

        codpgm = programa_data["codpgm"]
        curso_data = Graduacao.obter_curso_ativo(codpes)
        # Note: PHP uses self::curso which is deprecated for obterCursoAtivo
        # but obterCursoAtivo uses codundclg from env, PHP version passed codundclgi.
        # Assuming obtaining active course is sufficient.
        if not curso_data or "dtainivin" not in curso_data:
            return []

        dtainivin = str(curso_data["dtainivin"])
        ingresso = dtainivin[:4] if dtainivin else datetime.now().year  # Fallback?

        query = """
            SELECT DISTINCT H.coddis, H.rstfim, D.creaul, D.cretrb FROM HISTESCOLARGR AS H, DISCIPLINAGR AS D
            WHERE H.coddis = D.coddis AND H.verdis = D.verdis AND H.codpes = convert(int, :codpes) AND H.codpgm = convert(int, :codpgm)
            AND	(H.codtur = '0' OR CONVERT(INT, CONVERT(CHAR(4), H.codtur)) >= :ingresso_int)
            AND (H.rstfim = 'A' OR H.rstfim = 'D' OR (H.rstfim IS NULL AND H.stamtr = 'M' AND H.codtur LIKE :ingresso_like))
            ORDER BY H.coddis
        """

        params = {
            "codpes": codpes,
            "codpgm": codpgm,
            "ingresso_int": int(ingresso),
            "ingresso_like": f"{ingresso}1%",
        }

        return DB.fetch_all(query, params)

    @staticmethod
    def creditos_disciplina(coddis: str) -> int | None:
        """
        Método para trazer os créditos de uma disciplina.
        """
        query = """
            SELECT D1.creaul FROM DISCIPLINAGR AS D1
            WHERE (D1.verdis = (
                SELECT MAX(D2.verdis) FROM DISCIPLINAGR AS D2 WHERE (D2.coddis = D1.coddis)
            )) AND (D1.coddis = :coddis)
        """
        result = DB.fetch(query, {"coddis": coddis})
        return result["creaul"] if result else None

    @staticmethod
    def creditos_disciplinas_concluidas_aproveitamento_estudos_exterior(
        codpes: int, codundclgi: int
    ) -> list[dict[str, Any]]:
        """
        Créditos atribuídos por Aproveitamento de Estudos no exterior.
        """
        programa_data = Graduacao.programa(codpes)
        if not programa_data:
            return []
        codpgm = programa_data["codpgm"]

        query = """
            SELECT DISTINCT H.coddis, R.creaulatb
            FROM HISTESCOLARGR AS H, DISCIPLINAGR AS D, REQUERHISTESC AS R
            WHERE H.coddis = D.coddis AND H.verdis = D.verdis AND H.codpes = convert(int, :codpes) AND H.codpgm = convert(int, :codpgm)
            AND H.coddis = R.coddis AND H.verdis = R.verdis AND H.codtur = R.codtur AND H.codpes = R.codpes
            AND (H.rstfim = 'D') AND ((R.creaulatb IS NOT NULL) OR (R.creaulatb > 0))
            ORDER BY H.coddis
        """
        return DB.fetch_all(query, {"codpes": codpes, "codpgm": codpgm})

    @staticmethod
    def disciplinas_curriculo(codcur: int, codhab: int) -> list[dict[str, Any]]:
        """
        Disciplinas (grade curricular) para um currículo atual no JúpiterWeb.
        """
        query = """
            SELECT G.coddis, D.nomdis, G.verdis, G.numsemidl, G.tipobg
            FROM GRADECURRICULAR G INNER JOIN DISCIPLINAGR D ON (G.coddis = D.coddis AND G.verdis = D.verdis)
            WHERE G.codcrl IN (SELECT TOP 1 codcrl
            FROM CURRICULOGR
            WHERE codcur = :codcur AND codhab = convert(int, :codhab)
            ORDER BY dtainicrl DESC)
        """
        return DB.fetch_all(query, {"codcur": codcur, "codhab": codhab})

    @staticmethod
    def disciplinas_equivalentes_curriculo(
        codcur: int, codhab: int
    ) -> list[dict[str, Any]]:
        """
        Disciplinas equivalentes de um currículo atual no JúpiterWeb.
        """
        query = """
            SELECT G.codeqv, G.coddis, G.verdis, GC.tipobg, E.coddis as coddis_eq, E.verdis as verdis_eq
            FROM GRUPOEQUIVGR G INNER JOIN EQUIVALENCIAGR E ON (G.codeqv = E.codeqv)
            INNER JOIN GRADECURRICULAR GC ON (GC.coddis = G.coddis AND GC.verdis = G.verdis AND G.codcrl = GC.codcrl)
            WHERE G.codcrl IN (SELECT TOP 1 codcrl
            FROM CURRICULOGR
            WHERE codcur = :codcur AND codhab = convert(int, :codhab)
            ORDER BY dtainicrl DESC)
        """
        return DB.fetch_all(query, {"codcur": codcur, "codhab": codhab})

    @staticmethod
    def setor_aluno(codpes: int, codundclgi: int) -> dict[str, Any]:
        """
        Departamento de Ensino do Aluno de Graduação.
        """
        curso = Graduacao.obter_curso_ativo(codpes)
        if not curso:
            return {"nomabvset": "DEPARTAMENTO NÃO ENCONTRADO"}

        codcur = curso["codcur"]
        codhab = curso["codhab"]

        query = """
            SELECT TOP 1 L.nomabvset FROM CURSOGRCOORDENADOR AS C
            INNER JOIN LOCALIZAPESSOA AS L ON C.codpesdct = L.codpes
            WHERE C.codcur = CONVERT(INT, :codcur) AND C.codhab = CONVERT(INT, :codhab)
        """
        result = DB.fetch(query, {"codcur": codcur, "codhab": codhab})
        if not result:
            return {"nomabvset": "DEPARTAMENTO NÃO ENCONTRADO"}
        return result

    @staticmethod
    def contar_ativos_por_genero(sexpes: str, codcur: int | None = None) -> int:
        """
        Método para retornar o total de alunos de graduação do gênero.
        """
        unidades = os.getenv("REPLICADO_CODUNDCLG")

        query = f"""
            SELECT COUNT (DISTINCT L.codpes) as total
            FROM LOCALIZAPESSOA L
            INNER JOIN PESSOA P ON P.codpes = L.codpes
            INNER JOIN HABILPROGGR H ON H.codpes = L.codpes
            WHERE L.tipvin = 'ALUNOGR'
            AND L.codundclg IN ({unidades})
            AND P.sexpes = :sexpes
        """

        params = {"sexpes": sexpes}
        if codcur:
            query += " AND H.codcur = CONVERT(INT, :codcur) "
            params["codcur"] = codcur

        result = DB.fetch(query, params)
        return result["total"] if result else 0

    @staticmethod
    def verificar_coordenador_curso_grad(codpes: int) -> bool:
        """
        Retorna se codpes é coordenador de curso de graduação.
        """
        query = """
            SELECT COUNT(codpesdct) as qtde_cursos
            FROM CURSOGRCOORDENADOR
            WHERE codpesdct = convert(int, :codpes) AND (getdate() BETWEEN dtainicdn AND dtafimcdn)
        """
        result = DB.fetch(query, {"codpes": codpes})
        return result["qtde_cursos"] > 0 if result else False

    @staticmethod
    def verificar_pessoa_graduada_unidade(codpes: int) -> bool:
        """
        Método para retornar se uma pessoa é graduada nos cursos da unidade.
        """
        cursos_cods = Graduacao.obter_codigos_cursos()
        if not cursos_cods:
            return False

        cursos_in = ",".join(map(str, cursos_cods))

        query = f"""
            SELECT p.codpes
            FROM PROGRAMAGR p INNER JOIN HABILPROGGR h ON (p.codpes = h.codpes AND p.codpgm = h.codpgm)
            WHERE p.codpes = convert(int, :codpes)
            AND (tipencpgm LIKE :tipencpgm OR tipenchab LIKE :tipenchab)
            AND h.dtaclcgru IS NOT NULL
            AND h.codcur IN ({cursos_in})
        """
        # Note: Original uses "Conclus_o" for LIKE to match variations or encoding issues?
        params = {"codpes": codpes, "tipencpgm": "Conclus_o", "tipenchab": "Conclus_o"}

        result = DB.fetch(query, params)
        return bool(result)

    @staticmethod
    def verificar_ex_aluno_grad(codpes: int, codorg: int) -> bool:
        """
        Verifica se a pessoa é Ex-Aluna de Graduação.
        """
        query = """
            SELECT codpes from TITULOPES
            WHERE codpes = convert(int, :codpes)
            AND codcur IS NOT NULL
            AND codorg = convert(int, :codorg)
        """
        result = DB.fetch(query, {"codpes": codpes, "codorg": codorg})
        return bool(result)

    @staticmethod
    def obter_grade_horaria(codpes: int) -> list[dict[str, Any]]:
        """
        Retorna grade horária atual.
        """
        current_date = datetime.now()
        semester = 2 if current_date.month > 6 else 1
        current_term_code = f"{current_date.year}{semester}"  # ex: 20251

        query = """
            SELECT h.coddis, h.codtur, o.diasmnocp, p.horent, p.horsai FROM HISTESCOLARGR h
            INNER JOIN OCUPTURMA o ON (h.coddis = o.coddis AND h.codtur = o.codtur)
            INNER JOIN PERIODOHORARIO p ON (o.codperhor = p.codperhor)
            WHERE h.codpes = convert(int,:codpes) AND h.codtur LIKE :term_like
        """
        # Original uses '%{$current}%' which seems to act as simple contains or specific format.
        # Usually codtur is YYYYSEMESTRE... or similar.
        return DB.fetch_all(
            query, {"codpes": codpes, "term_like": f"%{current_term_code}%"}
        )

    @staticmethod
    def obter_codigos_cursos() -> list[int]:
        """
        Retornar apenas códigos de curso de Graduação da unidade.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")

        query = """
            SELECT codcur
            FROM CURSOGR
            WHERE codclg = convert(int, :codclg)
        """
        # Note: PHP logic handles multiple codundclg via getenv but binds only one :codclg?
        # If REPLICADO_CODUNDCLG has commas, convert(int, '45,88') fails in SQL Server.
        # The PHP actually did: $param = ['codclg' => $codigo_unidade];
        # If env is "45,46", this might fail if not split.
        # We will assume single unit for now or first one if comma.
        codclg = int(codundclg.split(",")[0]) if "," in codundclg else codundclg

        result = DB.fetch_all(query, {"codclg": codclg})
        return [row["codcur"] for row in result]

    @staticmethod
    def listar_disciplinas_grade_curricular(
        codcur: int, codhab: int, tipobg: str = "O"
    ) -> list[dict[str, Any]]:
        """
        Retorna lista das disciplinas de uma grade curricular.
        """
        query = """
            SELECT G.coddis, D.nomdis
            FROM GRADECURRICULAR G
            INNER JOIN DISCIPLINAGR D ON (G.coddis = D.coddis AND G.verdis = D.verdis)
            WHERE G.codcrl IN (
                SELECT TOP 1 C.codcrl
                FROM CURRICULOGR C
                WHERE C.codcur = convert(int, :codcur) AND C.codhab = convert(int, :codhab)
                AND C.dtafimcrl IS NULL
                ORDER BY C.dtainicrl DESC
            ) AND G.tipobg = :tipobg
        """
        return DB.fetch_all(
            query, {"codcur": codcur, "codhab": codhab, "tipobg": tipobg}
        )

    @staticmethod
    def listar_intercambios() -> list[dict[str, Any]]:
        """
        Retorna lista com os intercâmbios internacionais ativos.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")
        query = f"""
            SELECT DISTINCT I.codpes, O.nomorgpnt, P.nompas from INTERCAMBIOUSPORGAO I
            INNER JOIN LOCALIZAPESSOA L ON I.codpes = L.codpes
            INNER JOIN ORGAOPRETENDENTE O ON I.codorg = O.codorg
            INNER JOIN PAIS P ON O.codpas = P.codpas
            WHERE L.tipvin = 'ALUNOGR'
            AND I.dtafimitb > GETDATE()
            AND L.codundclg IN ({codundclg})
        """
        return DB.fetch_all(query)

    @staticmethod
    def obter_intercambio_por_codpes(codpes: int) -> list[dict[str, Any]]:
        """
        Retorna os dados sobre o intercâmbio do aluno.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")
        query = f"""
            SELECT O.nomorgpnt, P.nompas, I.dtainiitb, I.dtafimitb from INTERCAMBIOUSPORGAO I
            INNER JOIN LOCALIZAPESSOA L ON I.codpes = L.codpes
            INNER JOIN ORGAOPRETENDENTE O ON I.codorg = O.codorg
            INNER JOIN PAIS P ON O.codpas = P.codpas
            WHERE L.tipvin = 'ALUNOGR'
            AND I.dtafimitb > GETDATE()
            AND L.codundclg IN ({codundclg})
            AND I.codpes = convert(int,:codpes)
        """
        return DB.fetch_all(query, {"codpes": codpes})

    @staticmethod
    def listar_requerimentos_aluno(codpes: int) -> list[dict[str, Any]]:
        """
        Lista o histórico de requerimentos do aluno.

        Args:
            codpes (int): Número USP.

        Returns:
            List[Dict[str, Any]]: Lista de requerimentos.
        """
        query = """
            SELECT r.codrqm, r.tiprqm, r.starqm, r.dtacadrqm, r.rstfim
            FROM REQUERIMENTOGR r
            WHERE r.codpes = :codpes
            ORDER BY r.dtacadrqm DESC
        """
        result = DB.fetch_all(query, {"codpes": codpes})
        for row in result:
            row["tiprqm"] = row["tiprqm"].strip()
            row["starqm"] = row["starqm"].strip()
            if row["rstfim"]:
                row["rstfim"] = row["rstfim"].strip()
        return result

    @staticmethod
    def obter_detalhes_requerimento(codrqm: int) -> dict[str, Any]:
        """
        Retorna justificativa e parecer de um requerimento específico.

        Args:
            codrqm (int): Código do requerimento.

        Returns:
            Dict[str, Any]: Detalhes do requerimento.
        """
        query = """
            SELECT tiprqm, starqm, dtacadrqm, rstfim
            FROM REQUERIMENTOGR
            WHERE codrqm = :codrqm
        """
        result = DB.fetch(query, {"codrqm": codrqm})
        if result:
            if result.get("rstfim"):
                result["rstfim"] = result["rstfim"].strip()
        return result if result else {}

    @staticmethod
    def listar_alunos_especiais(coddis: str, verdis: int, codtur: str) -> list[dict[str, Any]]:
        """
        Lista alunos não regulares (especiais) inscritos na turma.

        Args:
            coddis (str): Código da disciplina.
            verdis (int): Versão da disciplina.
            codtur (str): Código da turma.

        Returns:
            List[Dict[str, Any]]: Lista de alunos especiais.
        """
        # Alunos especiais costumam ter registros em HISTESCOLARGR sem vínculo ALUNOGR direto na unidade
        # ou com tiping específico na tabela de ingresso.
        query = """
            SELECT p.codpes, p.nompes, h.rstfim
            FROM HISTESCOLARGR h
            INNER JOIN PESSOA p ON h.codpes = p.codpes
            INNER JOIN PROGRAMAGR pr ON h.codpes = pr.codpes AND h.codpgm = pr.codpgm
            WHERE h.coddis = :coddis AND h.verdis = :verdis AND h.codtur = :codtur
            AND pr.tiping = 'Aluno Especial'
        """
        params = {"coddis": coddis, "verdis": verdis, "codtur": codtur}
        result = DB.fetch_all(query, params)
        for row in result:
            row["nompes"] = row["nompes"].strip()
            if row["rstfim"]:
                row["rstfim"] = row["rstfim"].strip()
        return result

    @staticmethod
    def listar_disciplinas_por_prefixo(pfxdis: str) -> list[dict[str, Any]]:
        """
        Lista disciplinas filtrando pelo prefixo do departamento.

        Args:
            pfxdis (str): Prefixo da disciplina (ex: 'MAC', '430').

        Returns:
            List[Dict[str, Any]]: Lista de disciplinas.
        """
        query = """
            SELECT DISTINCT d.coddis, d.nomdis
            FROM DISCIPLINAGR d
            WHERE d.coddis LIKE :pfxdis
            AND d.verdis = (SELECT MAX(v.verdis) FROM DISCIPLINAGR v WHERE v.coddis = d.coddis)
        """
        result = DB.fetch_all(query, {"pfxdis": f"{pfxdis}%"})
        for row in result:
            row["coddis"] = row["coddis"].strip()
            row["nomdis"] = row["nomdis"].strip()
        return result

    @staticmethod
    def obter_normas_habilitacao(codcur: int, codhab: int) -> list[dict[str, Any]]:
        """
        Retorna as normas de reconhecimento da habilitação.

        Args:
            codcur (int): Código do curso.
            codhab (int): Código da habilitação.

        Returns:
            List[Dict[str, Any]]: Lista de normas.
        """
        query = """
            SELECT tippubnor, dtapubnorgrd, dtafimvalnor, numnorgrd
            FROM NORMARECONHECHABILGR
            WHERE codcurgrd = :codcur AND codhab = :codhab
        """
        result = DB.fetch_all(query, {"codcur": codcur, "codhab": codhab})
        for row in result:
            row["tippubnor"] = row["tippubnor"].strip()
        return result

    @staticmethod
    def obter_data_limite_conclusao(codpes: int) -> str | None:
        """
        Retorna o prazo máximo para formatura conforme o programa atual.

        Args:
            codpes (int): Número USP.

        Returns:
            Optional[str]: Data limite formatada ou None.
        """
        query = """
            SELECT dtamaxccl FROM PROGRAMAGR
            WHERE codpes = :codpes AND stapgm IN ('A', 'H', 'R')
            ORDER BY dtaing DESC
        """
        result = DB.fetch(query, {"codpes": codpes})
        if result and result["dtamaxccl"]:
            return result["dtamaxccl"].strftime("%d/%m/%Y")
        return None

    @staticmethod
    def listar_alunos_por_status_programa(stapgm: str) -> list[dict[str, Any]]:
        """
        Lista alunos filtrando pelo status do programa (A, H, T, P, J, C, etc).

        Args:
            stapgm (str): Status do programa (A=Ativo, T=Trancado, J=Jubilado, etc).

        Returns:
            List[Dict[str, Any]]: Lista de alunos.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")
        query = f"""
            SELECT p.codpes, p.nompes, pg.codpgm, pg.stapgm
            FROM PESSOA p
            INNER JOIN PROGRAMAGR pg ON p.codpes = pg.codpes
            INNER JOIN LOCALIZAPESSOA l ON p.codpes = l.codpes
            WHERE pg.stapgm = :stapgm AND l.codundclg IN ({codundclg})
            AND l.tipvin = 'ALUNOGR'
        """
        result = DB.fetch_all(query, {"stapgm": stapgm})
        for row in result:
            row["nompes"] = row["nompes"].strip()
            row["stapgm"] = row["stapgm"].strip()
        return result

    @staticmethod
    def listar_disciplinas_com_vagas_extracurriculares() -> list[dict[str, Any]]:
        """
        Lista disciplinas que oferecem vagas para alunos extracurriculares.

        Returns:
            List[Dict[str, Any]]: Lista de disciplinas com vagas extras.
        """
        query = """
            SELECT d.coddis, d.nomdis, t.codtur, t.numvagecr
            FROM TURMAGR t
            INNER JOIN DISCIPLINAGR d ON t.coddis = d.coddis AND t.verdis = d.verdis
            WHERE t.numvagecr > 0 AND t.statur = 'A'
        """
        result = DB.fetch_all(query)
        for row in result:
            row["coddis"] = row["coddis"].strip()
            row["nomdis"] = row["nomdis"].strip()
            row["codtur"] = row["codtur"].strip()
        return result

    @staticmethod
    def listar_disciplinas_aluno(
        codpes: int,
        codpgm: int | None = None,
        rstfim: list[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Lista as disciplinas cursadas por um aluno.
        """
        if rstfim is None:
            rstfim = ["A", "RN", "RA", "RF"]
        params = {"codpes": codpes}

        if codpgm is None:
            query_codpgm = "H.codpgm = (SELECT MAX(H2.codpgm) FROM HISTESCOLARGR H2 WHERE H2.codpes = convert(int,:codpes))"
        else:
            query_codpgm = "H.codpgm = CONVERT(INT,:codpgm)"
            params["codpgm"] = codpgm

        # Handle NULL in rstfim list
        query_rstfim_null = ""
        valid_rstfim = [r for r in rstfim if r != "NULL"]
        if "NULL" in rstfim:
            query_rstfim_null = "OR H.rstfim IS NULL"

        rstfim_str = "', '".join(valid_rstfim)
        rstfim_clause = f"(H.rstfim IN ('{rstfim_str}') {query_rstfim_null})"

        query = f"""
            SELECT D.coddis, D.nomdis, D.creaul, D.cretrb
                , H.notfim, H.notfim2, H.rstfim, H.codtur
            FROM HISTESCOLARGR H
            INNER JOIN DISCIPLINAGR D ON H.coddis = D.coddis AND H.verdis = D.verdis
            WHERE H.codpes = convert(int,:codpes)
                AND {rstfim_clause}
                AND {query_codpgm}
        """

        return DB.fetch_all(query, params)

    @staticmethod
    def obter_media_ponderada(
        codpes: int,
        codpgm: int | None = None,
        rstfim: list[str] = None,
    ) -> float:
        """
        Retorna a média ponderada.
        """
        if rstfim is None:
            rstfim = ["A", "RN", "RA", "RF"]
        disciplinas = Graduacao.listar_disciplinas_aluno(codpes, codpgm, rstfim)

        creditos = 0
        soma = 0.0

        for row in disciplinas:
            creaul = row["creaul"] or 0
            cretrb = row["cretrb"] or 0
            total_cre = creaul + cretrb

            # PHP: $nota = $row['notfim2'] ?: $row['notfim'];
            # If notfim2 is not null/empty use it, else notfim
            nota_val = row["notfim2"]
            if nota_val is None or nota_val == "":
                nota_val = row["notfim"]

            if nota_val is not None and str(nota_val).strip() != "":
                try:
                    # Replace comma with dot if necessary usually sql returns float or decimal
                    # python driver might return float direct
                    nota = float(nota_val)
                    creditos += total_cre
                    soma += nota * total_cre
                except ValueError:
                    pass

        return round(soma / creditos, 1) if creditos > 0 else 0.0

    @staticmethod
    def obter_media_ponderada_limpa(codpes: int, codpgm: int | None = None) -> float:
        return Graduacao.obter_media_ponderada(codpes, codpgm, ["A"])

    @staticmethod
    def obter_media_ponderada_suja(codpes: int, codpgm: int | None = None) -> float:
        return Graduacao.obter_media_ponderada(codpes, codpgm, ["A", "RN", "RA", "RF"])

    @staticmethod
    def listar_disciplinas_aluno_ano_semestre(
        codpes: int,
        ano_semestre: int,
        rstfim: list[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Retorna lista de disciplinas cursadas pelo aluno em determinado semestre.
        """
        # Handle NULL in rstfim list
        if rstfim is None:
            rstfim = ["A", "AR", "R", "RN", "RA", "RF", "NULL"]
        query_rstfim_null = ""
        valid_rstfim = [r for r in rstfim if r != "NULL"]
        if "NULL" in rstfim:
            query_rstfim_null = "OR H.rstfim IS NULL"

        rstfim_str = "', '".join(valid_rstfim)
        rstfim_clause = f"(H.rstfim IN ('{rstfim_str}') {query_rstfim_null})"

        query = f"""
            SELECT
                DISTINCT PROF.nompes, PROF.codpes, O.codtur, T.tiptur, D.coddis, D.nomdis, D.verdis, H.rstfim
            FROM
                ALUNOGR A INNER JOIN PROGRAMAGR PR ON A.codpes=PR.codpes
                INNER JOIN HISTESCOLARGR H ON PR.codpes=H.codpes AND PR.codpgm=H.codpgm
                INNER JOIN TURMAGR T ON H.coddis=T.coddis AND H.verdis=T.verdis AND H.codtur=T.codtur
                INNER JOIN DISCIPLINAGR D ON T.coddis=D.coddis AND T.verdis=D.verdis
                INNER JOIN OCUPTURMA O ON T.coddis=O.coddis AND T.verdis=O.verdis AND T.codtur=O.codtur
                INNER JOIN MINISTRANTE M ON O.coddis=M.coddis AND O.codtur=M.codtur AND O.verdis=M.verdis
                INNER JOIN PESSOA PROF ON M.codpes=PROF.codpes
            WHERE
                A.codpes = :codpes
                AND T.codtur LIKE :anoSemestre
                AND H.stamtr = 'M'
                AND {rstfim_clause}
            ORDER BY
                D.nomdis
        """

        return DB.fetch_all(
            query, {"codpes": codpes, "anoSemestre": f"{ano_semestre}%"}
        )

    @staticmethod
    def listar_departamentos_de_ensino() -> list[dict[str, Any]]:
        """
        Retorna lista com os departamentos de ensino da unidade.
        """
        codundclgs = os.getenv("REPLICADO_CODUNDCLG")
        query = f"""
            SELECT * FROM SETOR
            WHERE codund IN ({codundclgs})
            AND tipset = 'Departamento de Ensino'
            AND dtadtvset IS NULL
            ORDER BY nomset
        """
        return DB.fetch_all(query)
