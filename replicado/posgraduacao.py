import logging
import os
from datetime import datetime
from typing import Any

from replicado.connection import DB

nlogger = logging.getLogger(__name__)


class Posgraduacao:
    """
    Classe para métodos relacionados à Pós-Graduação.
    """

    @staticmethod
    def verifica(codpes: int, codundclgi: int) -> bool:
        """
        Verifica se aluno (codpes) tem matrícula ativa na pós-graduação da unidade.
        """
        query = "SELECT * FROM LOCALIZAPESSOA WHERE codpes = :codpes"
        result = DB.fetch_all(query, {"codpes": codpes})

        for row in result:
            if (
                row["tipvin"] == "ALUNOPOS"
                and row["sitatl"] == "A"
                and int(row["codundclg"]) == codundclgi
            ):
                return True
        return False

    @staticmethod
    def ativos(codundclgi: int) -> list[dict[str, Any]]:
        """
        Retorna todos alunos de pós-graduação ativos na unidade.
        """
        query = """
            SELECT LOCALIZAPESSOA.*, PESSOA.* FROM LOCALIZAPESSOA
            INNER JOIN PESSOA ON (LOCALIZAPESSOA.codpes = PESSOA.codpes)
            WHERE LOCALIZAPESSOA.tipvin = 'ALUNOPOS'
            AND LOCALIZAPESSOA.codundclg = :codundclgi
            AND LOCALIZAPESSOA.sitatl = 'A'
            ORDER BY PESSOA.nompes ASC
        """
        return DB.fetch_all(query, {"codundclgi": codundclgi})

    @staticmethod
    def contar_ativos(codare: int | None = None) -> int:
        """
        Retorna quantidade alunos de pós-graduação.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")

        # Query baseada em Posgraduacao.contarAtivos.sql
        query = f"""
            SELECT COUNT(DISTINCT l.codpes) as total FROM LOCALIZAPESSOA l
            JOIN PESSOA p ON p.codpes = l.codpes
            JOIN HISTPROGRAMA h ON h.codpes = l.codpes
            WHERE l.tipvin = 'ALUNOPOS'
            AND l.codundclg IN ({codundclg})
        """

        params = {}
        if codare:
            query += " AND (h.codare = :codare)"
            params["codare"] = codare

        result = DB.fetch(query, params)
        # Note: Original PHP might return computed or total?
        # PHP wrapper usually returns row. 'count(*)' usually needs alias or is accessed by index.
        # My DB.fetch returns a dict.
        # If sql has no alias, keys might be empty or 'computed'.
        # I added alias 'total' in my query string above.
        return result["total"] if result else 0

    @staticmethod
    def programas(
        codundclgi: int | None = None,
        codcur: int | None = None,
        codare: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Retorna programas de pós-graduação da unidade.
        """
        if not codundclgi:
            codundclgi = os.getenv("REPLICADO_CODUNDCLG")

        query = f"""
            SELECT C.codcur, NC.nomcur, A.codare, N.nomare
            FROM CURSO AS C
            INNER JOIN NOMECURSO AS NC ON C.codcur = NC.codcur
            INNER JOIN AREA AS A ON C.codcur = A.codcur
            INNER JOIN NOMEAREA AS N ON A.codare = N.codare
            WHERE (C.codclg IN ({codundclgi}))
            AND (C.tipcur = 'POS')
            AND (N.dtafimare IS NULL)
            AND (C.dtainiccp IS NOT NULL)
            AND (NC.dtafimcur IS NULL)
        """

        params = {}
        if codcur:
            params["codcur"] = codcur
            query += " AND (C.codcur = :codcur)"

        if codare:
            params["codare"] = codare
            query += " AND (A.codare = :codare)"

        query += " ORDER BY NC.nomcur ASC "

        return DB.fetch_all(query, params)

    @staticmethod
    def orientadores(codare: int) -> list[dict[str, Any]]:
        """
        Retorna lista dos orientadores credenciados na área de concentração.
        """
        query = """
            SELECT r.codpes, MAX(r.dtavalini) AS dtavalini, MAX(p.sexpes) AS sexpes,
            MAX(r.dtavalfim) AS dtavalfim, MIN(r.nivare) AS nivare, MIN(p.nompes) AS nompes
            FROM R25CRECREDOC as r, PESSOA as p
            WHERE r.codpes = p.codpes
            AND r.codare = :codare
            AND r.dtavalfim > GETDATE()
            GROUP BY r.codpes
            ORDER BY nompes ASC
        """
        return DB.fetch_all(query, {"codare": codare})

    @staticmethod
    def catalogo_disciplinas(codare: int) -> list[dict[str, Any]]:
        """
        Retorna catálogo das disciplinas pertencentes à área de concentração.
        """
        query = """
            SELECT DISTINCT r.sgldis, d.nomdis, r.numseqdis, r.dtaatvdis
            FROM R27DISMINCRE AS r, DISCIPLINA AS d
            WHERE d.sgldis = r.sgldis
            AND d.numseqdis = r.numseqdis
            AND r.codare = :codare
            AND (r.dtadtvdis IS NULL OR r.dtadtvdis > getdate())
            AND d.dtaatvdis IS NOT NULL
            AND dateadd(yy,5,d.dtaatvdis)>=getdate()
            ORDER BY d.nomdis ASC
        """
        return DB.fetch_all(query, {"codare": codare})

    @staticmethod
    def disciplina(sgldis: str) -> dict[str, Any] | None:
        """
        Retorna dados da disciplina pela sigla.
        """
        query = """
            SELECT TOP 1 * FROM DISCIPLINA
            WHERE sgldis = :sgldis
            ORDER BY numseqdis DESC
        """
        return DB.fetch(query, {"sgldis": sgldis})

    @staticmethod
    def disciplinas_oferecimento(codare: int) -> list[dict[str, Any]]:
        """
        Retorna a lista de disciplinas em oferecimento de uma determinada área.
        """
        query = """
            SELECT d.nomdis, d.numcretotdis, o.*
            FROM OFERECIMENTO o
            INNER JOIN (
                SELECT MAX(numofe) numofe, sgldis, numseqdis FROM OFERECIMENTO
                WHERE sgldis in (
                    SELECT DISTINCT(sgldis) FROM R27DISMINCRE
                    WHERE codare = :codare AND dtadtvdis is NULL AND dtaatvdis is NOT NULL
                )
                GROUP BY sgldis, numseqdis
            ) tb on tb.numofe = o.numofe AND tb.sgldis = o.sgldis and tb.numseqdis = o.numseqdis
            INNER JOIN DISCIPLINA d on d.sgldis = o.sgldis AND d.numseqdis = o.numseqdis
            WHERE o.stacslofe IS NULL
                AND o.stacslatm IS NULL
                AND o.dtacantur IS NULL
                AND o.dtafimofe > GETDATE()
            ORDER BY o.sgldis
        """
        return DB.fetch_all(query, {"codare": codare})

    @staticmethod
    def oferecimento(sgldis: str, numofe: int) -> dict[str, Any] | None:
        """
        Retorna dados de um oferecimento de disciplina.
        """
        query = """
           SELECT o.*, d.nomdis, d.numcretotdis
           FROM OFERECIMENTO as o, DISCIPLINA as d
           WHERE o.sgldis = d.sgldis
           AND o.numseqdis = d.numseqdis
           AND o.sgldis = :sgldis
           AND o.numofe = :numofe
           AND o.numseqdis = (SELECT MAX(numseqdis) FROM OFERECIMENTO WHERE sgldis = :sgldis AND numofe = :numofe)
        """
        result = DB.fetch(query, {"sgldis": sgldis, "numofe": numofe})

        if result:
            # Add extra data
            result["espacoturma"] = Posgraduacao.espacoturma(
                result["sgldis"], result["numseqdis"], result["numofe"]
            )
            result["ministrante"] = Posgraduacao.ministrante(
                result["sgldis"], result["numseqdis"], result["numofe"]
            )
            # Note: Date formatting omitted to keep return types native (datetime) or raw string. Uteis::data_mes logic can be applied by consumer or added here if strict adherence needed.

        return result

    @staticmethod
    def espacoturma(sgldis: str, numseqdis: int, numofe: int) -> list[dict[str, Any]]:
        """
        Retorna local e horário dos oferecimentos da disciplina.
        """
        query = """
            SELECT * FROM ESPACOTURMA
            WHERE sgldis = :sgldis
            AND numseqdis = :numseqdis
            AND numofe = :numofe
        """
        return DB.fetch_all(
            query, {"sgldis": sgldis, "numseqdis": numseqdis, "numofe": numofe}
        )

    @staticmethod
    def ministrante(sgldis: str, numseqdis: int, numofe: int) -> list[dict[str, Any]]:
        """
        Retorna lista de ministrantes da disciplina.
        """
        query = """
            SELECT r.codpes, p.nompes FROM R32TURMINDOC AS r, PESSOA AS p
            WHERE r.codpes = p.codpes
            AND sgldis = :sgldis
            AND numseqdis = :numseqdis
            AND numofe = :numofe
            ORDER BY p.nompes ASC
        """
        return DB.fetch_all(
            query, {"sgldis": sgldis, "numseqdis": numseqdis, "numofe": numofe}
        )

    @staticmethod
    def obter_vinculo_ativo(codpes: int) -> dict[str, Any] | None:
        """
        Retorna dados do vínculo ativo do aluno de Pós Graduação.
        """
        query = """
            SELECT p.nompes as nompesori, r.codpes as codpesori,
                n.nomare,
                nc.nomcur,
                v.*
            FROM VINCULOPESSOAUSP v
            JOIN R39PGMORIDOC r ON (v.codpes = r.codpespgm AND r.dtafimort IS NULL)
            JOIN PESSOA p ON (p.codpes = r.codpes)
            JOIN AREA a ON (a.codare = v.codare)
            JOIN NOMEAREA n ON (v.codare = n.codare and n.dtafimare IS NULL)
            JOIN NOMECURSO nc on (a.codcur = nc.codcur AND nc.dtafimcur IS NULL)
            WHERE v.codpes = :codpes
            AND v.tipvin IN ('ALUNOPOS','INSCRITOPOS')
            AND v.sitatl = 'A'
        """
        return DB.fetch(query, {"codpes": codpes})

    @staticmethod
    def listar_orientandos_ativos(codpes: int) -> list[dict[str, Any]]:
        """
        Retorna lista de orientandos ativos de um docente.
        """
        # 1. Get List of codpes (orientandos)
        query_orientandos = """
            SELECT DISTINCT (codpespgm) as codpes
            FROM R39PGMORIDOC
            WHERE codpes = :codpes
            AND dtafimort IS NULL
        """
        orientandos_ids = DB.fetch_all(query_orientandos, {"codpes": codpes})

        results = []
        for row in orientandos_ids:
            # 2. Fetch full details for each orientando
            vinculo = Posgraduacao.obter_vinculo_ativo(int(row["codpes"]))
            if vinculo:
                results.append(vinculo)

        # Sort by nompes
        results.sort(key=lambda x: x.get("nompes", ""))
        return results

    @staticmethod
    def listar_defesas(intervalo: dict[str, str] | None = None) -> list[dict[str, Any]]:
        """
        Listar defesas em um intervalo de tempo (inicio, fim).
        """
        if not intervalo:
            now_year = datetime.now().year
            intervalo = {"inicio": f"{now_year}-01-01", "fim": f"{now_year}-12-31"}

        codundclg = os.getenv("REPLICADO_CODUNDCLG")

        query = f"""
            SELECT
              P.codpes,
                    nompes = (SELECT DISTINCT nompes FROM PESSOA WHERE codpes=P.codpes),
                    P.dtadfapgm,
                    P.nivpgm,
                    P.codare,
                    NA.nomare,
                    NC.codcur,
                    NC.nomcur,
                    T.tittrb

            FROM
                    AGPROGRAMA P
                    INNER JOIN AREA A ON P.codare = A.codare
                    INNER JOIN NOMEAREA NA ON P.codare = NA.codare
                    INNER JOIN CURSO AS C ON A.codcur = C.codcur
                    INNER JOIN NOMECURSO NC ON A.codcur = NC.codcur
                    INNER JOIN TRABALHOPROG T ON (P.numseqpgm = T.numseqpgm AND P.codpes = T.codpes AND P.codare = T.codare)
            WHERE
              C.codclg IN ({codundclg}) AND
              (
              P.dtadfapgm >= :inicio AND
              P.dtadfapgm <= :fim
            )
        """

        return DB.fetch_all(
            query, {"inicio": intervalo["inicio"], "fim": intervalo["fim"]}
        )

    @staticmethod
    def areas_programas(
        codundclgi: int | None = None, codcur: int | None = None
    ) -> dict[int, list[dict[str, Any]]]:
        """
        Retorna as áreas de concentração ativas dos programas de pós-graduação.
        """
        if not codundclgi:
            codundclgi_env = os.getenv("REPLICADO_CODUNDCLG")
            # Handle multiple units by taking the first one for logic that requires int
            codundclgi = int(codundclgi_env.split(",")[0]) if codundclgi_env else 0

        programas = Posgraduacao.programas(codundclgi, codcur)
        programas_areas = {}

        for p in programas:
            curr_codcur = p["codcur"]

            # 1. Get areas for the course
            query_areas = "SELECT codare FROM AREA WHERE codcur = :codcur"
            cod_areas = DB.fetch_all(query_areas, {"codcur": curr_codcur})

            areas_list = []
            for a in cod_areas:
                codare = a["codare"]
                query_detail = """
                    SELECT TOP 1 N.codcur, N.codare, N.nomare
                    FROM NOMEAREA as N
                    INNER JOIN CREDAREA as C ON N.codare = C.codare
                    WHERE N.codare = :codare
                    AND C.dtadtvare IS NULL
                """
                area_detail = DB.fetch(query_detail, {"codare": codare})
                if area_detail:
                    areas_list.append(
                        {
                            "codare": area_detail["codare"],
                            "nomare": area_detail["nomare"],
                        }
                    )

            if areas_list:
                programas_areas[curr_codcur] = areas_list

        return programas_areas

    @staticmethod
    def alunos_programa(
        codundclgi: int, codcur: int, codare: int | None = None
    ) -> list[dict[str, Any]]:
        """
        Retorna os alunos de um programa de pós.
        """
        codares = []
        if not codare:
            areas_map = Posgraduacao.areas_programas(codundclgi, codcur)
            if codcur in areas_map:
                codares = [area["codare"] for area in areas_map[codcur]]
        else:
            codares = [codare]

        alunos_programa = []
        for c_are in codares:
            query = """
                SELECT DISTINCT V.codare,V.codpes,L.nompes,V.nivpgm,L.codema, V.dtainivin
                FROM VINCULOPESSOAUSP as V
                INNER JOIN LOCALIZAPESSOA as L ON (V.codpes = L.codpes)
                WHERE V.tipvin = 'ALUNOPOS'
                AND V.sitatl = 'A'
                AND L.codundclg = :codundclgi
                AND V.codare = :codare
                ORDER BY L.nompes ASC
            """
            alunos_area = DB.fetch_all(
                query, {"codundclgi": codundclgi, "codare": c_are}
            )
            alunos_programa.extend(alunos_area)

        return alunos_programa

    @staticmethod
    def idioma_disciplina(codlinofe: str | int) -> str | None:
        """
        Retorna nome completo do idioma da disciplina.
        """
        if codlinofe:
            query = "SELECT dsclin FROM IDIOMA WHERE codlin = :codlinofe"
            result = DB.fetch(query, {"codlinofe": codlinofe})
            return result["dsclin"] if result else None
        return None

    @staticmethod
    def egressos_area(codare: int) -> list[dict[str, Any]]:
        """
        Retorna lista de alunos que defenderam pós-graduação em determinada área.
        """
        query = """
            SELECT p.nompesttd AS nompes, p.codpes AS codpes, a.nivpgm, a.dtadfapgm
            FROM HISTPROGRAMA AS h, PESSOA AS p, AGPROGRAMA AS a, TRABALHOPROG AS t
            WHERE h.tiphstpgm = 'CON'
            AND t.codare = h.codare AND t.codpes = h.codpes AND t.numseqpgm = h.numseqpgm
            AND p.codpes = h.codpes
            AND a.codpes = h.codpes AND a.codare = h.codare AND a.numseqpgm = h.numseqpgm
            AND h.codare = :codare
            ORDER BY h.dtaocopgm DESC, h.codpes ASC
        """
        return DB.fetch_all(query, {"codare": codare})

    @staticmethod
    def contar_egressos_area_agrupado_por_ano(codare: int) -> dict[int, int]:
        """
        Retorna contagem de egressos agrupada por ano.
        """
        query = """
            SELECT year(a.dtadfapgm) AS ano, count(h.codpes) as quantidade
            FROM HISTPROGRAMA AS h
            INNER JOIN PESSOA AS p ON p.codpes = h.codpes
            INNER JOIN AGPROGRAMA AS a ON (a.codpes = h.codpes AND a.codare = h.codare AND a.numseqpgm = h.numseqpgm)
            INNER JOIN TRABALHOPROG AS t ON (t.codare = h.codare AND t.codpes = h.codpes AND t.numseqpgm = h.numseqpgm)
            WHERE h.tiphstpgm = 'CON'
            AND h.codare = :codare
            GROUP BY year(a.dtadfapgm)
            ORDER BY year(a.dtadfapgm)
        """
        result = DB.fetch_all(query, {"codare": codare})
        return {row["ano"]: row["quantidade"] for row in result}

    @staticmethod
    def total_pos_nivel_programa(nivpgm: str, codundclg: int) -> int:
        """
        Total de alunos matriculados por nível (ME, DO, DD).
        """
        query = """
            SELECT COUNT(lp.codpes) as total FROM LOCALIZAPESSOA AS lp
            INNER JOIN VINCULOPESSOAUSP AS vpu ON (lp.codpes = vpu.codpes AND lp.tipvin = vpu.tipvin)
            WHERE lp.tipvin='ALUNOPOS'
            AND lp.codundclg= :codundclg
            AND lp.sitatl='A'
            AND vpu.nivpgm=:nivpgm
        """
        result = DB.fetch(query, {"nivpgm": nivpgm, "codundclg": codundclg})
        return result["total"] if result else 0

    @staticmethod
    def contar_ativos_por_genero(sexpes: str, codare: int | None = None) -> int:
        """
        Retorna quantidade alunos de pós-graduação do gênero.
        """
        unidades = os.getenv("REPLICADO_CODUNDCLG")

        query = f"""
            SELECT COUNT(DISTINCT l.codpes) as total FROM LOCALIZAPESSOA l
            JOIN PESSOA p ON p.codpes = l.codpes
            JOIN HISTPROGRAMA h ON h.codpes = l.codpes
            WHERE l.tipvin = 'ALUNOPOS'
            AND l.codundclg IN ({unidades})
            AND p.sexpes = :sexpes
        """
        params = {"sexpes": sexpes}
        if codare:
            query += " AND (h.codare = :codare)"
            params["codare"] = codare

        result = DB.fetch(query, params)
        return result["total"] if result else 0

    @staticmethod
    def verificar_ex_aluno_pos(codpes: int, codorg: int) -> bool:
        """
        Verifica se é ex-aluno de pós.
        """
        query = """
            SELECT codpes from TITULOPES
            WHERE codpes = :codpes
            AND codcurpgr IS NOT NULL
            AND codorg = :codorg
        """
        result = DB.fetch(query, {"codpes": codpes, "codorg": codorg})
        return bool(result)

    @staticmethod
    def listar_membros_banca(
        codpes: int, codare: int | None = None, numseqpgm: int | None = None
    ) -> list[dict[str, Any]]:
        """
        Retorna os membros da banca de um discente.
        """
        query = """
            SELECT nompesttd = (SELECT nompesttd FROM PESSOA p WHERE p.codpes = r.codpesdct)
            , r.*
            FROM R48PGMTRBDOC r
            WHERE r.codpes = :codpes
        """
        params = {"codpes": codpes}

        if codare:
            query += " AND r.codare = :codare"
            params["codare"] = codare

        if numseqpgm:
            query += " AND r.numseqpgm = :numseqpgm"
            params["numseqpgm"] = numseqpgm

        return DB.fetch_all(query, params)

    @staticmethod
    def obter_orientandos_ativos(codpes: int) -> list[dict[str, Any]]:
        """
        Alias para listar_orientandos_ativos.
        """
        return Posgraduacao.listar_orientandos_ativos(codpes)

    @staticmethod
    def obter_orientandos_concluidos(codpes: int) -> list[dict[str, Any]]:
        """
        Alias para listar_orientandos_concluidos.
        """
        return Posgraduacao.listar_orientandos_concluidos(codpes)

    @staticmethod
    def listar_orientandos_concluidos(codpes: int) -> list[dict[str, Any]]:
        """
        Retorna lista de orientandos que já concluíram.
        """
        query = """
            SELECT DISTINCT (r.codpespgm) as codpes, (p.nompes), (a.nivpgm), (n.nomare), (a.dtadfapgm)
            FROM R39PGMORIDOC r
            INNER JOIN PESSOA p  ON r.codpespgm = p.codpes
            INNER JOIN NOMEAREA n ON r.codare = n.codare
            INNER JOIN AGPROGRAMA a ON a.codpes = r.codpespgm
            WHERE r.codpes = :codpes
            AND r.dtafimort IS NOT NULL
            AND n.dtafimare IS NOT NULL
            AND a.dtadfapgm IS NOT NULL
            AND a.nivpgm IS NOT NULL
            AND a.starmtpgm IS NULL
            ORDER BY a.nivpgm
        """
        return DB.fetch_all(query, {"codpes": codpes})

    @staticmethod
    def obter_defesas(codpes: int) -> list[dict[str, Any]]:
        """
        Obter todas defesas concluídas de uma pessoa.
        """
        query = """
            SELECT
            t1.dtadfapgm,
            t1.codpes AS discente,
            nome_discente = (SELECT DISTINCT nompes FROM PESSOA WHERE codpes=t1.codpes),
            t2.codpes AS docente,
            nome_docente = (SELECT DISTINCT nompes FROM PESSOA WHERE codpes=t2.codpes),
            t1.dtadfapgm, -- Data da Defesa
            t1.nivpgm,    -- ME/DO
            t4.nomcur,    -- Nome do programa de Pós-Graduação
            t5.tittrb     -- Título da Dissertação / Tese

            FROM AGPROGRAMA t1
            INNER JOIN R39PGMORIDOC t2 ON (t1.numseqpgm = t2.numseqpgm AND t1.codpes = t2.codpespgm AND t1.codare = t2.codare)
            INNER JOIN AREA t3 ON (t1.codare = t3.codare)
            INNER JOIN NOMECURSO t4 ON (t3.codcur = t4.codcur)
            INNER JOIN TRABALHOPROG t5 ON (t1.numseqpgm = t5.numseqpgm AND t1.codpes = t5.codpes AND t1.codare = t5.codare)

            WHERE t1.codpes = :codpes
            AND t2.dtafimort = t1.dtadfapgm
        """
        return DB.fetch_all(query, {"codpes": codpes})

    @staticmethod
    def listar_alunos_ativos_programa(codare: int) -> list[dict[str, Any]]:
        """
        Retorna nome e número USP dos alunos ativos nos programas de pós-graduação.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")
        query = f"""
            SELECT DISTINCT l.nompes, l.codpes FROM LOCALIZAPESSOA l
            JOIN VINCULOPESSOAUSP v ON (l.codpes = v.codpes)
            WHERE l.tipvin = 'ALUNOPOS'
            AND l.codundclg IN ({codundclg})
            AND v.codare = :codare
            AND l.sitatl = 'A'
            ORDER BY v.nompes ASC
        """
        return DB.fetch_all(query, {"codare": codare})

    @staticmethod
    def listar_programas() -> list[dict[str, Any]]:
        """
        Lista os programas de Pós-graduação da unidade.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")
        query = f"""
            SELECT C.codcur, NC.nomcur
            FROM CURSO C
            INNER JOIN NOMECURSO NC ON C.codcur = NC.codcur
            WHERE (C.codclg IN ({codundclg}))
            AND (C.tipcur = 'POS')
            AND (C.dtainiccp IS NOT NULL)
            AND (NC.dtafimcur IS NULL)
            ORDER BY NC.nomcur ASC
        """
        return DB.fetch_all(query)

    @staticmethod
    def listar_disciplinas() -> list[dict[str, Any]]:
        """
        Método para listar todos os dados das disciplinas de pós-graduação
        """
        codclg = os.getenv("REPLICADO_CODUNDCLG")
        if not codclg:
            return []

        query = f"""
            SELECT d.*
            FROM
            (
                SELECT MAX(numseqdis) AS numseqdis, sgldis
                FROM DISCIPLINA
                GROUP BY sgldis
            ) AS tbl JOIN DISCIPLINA AS d ON d.sgldis = tbl.sgldis AND d.numseqdis = tbl.numseqdis
            JOIN AREA ON AREA.codare = d.codare
            JOIN CURSO ON CURSO.codcur = AREA.codcur
            WHERE CURSO.codclg IN ({codclg})
            AND d.dtadtvdis IS NULL
            ORDER BY d.nomdis ASC
        """
        return DB.fetch_all(query)
