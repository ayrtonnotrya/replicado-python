import logging
from typing import Any

from replicado.connection import DB

logger = logging.getLogger(__name__)


class AEX:
    """
    Classe para métodos relacionados às Atividades Curriculares de Extensão (AEX).
    """

    @staticmethod
    def listar_atividades(codundclg: int | str = None) -> list[dict[str, Any]]:
        """
        Lista atividades de extensão curricular (CATÁLOGO).

        Args:
            codundclg (int/str, optional): Código da unidade/colegiado. Se não informado, usa ENV.
        """
        if not codundclg:
            import os

            codundclg = os.getenv("REPLICADO_CODUNDCLG")

        query = f"""
            SELECT DISTINCT
                A.codaex, A.veraex, A.titaex, A.sglaex, 
                A.cgahoraluaex, A.sitaex
            FROM AEXATIVIDADECURRICULAR A
            INNER JOIN AEXOFERECIMENTO O ON A.codaex = O.codaex AND A.veraex = O.veraex
            WHERE A.sitaex = 'APR' -- Aprovada
            AND A.codclgaex IN ({codundclg})
            ORDER BY A.titaex
        """
        return DB.fetch_all(query)

    @staticmethod
    def buscar_por_codigo(codaex: int) -> dict[str, Any] | None:
        """
        Retorna detalhes de uma atividade extensionista específica (última versão aprovada).
        """
        query = """
            SELECT TOP 1 *
            FROM AEXATIVIDADECURRICULAR
            WHERE codaex = :codaex
            ORDER BY veraex DESC
        """
        return DB.fetch(query, {"codaex": codaex})

    @staticmethod
    def listar_inscritos(codaex: int, veraex: int = None) -> list[dict[str, Any]]:
        """
        Lista alunos inscritos em uma atividade extensionista.
        """
        params = {"codaex": codaex}
        ver_filter = ""

        if veraex:
            ver_filter = "AND I.veraex = :veraex"
            params["veraex"] = veraex

        query = f"""
            SELECT 
                I.codpes, P.nompes, I.staactalu, I.rstptpaluaex,
                I.dtainsaex
            FROM AEXINSCRICAO I
            INNER JOIN PESSOA P ON I.codpes = P.codpes
            WHERE I.codaex = :codaex
            {ver_filter}
            ORDER BY P.nompes
        """
        return DB.fetch_all(query, params)
