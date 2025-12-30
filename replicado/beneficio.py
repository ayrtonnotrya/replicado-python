import logging
from typing import Any

from replicado.connection import DB

nlogger = logging.getLogger(__name__)


class Beneficio:
    """
    Classe para métodos relacionados a benefícios (Bolsas, Auxílios, etc).
    """

    @staticmethod
    def listar_beneficios() -> list[dict[str, Any]]:
        """
        Retorna a lista de benefícos concedidos e não encerrados.
        FALLBACK: Usa VINCULOPESSOAUSP para encontrar bolsistas.
        """
        # Buscar pessoas com vinculo de bolsista
        query = """
            SELECT
                vp.tipvin as tipbnfalu,
                vp.tipvin as nombnfloc,
                vp.nompes as nompesttd,
                NULL as dtanas,
                NULL as sexpes,
                vp.codpes,
                vp.dtainivin as dtainiccd,
                vp.dtafimvin as dtafimccd,
                'Ativo' as sitatl
            FROM VINCULOPESSOAUSP vp
            WHERE (
                vp.tipvin LIKE 'BOLSISTA%'
                OR vp.tipvin LIKE 'ESTAGIARIO%'
            )
            AND vp.dtafimvin >= getdate()
            AND vp.sitatl = 'A'
        """
        nlogger.warning("Using fallback query for Benefícios (VINCULOPESSOAUSP)")
        return DB.fetch_all(query)

    @staticmethod
    def listar_monitores_pro_aluno(
        codigos_sala_monitor: str | list[int] | int,
    ) -> list[dict[str, Any]]:
        """
        Retorna a lista de monitores da sala Pró-Aluno.
        FALLBACK: Retorna vazio pois não há como filtrar por sala sem tabelas específicas.
        """
        nlogger.warning(
            "listar_monitores_pro_aluno: Retornando lista vazia (Tabelas ausentes)"
        )
        return []
