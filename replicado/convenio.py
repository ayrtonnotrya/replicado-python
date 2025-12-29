import logging
import os
from typing import Any

from replicado.connection import DB
from replicado.utils import data_mes

nlogger = logging.getLogger(__name__)


class Convenio:
    """
    Classe para métodos relacionados a Convênios.
    """

    @staticmethod
    def listar_convenios_academicos_internacionais(
        ativos: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Método para listar convênios acadêmicos internacionais.
        """
        if ativos:
            query = """
                SELECT
                    c.codcvn,
                    c.nomcvn AS nomeConvenio,
                    c.dtaasicvn AS dataInicio,
                    c.dtadtvcvn AS dataFim
                FROM CONVENIO c
                JOIN CONVUNIDDESP u ON u.codcvn = c.codcvn
                WHERE
                    c.tipcvn = 13 --convenio
                    AND c.sticvn = 2 --internacional
                    AND c.stacvn = 'Aprovado'
                    AND u.codunddsp in (__codundclg__)
                    AND c.dtaasicvn IS NOT NULL
                    AND c.dtadtvcvn IS NOT NULL
                    AND GETDATE() BETWEEN c.dtaasicvn AND c.dtadtvcvn
                ORDER BY c.dtaasicvn
            """
        else:
            query = """
                SELECT
                    c.codcvn,
                    c.nomcvn AS nomeConvenio,
                    c.dtaasicvn AS dataInicio,
                    c.dtadtvcvn AS dataFim
                FROM CONVENIO c
                JOIN CONVUNIDDESP u ON u.codcvn = c.codcvn
                WHERE
                    c.tipcvn = 13 --convenio
                    AND c.sticvn = 2 --internacional
                    AND c.stacvn = 'Aprovado'
                    AND u.codunddsp IN (__codundclg__)
                    AND c.dtaasicvn IS NOT NULL
                    AND c.dtadtvcvn IS NOT NULL
                    AND c.dtadtvcvn < GETDATE()
                ORDER BY c.dtaasicvn
            """

        codundclg = os.getenv("REPLICADO_CODUNDCLG", "")
        query = query.replace("__codundclg__", codundclg)

        convenios = DB.fetch_all(query)

        for convenio in convenios:
            # Format dates (simplified, usually already datetime or string from DB)
            # data_mes helper uses d/m/Y format
            convenio["dataInicio"] = (
                data_mes(convenio["dataInicio"]) if convenio["dataInicio"] else "—"
            )
            convenio["dataFim"] = (
                data_mes(convenio["dataFim"]) if convenio["dataFim"] else "—"
            )

            # Coordenadores
            resps = Convenio.listar_coordenadores_convenio(convenio["codcvn"])
            convenio["coordenadores"] = "|".join([r["nompesttd"] for r in resps])

            # Organizacoes
            orgs = Convenio.listar_organizacoes_convenio(convenio["codcvn"])
            convenio["organizacoes"] = "|".join([o["nomeOrganizacao"] for o in orgs])

        return convenios

    @staticmethod
    def listar_coordenadores_convenio(codcvn: int) -> list[dict[str, Any]]:
        """
        Método para listar os responsáveis vinculados a um convênio específico.
        """
        query = """
            SELECT
                r.codcvn,
                r.codpes,
                p.nompesttd
            FROM RESPCONVSERV r
            JOIN PESSOA p ON p.codpes = r.codpes
            WHERE
                r.codcvn = CONVERT(int, :codcvn)
                AND r.codtiprsp = 1
        """
        return DB.fetch_all(query, {"codcvn": codcvn})

    @staticmethod
    def listar_organizacoes_convenio(codcvn: int) -> list[dict[str, Any]]:
        """
        Método para listar as organizações externas vinculadas a um convênio específico.
        """
        query = """
            SELECT
                co.codcvn,
                co.codorg,
                o.nomrazsoc AS nomeOrganizacao
            FROM CONVORGAN co
            JOIN ORGANIZACAO o ON o.codorg = co.codorg
            WHERE co.codcvn = convert(int, :codcvn)
        """
        return DB.fetch_all(query, {"codcvn": codcvn})
