import logging
import os
from typing import Any

from replicado.connection import DB
from replicado.pessoa import Pessoa

nlogger = logging.getLogger(__name__)


class Pesquisa:
    """
    Classe para métodos relacionados a Pesquisa (IC, Pós-Doc, Colaboradores).
    """

    @staticmethod
    def listar_iniciacao_cientifica(
        departamento: list[str] | None = None,
        ano_ini: int | None = None,
        ano_fim: int | None = None,
        somente_ativos: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Método para retornar as iniciações científicas.
        NOTA: Devido a ausência das tabelas originais (ICTPROJETO),
        esta query utiliza VINCULOPESSOAUSP como fallback.
        Alguns campos como título e orientador podem não estar disponíveis.
        """
        unidades = os.getenv("REPLICADO_CODUNDCLG", "")
        # Filter for IC types
        query = """
            SELECT
                vp.codpes as aluno,
                vp.nompes as nome_aluno,
                vp.dtainivin as data_ini,
                vp.dtafimvin as data_fim,
                vp.codund as cod_unidade,
                vp.codset as cod_setor,
                vp.sglclg as sigla_departamento,
                vp.tipvin as tipo_vinculo,
                vp.sitatl as status_projeto,
                NULL as titulo_pesquisa,
                NULL as nome_orientador
            FROM VINCULOPESSOAUSP vp
            WHERE vp.codund in (__unidades__)
            AND vp.tipvin IN ('ALUNOIC', 'ALUNOICD', 'ALUNOICDPOS')
            __data__
            __departamento__
            ORDER BY vp.nompes
        """
        query = query.replace("__unidades__", unidades)

        # Handle department filter (sglclg in VINCULOPESSOAUSP usually holds dept sigla)
        query_depto = ""
        if departamento:
            if isinstance(departamento, list):
                depto_str = "'" + "','".join(departamento) + "'"
            else:
                depto_str = f"'{departamento}'"
            query_depto = f"AND vp.sglclg in ({depto_str})"
        query = query.replace("__departamento__", query_depto)

        # Handle date filter
        query_data = ""
        if ano_ini and ano_fim:
            query_data = f"""
                AND (vp.dtafimvin BETWEEN '{ano_ini}-01-01' AND '{ano_fim}-12-31' OR
                vp.dtainivin BETWEEN '{ano_ini}-01-01' AND '{ano_fim}-12-31')
            """

        if somente_ativos:
            query_data += " AND (vp.dtafimvin > GETDATE() or vp.dtafimvin IS NULL) AND vp.sitatl = 'A'"

        query = query.replace("__data__", query_data)

        # Log warning about missing tables
        nlogger.warning("Using fallback query for Iniciação Científica (VINCULOPESSOAUSP)")

        try:
            results = DB.fetch_all(query)
        except Exception as e:
            nlogger.error(f"Erro ao listar IC: {e}")
            return []

        iniciacao_cientifica = []
        for ic in results:
            # Enrich with course info
            curso = Pessoa.retornar_curso_por_codpes(ic["aluno"])
            ic["codcur"] = curso["codcurgrd"] if curso else None
            ic["nome_curso"] = curso["nomcur"] if curso else None
            ic["cod_projeto"] = f"IC-{ic['aluno']}" # Fake ID
            ic["ano_projeto"] = ic["data_ini"].year if ic["data_ini"] else None
            
            # Scholarship logic inferred from tipvin
            if "ICD" in ic["tipo_vinculo"]:
                 ic["bolsa"] = "true"
                 ic["codctgedi"] = "BOLSA"
            else:
                 ic["bolsa"] = "false"
                 ic["codctgedi"] = ""

            iniciacao_cientifica.append(ic)

        return iniciacao_cientifica

    @staticmethod
    def listar_pesquisadores_colaboradores_ativos() -> list[dict[str, Any]]:
        """
        Método para retornar os colaboradores ativos.
        Fallback usando VINCULOPESSOAUSP.
        """
        unidades = os.getenv("REPLICADO_CODUNDCLG", "")
        
        query = """
            SELECT 
                vp.codpes,
                NULL as codprj,
                vp.nompes AS pesquisador,
                'Pesquisa Colaborativa' as titulo_pesquisa,
                NULL as responsavel,
                vp.sglclg as departamento,
                vp.sglclg as sigla_departamento,
                vp.dtainivin as data_ini,
                vp.dtafimvin as data_fim
            FROM VINCULOPESSOAUSP vp
            WHERE vp.tipvin = 'PESQUISADORCOLAB'
            AND vp.sitatl = 'A'
            AND vp.codund in (__unidades__)
            ORDER BY vp.nompes
        """
        query = query.replace("__unidades__", unidades)
        
        nlogger.warning("Using fallback query for Pesquisadores Colaboradores (VINCULOPESSOAUSP)")
        return DB.fetch_all(query)

    @staticmethod
    def listar_pesquisa_pos_doutorandos() -> list[dict[str, Any]]:
        """
        Método para listar os pós-doutorandos.
        Fallback usando VINCULOPESSOAUSP.
        """
        unidades = os.getenv("REPLICADO_CODUNDCLG", "")
        query = """
            SELECT 
                vp.codpes,
                vp.nompes as nome_aluno,
                vp.codpes as codprj, -- Fake ID
                'Pós-Doutorado' as titprj,
                vp.dtainivin as dtainiprj,
                vp.dtafimvin as dtafimprj,
                vp.sglclg as departamento,
                vp.sglclg as sigla_departamento
            FROM VINCULOPESSOAUSP vp
            WHERE vp.tipvin = 'ALUNOPD'
                AND vp.sitatl = 'A'
                AND vp.codund in (__codundclgs__)
                AND (vp.dtafimvin > GETDATE() or vp.dtafimvin IS NULL)
            ORDER BY vp.nompes
        """
        query = query.replace("__codundclgs__", unidades)
        
        nlogger.warning("Using fallback query for Pós-Doutorandos (VINCULOPESSOAUSP)")
        
        try:
            pesquisas = DB.fetch_all(query)
        except Exception:
            return []

        for p in pesquisas:
            # Cannot fetch real supervisor without tables, leave None or fake
            p["supervisor"] = None 
            # In VINCULOPESSOAUSP, ALUNOPD usually implies some connection, assume valid
            p["bolsa"] = "false" # Default

        return pesquisas

    @staticmethod
    def contar_pd_por_ano(statuses: list[str] | None = None) -> dict[int, int]:
        """
        Retorna a quantidade de projetos PD por ano (baseado em dtainivin).
        """
        unidades = os.getenv("REPLICADO_CODUNDCLG", "")
        # VINCULOPESSOAUSP usually has 'A' (Ativo), 'D' (Desligado), etc.
        # Mapping statuses might be tricky, so we ignore for fallback or assume 'A'
        
        query = """
            SELECT
                YEAR(vp.dtainivin) AS Ano,
                COUNT(vp.codpes) AS qtdProjetosAtivos
            FROM VINCULOPESSOAUSP vp
            WHERE vp.codund IN (__codundclg__)
              AND vp.tipvin = 'ALUNOPD'
              AND vp.dtainivin IS NOT NULL
            GROUP BY YEAR(vp.dtainivin)
            ORDER BY YEAR(vp.dtainivin)
        """
        query = query.replace("__codundclg__", unidades)
        
        try:
            results = DB.fetch_all(query)
            return {r["Ano"]: r["qtdProjetosAtivos"] for r in results if r["Ano"]}
        except Exception:
            return {}

    @staticmethod
    def contar_pd_por_ultimos_12_meses(
        statuses: list[str] | None = None,
    ) -> dict[str, int]:
        """
        Retorna a quantidade de projetos PD por mês nos últimos 12 meses.
        Fallback simplificado.
        """
        unidades = os.getenv("REPLICADO_CODUNDCLG", "")

        query = """
            SELECT
                CAST(YEAR(vp.dtainivin) AS VARCHAR(4)) + '-' + RIGHT('0' + CAST(MONTH(vp.dtainivin) AS VARCHAR(2)), 2) AS AnoMes,
                COUNT(vp.codpes) AS qtdProjetosAtivos
            FROM VINCULOPESSOAUSP vp
            WHERE vp.codund IN (__codundclg__)
              AND vp.tipvin = 'ALUNOPD'
              AND vp.dtainivin >= DATEADD(year, -1, GETDATE())
            GROUP BY YEAR(vp.dtainivin), MONTH(vp.dtainivin)
            ORDER BY AnoMes
        """
        query = query.replace("__codundclg__", unidades)
        
        try:
            results = DB.fetch_all(query)
            return {r["AnoMes"]: r["qtdProjetosAtivos"] for r in results}
        except Exception:
            return {}
