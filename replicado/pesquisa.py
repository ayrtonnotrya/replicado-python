import logging
logger = logging.getLogger(__name__)
from typing import List, Dict, Any, Union, Optional
import os
from replicado.connection import DB
from replicado.pessoa import Pessoa

class Pesquisa:
    """
    Classe para métodos relacionados a Pesquisa (IC, Pós-Doc, Colaboradores).
    """

    @staticmethod
    def listar_iniciacao_cientifica(
        departamento: Optional[List[str]] = None, 
        ano_ini: Optional[int] = None, 
        ano_fim: Optional[int] = None, 
        somente_ativos: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Método para retornar as iniciações científicas.
        """
        unidades = os.getenv('REPLICADO_CODUNDCLG', '')
        query = """
            SELECT 
                ic.codprj as cod_projeto,
                ic.codpesalu as aluno,
                p1.nompes as nome_aluno,
                ic.titprj as titulo_pesquisa,
                ic.codpesrsp as orientador, 
                p2.nompes as nome_orientador,
                ic.dtainiprj as data_ini,
                ic.dtafimprj as data_fim, 
                ic.anoprj as ano_projeto,
                s.nomset as departamento,
                s.nomabvset as sigla_departamento,
                ic.staprj as status_projeto
            FROM ICTPROJETO ic
            INNER JOIN PESSOA p1 ON p1.codpes = ic.codpesalu
            INNER JOIN PESSOA p2 ON p2.codpes = ic.codpesrsp
            INNER JOIN SETOR s ON s.codset = ic.codsetprj 
            WHERE ic.codundprj in (__unidades__) 
            __data__
            __departamento__ 
            ORDER BY p1.nompes
        """
        query = query.replace('__unidades__', unidades)

        # Handle department filter
        query_depto = ""
        if departamento:
            if isinstance(departamento, list):
                depto_str = "'" + "','".join(departamento) + "'"
            else:
                depto_str = f"'{departamento}'"
            query_depto = f"AND s.nomabvset in ({depto_str})"
        query = query.replace('__departamento__', query_depto)

        # Handle date filter
        query_data = ""
        if ano_ini and ano_fim:
            query_data = f""" 
                AND (ic.dtafimprj BETWEEN '{ano_ini}-01-01' AND '{ano_fim}-12-31' OR
                ic.dtainiprj BETWEEN '{ano_ini}-01-01' AND '{ano_fim}-12-31') 
            """
        
        if somente_ativos:
            query_data += " AND (ic.dtafimprj > GETDATE() or ic.dtafimprj IS NULL)"
            
        query = query.replace('__data__', query_data)

        results = DB.fetch_all(query)

        iniciacao_cientifica = []
        for ic in results:
            # Enrich with course info
            curso = Pessoa.retornar_curso_por_codpes(ic['aluno'])
            ic['codcur'] = curso['codcurgrd'] if curso else None
            ic['nome_curso'] = curso['nomcur'] if curso else None

            # Scholarship status
            q_bolsa = """
                SELECT b.codctgedi FROM ICTPROJEDITALBOLSA b
                INNER JOIN ICTPROJETO i ON i.codprj = b.codprj 
                WHERE codundprj in (__unidades__)
                AND codmdl = 1
                AND i.codpesalu = convert(int, :codpes)
                AND i.codprj = convert(int, :codprj)
            """
            q_bolsa = q_bolsa.replace('__unidades__', unidades)
            bolsa_res = DB.fetch(q_bolsa, {'codpes': ic['aluno'], 'codprj': ic['cod_projeto']})
            
            if not bolsa_res:
                ic['bolsa'] = 'false'
                ic['codctgedi'] = ''
            else:
                ic['bolsa'] = 'true'
                ic['codctgedi'] = 'PIBIC' if bolsa_res['codctgedi'] == '1' else 'PIBITI'

            iniciacao_cientifica.append(ic)
            
        return iniciacao_cientifica

    @staticmethod
    def listar_pesquisadores_colaboradores_ativos() -> List[Dict[str, Any]]:
        """
        Método para retornar os colaboradores ativos.
        """
        # Original PHP implies unities filter is TODO, so we port as is
        query = """
            SELECT DISTINCT l.codpes, 
                d.codprj, 
                l.nompes AS pesquisador, 
                p.titprj as titulo_pesquisa, 
                n.nompes as responsavel, 
                s.nomset as departamento,
                s.nomabvset as sigla_departamento,
                p.dtainiprj as data_ini,
                p.dtafimprj as data_fim
            FROM LOCALIZAPESSOA l
            INNER JOIN PDPROJETO p ON l.codpes = p.codpes_pd 
            INNER JOIN PDPROJETOSUPERVISOR d ON d.codprj = p.codprj
            INNER JOIN PESSOA n ON n.codpes = d.codpesspv 
            INNER JOIN VINCULOPESSOAUSP v ON l.codpes = v.codpes
            INNER JOIN SETOR s ON p.codsetprj = s.codset 
            WHERE l.tipvin = 'PESQUISADORCOLAB' AND d.dtainispv IS NOT NULL
            AND p.staatlprj = 'Ativo'
            ORDER BY l.nompes
        """
        return DB.fetch_all(query)

    @staticmethod
    def listar_pesquisa_pos_doutorandos() -> List[Dict[str, Any]]:
        """
        Método para listar os pós-doutorandos e dados do projeto, supervisor e setor.
        """
        unidades = os.getenv('REPLICADO_CODUNDCLG', '')
        query = """
            SELECT DISTINCT l.codpes, 
                l.nompes as nome_aluno, 
                p.codprj, 
                p.titprj, 
                p.dtainiprj, 
                p.dtafimprj,
                s.nomset as departamento,
                s.nomabvset as sigla_departamento
            FROM LOCALIZAPESSOA l 
            INNER JOIN PDPROJETO p ON l.codpes = p.codpes_pd 
            INNER JOIN SETOR s ON s.codset = p.codsetprj 
            WHERE l.tipvin = 'ALUNOPD' 
                AND (p.staatlprj = 'Ativo' or p.staatlprj = 'Aprovado')
                AND l.sitatl = 'A' 
                AND p.codund in (__codundclgs__)
                AND p.codmdl = 2
                AND (p.dtafimprj > GETDATE() or p.dtafimprj IS NULL) 
            ORDER BY l.nompes
        """
        query = query.replace('__codundclgs__', unidades)
        pesquisas = DB.fetch_all(query)

        for p in pesquisas:
            # Supervisor
            q_sup = """
                SELECT TOP 1 p2.nompes 
                FROM PDPROJETOSUPERVISOR p 
                INNER JOIN PESSOA p2 ON p.codpesspv = p2.codpes
                WHERE p.codprj = convert(int, :codprj)
                ORDER BY p.anoprj DESC
            """
            supervisor = DB.fetch(q_sup, {'codprj': p['codprj']})
            p['supervisor'] = supervisor['nompes'] if supervisor else None

            # Scholarship
            q_bolsa = """
                SELECT DISTINCT v.codpes, v.nompes FROM ICTPROJEDITALBOLSA i
                INNER JOIN PDPROJETO p ON i.codprj = p.codprj  
                INNER JOIN VINCULOPESSOAUSP v ON p.codpes_pd = v.codpes 
                WHERE v.tipvin = 'ALUNOPD'
                AND (p.staatlprj = 'Ativo' OR p.staatlprj = 'Inscrito')
                AND p.codund in (__codundclgs__) 
                AND (p.dtafimprj > GETDATE() or p.dtafimprj IS NULL)
                AND v.codpes = convert(int, :codpes)
            """
            q_bolsa = q_bolsa.replace('__codundclgs__', unidades)
            bolsa_res = DB.fetch(q_bolsa, {'codpes': p['codpes']})
            p['bolsa'] = 'true' if bolsa_res else 'false'
            
        return pesquisas

    @staticmethod
    def contar_pd_por_ano(statuses: Optional[List[str]] = None) -> Dict[int, int]:
        """
        Retorna a quantidade de projetos PD por ano.
        """
        unidades = os.getenv('REPLICADO_CODUNDCLG', '')
        if not statuses:
            q_stats = "SELECT DISTINCT staatlprj FROM PDPROJETO"
            stats_res = DB.fetch_all(q_stats)
            statuses = [row['staatlprj'] for row in stats_res if row['staatlprj']] or ['Ativo', 'Aprovado']

        query = """
            SELECT 
                anos.Ano,
                COUNT(p.codprj) AS qtdProjetosAtivos
            FROM (
                SELECT DISTINCT YEAR(dtainiprj) AS Ano
                FROM PDPROJETO
                WHERE codund IN (__codundclg__)
                  AND staatlprj IN (__statuses__)
                  AND dtainiprj IS NOT NULL
                  AND YEAR(dtainiprj) <= YEAR(GETDATE())
                UNION
                SELECT DISTINCT YEAR(dtafimprj) AS Ano
                FROM PDPROJETO
                WHERE codund IN (__codundclg__)
                  AND staatlprj IN (__statuses__)
                  AND dtafimprj IS NOT NULL
                  AND YEAR(dtafimprj) <= YEAR(GETDATE())
            ) anos
            LEFT JOIN PDPROJETO p
                ON p.codund IN (__codundclg__)
               AND p.staatlprj IN (__statuses__)
               AND p.dtainiprj IS NOT NULL
               AND YEAR(p.dtainiprj) <= anos.Ano
               AND (
                    p.dtafimprj IS NULL 
                    OR YEAR(p.dtafimprj) >= anos.Ano
               )
            GROUP BY anos.Ano
            ORDER BY anos.Ano
        """
        status_str = "'" + "','".join(statuses) + "'"
        query = query.replace('__codundclg__', unidades).replace('__statuses__', status_str)
        
        results = DB.fetch_all(query)
        return {r['Ano']: r['qtdProjetosAtivos'] for r in results}

    @staticmethod
    def contar_pd_por_ultimos_12_meses(statuses: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Retorna a quantidade de projetos PD por mês nos últimos 12 meses.
        """
        unidades = os.getenv('REPLICADO_CODUNDCLG', '')
        if not statuses:
            q_stats = "SELECT DISTINCT staatlprj FROM PDPROJETO"
            stats_res = DB.fetch_all(q_stats)
            statuses = [row['staatlprj'] for row in stats_res if row['staatlprj']] or ['Ativo', 'Aprovado']

        query = """
            SELECT
                CAST(YEAR(dt) AS VARCHAR(4))
                    + '-' +
                  RIGHT('0' + CAST(MONTH(dt) AS VARCHAR(2)), 2) AS AnoMes,
                COUNT(p.codprj) AS qtdProjetosAtivos
            FROM (
                SELECT DATEADD(
                           month, 
                           -Nums.Num,
                           DATEADD(day, 1 - DAY(GETDATE()), GETDATE())
                       ) AS dt
                FROM (
                    SELECT 0 AS Num
                    UNION ALL SELECT 1
                    UNION ALL SELECT 2
                    UNION ALL SELECT 3
                    UNION ALL SELECT 4
                    UNION ALL SELECT 5
                    UNION ALL SELECT 6
                    UNION ALL SELECT 7
                    UNION ALL SELECT 8
                    UNION ALL SELECT 9
                    UNION ALL SELECT 10
                    UNION ALL SELECT 11
                ) Nums
            ) Meses
            LEFT JOIN PDPROJETO p
                ON p.codund IN (__codundclg__)
               AND p.staatlprj IN (__statuses__)
               AND p.dtainiprj <= DATEADD(day, -1, DATEADD(month, 1, dt))
               AND (p.dtafimprj IS NULL OR p.dtafimprj >= dt)
            GROUP BY
                CAST(YEAR(dt) AS VARCHAR(4))
                    + '-' +
                  RIGHT('0' + CAST(MONTH(dt) AS VARCHAR(2)), 2)
        """
        # Simplified query slightly for return value
        status_str = "'" + "','".join(statuses) + "'"
        query = query.replace('__codundclg__', unidades).replace('__statuses__', status_str)
        
        results = DB.fetch_all(query)
        return {r['AnoMes']: r['qtdProjetosAtivos'] for r in results}
