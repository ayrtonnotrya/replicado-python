import logging
logger = logging.getLogger(__name__)
from typing import List, Dict, Any, Union, Optional
from datetime import date
import os
from replicado.connection import DB

class CEU:
    """
    Classe para métodos relacionados a Cultura e Extensão (CEU).
    """

    @staticmethod
    def listar_cursos(
        ano_inicio: Optional[int] = None, 
        ano_fim: Optional[int] = None, 
        deptos: Optional[Union[List[int], str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Método para retornar os cursos de cultura e extensão de um período.
        """
        ano_inicio = ano_inicio or date.today().year
        ano_fim = ano_fim or ano_inicio

        query = """
            SELECT
                e.codcurceu, e.codedicurceu,
                c.nomcurceu, cast(c.objcur as NVARCHAR(MAX)) as objcur, cast(c.juscur as NVARCHAR(MAX)) as juscur, c.dscpbcinr, c.fmtcurceu,
                s.codset, s.nomset, s.nomabvset,
                ec.numpro, ec.staedi,
                convert(varchar, e.dtainiofeedi, 103) as dtainiofeedi, convert(varchar, e.dtafimofeedi, 103) as dtafimofeedi, e.qtdvagofe,
                count(m.codpes) as matriculados
            FROM EDICAOCURSOOFECEU e
            LEFT JOIN CURSOCEU c ON c.codcurceu = e.codcurceu
            LEFT JOIN SETOR s ON c.codsetdep = s.codset
            LEFT JOIN EDICAOCURSOCEU ec ON (ec.codcurceu = c.codcurceu AND ec.codedicurceu = e.codedicurceu)
            LEFT JOIN MATRICULACURSOCEU m ON (m.codcurceu = c.codcurceu AND m.codedicurceu = e.codedicurceu)
            WHERE
                c.codclg in (__codundclgs__)
                __deptos__
                AND
                    ec.staedi != 'CAN' -- edição do curso cancelada
                AND (
                    (year(e.dtainiofeedi) BETWEEN convert(int,:ano_inicio) AND convert(int,:ano_fim))
                    OR (year(e.dtafimofeedi) BETWEEN convert(int,:ano_inicio) AND convert(int,:ano_fim))
                )
            GROUP BY -- o group by com quase todos os itens do select é para funcionar o 'count(m.codpes) as matriculados'
                e.codcurceu, e.codedicurceu,
                c.nomcurceu, cast(c.objcur as NVARCHAR(MAX)), cast(c.juscur as NVARCHAR(MAX)), c.dscpbcinr, c.fmtcurceu,
                s.codset, s.nomset, s.nomabvset,
                ec.numpro, ec.staedi,
                e.dtainiofeedi, e.dtafimofeedi, e.qtdvagofe
            ORDER BY
                e.dtainiofeedi
        """

        # Replace __codundclgs__
        codundclgs = os.getenv('REPLICADO_CODUNDCLG', '')
        query = query.replace('__codundclgs__', codundclgs)

        # Handle deptos
        query_deptos = ''
        if deptos:
            if isinstance(deptos, list):
                depto_str = ','.join(str(d) for d in deptos)
            else:
                depto_str = str(deptos)
            query_deptos = f"AND C.codsetdep IN ({depto_str})"
        
        query = query.replace('__deptos__', query_deptos)

        params = {'ano_inicio': ano_inicio, 'ano_fim': ano_fim}
        cursos = DB.fetch_all(query, params)
        
        # Enrich with ministrantes
        for curso in cursos:
            q_min = """
                SELECT m.codpes, p.nompes
                FROM OFERECIMENTOATIVIDADECEU o
                INNER JOIN MINISTRANTECEU m ON o.codofeatvceu = m.codofeatvceu
                INNER JOIN PESSOA p ON m.codpes = p.codpes
                WHERE o.codcurceu = convert(int,:codcurceu)
                    AND o.codedicurceu = convert(int,:codedicurceu)
            """
            p_min = {
                'codcurceu': curso['codcurceu'],
                'codedicurceu': curso['codedicurceu']
            }
            ministrantes = DB.fetch_all(q_min, p_min)
            if ministrantes:
                curso['ministrantes'] = ', '.join([m['nompes'] for m in ministrantes])
            else:
                curso['ministrantes'] = ''
                
        return cursos
