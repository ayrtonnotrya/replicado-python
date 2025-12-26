from typing import List, Dict, Any, Union
from replicado.connection import DB
from replicado.utils import clean_string

class Beneficio:
    """
    Classe para métodos relacionados a benefícios (Bolsas, Auxílios, etc).
    """

    @staticmethod
    def listar_beneficios() -> List[Dict[str, Any]]:
        """
        Retorna a lista de benefícos concedidos e não encerrados.
        """
        query = """
            SELECT B.tipbnfalu, B.nombnfloc, P.nompesttd, P.dtanas, P.sexpes, BC.*
            FROM BENEFICIOALUCONCEDIDO BC
            JOIN BENEFICIOALUNO B ON (BC.codbnfalu = B.codbnfalu)
            JOIN PESSOA P on (BC.codpes = P.codpes)
            WHERE BC.dtafimccd >= getdate()
        """
        return DB.fetch_all(query)

    @staticmethod
    def listar_monitores_pro_aluno(codigos_sala_monitor: Union[str, List[int], int]) -> List[Dict[str, Any]]:
        """
        Retorna a lista de monitores da sala Pró-Aluno.
        
        Args:
            codigos_sala_monitor (Union[str, List[int], int]): Pode ser string separada por vírgula, inteiro ou lista de inteiros.
        """
        if isinstance(codigos_sala_monitor, (int, str)) and not isinstance(codigos_sala_monitor, list):
             codigos_str = str(codigos_sala_monitor)
             cods = [c.strip() for c in codigos_str.split(',') if c.strip()]
        else:
             cods = [str(c) for c in codigos_sala_monitor]

        if not cods:
            return []

        # Build safe IN clause
        # parameters: cod0, cod1...
        params = {}
        placeholders = []
        for i, code in enumerate(cods):
             key = f"cod{i}"
             params[key] = code
             placeholders.append(f":{key}")
             
        in_clause = ", ".join(placeholders)
        
        query = f"""
            SELECT DISTINCT
                t1.codpes,
                t2.tipbnfalu,
                t1.codslamon
            FROM
                BENEFICIOALUCONCEDIDO t1
                INNER JOIN BENEFICIOALUNO t2 ON t1.codbnfalu = t2.codbnfalu
                AND t1.dtafimccd > GETDATE ()
                AND t1.dtacanccd IS NULL
                AND t2.codbnfalu = 32
                AND t1.codslamon IN ({in_clause})
        """
        return DB.fetch_all(query, params)
