from typing import List, Dict, Any
import os
from replicado.connection import DB

class Financeiro:
    """
    Classe para métodos relacionados a dados financeiros e centros de despesas.
    """

    @staticmethod
    def listar_centros_despesas() -> List[Dict[str, Any]]:
        """
        Método que retorna os centros de despesas.
        Utiliza o REPLICADO_CODUNDCLG do ambiente.
        """
        unidades = os.getenv('REPLICADO_CODUNDCLG', '')
        if not unidades:
            return []
            
        # Nota: Assim como no PHP, injetamos as unidades diretamente na query 
        # pois é uma lista de IDs. Assumimos que o ambiente é confiável.
        query = f"""
            SELECT etrhie 
            FROM CENTRODESPHIERARQUIA
            WHERE dtadtv IS NULL
            AND codunddsp IN ({unidades})
        """
        return DB.fetch_all(query)
