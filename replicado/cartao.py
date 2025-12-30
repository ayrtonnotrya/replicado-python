import logging
import os
from typing import Any

from replicado.connection import DB

logger = logging.getLogger(__name__)


class CartaoUSP:
    """
    Classe para métodos relacionados ao Cartão USP (físico/digital) e Controle de Acesso (Catracas).
    Baseado na visão `vucatr`.
    """

    @staticmethod
    def verificar_acesso(codpes: int) -> bool:
        """
        Verifica se a pessoa possui cartão ativo para acesso (tabela CATR_CRACHA).
        Indica se a catraca liberaria a entrada.
        
        Args:
            codpes (int): Número USP.
            
        Returns:
            bool: True se possui crachá ativo, False caso contrário.
        """
        query = "SELECT TOP 1 * FROM CATR_CRACHA WHERE codpescra = :codpes AND sitpescra = 'A'"
        result = DB.fetch(query, {"codpes": str(codpes)})
        return bool(result)

    @staticmethod
    def buscar_cracha_ativo(codpes: int) -> dict[str, Any] | None:
        """
        Retorna os dados do crachá ativo da pessoa, incluindo código do crachá e número do chip.
        
        Args:
            codpes (int): Número USP.
            
        Returns:
            dict | None: Dados do crachá ou None se não tiver.
        """
        query = "SELECT * FROM CATR_CRACHA WHERE codpescra = :codpes AND sitpescra = 'A'"
        return DB.fetch(query, {"codpes": str(codpes)})

    @staticmethod
    def listar_solicitacoes(codpes: int) -> list[dict[str, Any]]:
        """
        Lista o histórico de solicitações de cartão USP da pessoa (tabela CARTAOUSPSOLICITACAO).
        Inclui status de pagamento e emissão.
        
        Args:
            codpes (int): Número USP.
            
        Returns:
            list[dict]: Lista de solicitações ordenadas por data (mais recente primeiro).
        """
        query = """
            SELECT * FROM CARTAOUSPSOLICITACAO 
            WHERE codpes = :codpes 
            ORDER BY dtacad DESC
        """
        return DB.fetch_all(query, {"codpes": codpes})
