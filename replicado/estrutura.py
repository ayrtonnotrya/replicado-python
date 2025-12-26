import logging
logger = logging.getLogger(__name__)
import os
from typing import Optional, List, Dict, Any
from replicado.connection import DB

class Estrutura:
    """
    Classe para métodos relacionados à Estrutura (Setores, Unidades, Locais).
    """

    @staticmethod
    def dump(codset: int) -> Optional[Dict[str, Any]]:
        """
        Retorna todos campos da tabela SETOR.
        
        Args:
            codset (int): Código do setor.

        Returns:
            Optional[Dict[str, Any]]: Dados do setor.
        """
        query = "SELECT s.* FROM SETOR AS s WHERE s.codset = convert(int, :codset)"
        return DB.fetch(query, {"codset": codset})

    @staticmethod
    def listar_setores(codund: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Retorna todos os setores ativos de uma unidade.
        """
        if not codund:
            codund = os.getenv('REPLICADO_CODUNDCLG')

        query = """
            SELECT codset, tipset, nomabvset, nomset, codsetspe FROM SETOR
            WHERE codund = convert(int, :codund) 
            AND dtadtvset IS NULL 
            AND nomset NOT LIKE 'Inativo'
            ORDER BY codset ASC
        """
        return DB.fetch_all(query, {"codund": codund})

    @staticmethod
    def get_chefia_setor(codset: int, substitutos: bool = True) -> List[Dict[str, Any]]:
        """
        Retorna a(s) chefia(s) do setor.
        
        Args:
            codset (int): Código do setor.
            substitutos (bool): Se true inclui todas as designações, se false exclui temporárias.

        Returns:
            List[Dict[str, Any]]: Lista de chefias.
        """
        s = ""
        if not substitutos:
            # designação uma função (D); pró-labore (P); exercício de liderança (L); Exercendo Coordenação (C).
            s = "AND (c.tipdsg LIKE 'D' OR c.tipdsg LIKE 'P' OR c.tipdsg LIKE 'L' OR c.tipdsg LIKE 'C')"
        
        # Note: Original PHP had "AND c.tipdsg LIKE 'D' OR ..." which is dangerous precedence without parenthesis if not careful, 
        # but here it is prepended with "AND c.tipvinext LIKE 'Servidor Designado' ". 
        # PHP: ... AND c.tipvinext LIKE 'Servidor Designado' AND c.tipdsg LIKE 'D' OR ...
        # If precedence is AND > OR (standard SQL), then A AND B AND C OR D means (A&B&C) OR D. 
        # The PHP code was: "AND c.tipvinext LIKE 'Servidor Designado' " . $s
        # where $s = "AND c.tipdsg LIKE 'D' OR ...".
        # So "WHERE ... AND ... AND D OR P OR L OR C".
        # This implies it matches if it is a designated server with type D, OR if it is type P (regardless of designated server check?), etc.
        # This seems buggy in original if unintended. Python port should be faithful or safe?
        # Assuming original intent was grouping the ORs: AND (D OR P ...).
        # But if the original string was literally appended, I will replicate literal append logic or try to interpret.
        # I'll stick to reproducing the literal string logic but with proper f-string or manual construction to match original query behavior.
        # Actually, adding parenthesis around the ORs is safer and likely intended.
        
        query = f"""
            SELECT c.codpes, c.nompes, c.nomfnc, s.codsetspe, s.nomabvset, s.nomset 
            FROM SETOR AS s 
            INNER JOIN LOCALIZAPESSOA AS c ON c.codset = s.codset
            WHERE s.codset = convert(int, :codset) 
            AND s.dtadtvset IS NULL 
            AND c.tipvinext LIKE 'Servidor Designado' 
            {s}
            ORDER BY s.tipset ASC, s.nomset ASC
        """
        return DB.fetch_all(query, {"codset": codset})

    @staticmethod
    def listar_unidades() -> List[Dict[str, Any]]:
        """
        Retorna lista com todas as unidades ativas da universidade.
        """
        query = """
            SELECT C.*, U.* FROM UNIDADE U
            INNER JOIN CAMPUS C ON U.codcam = C.codcam AND C.numpticam = U.numpticam
            WHERE U.dtadtvund IS NULL 
            ORDER BY C.nomofccam, U.nomund
        """
        return DB.fetch_all(query)

    @staticmethod
    def obter_unidade(codund: int) -> Optional[Dict[str, Any]]:
        """
        Retorna todos campos da tabela UNIDADE.
        """
        query = """
            SELECT U.*, E.*, L.*
            FROM UNIDADE U, ENDUSP E, LOCALIDADE L
            WHERE U.codund = CONVERT(int, :codund)
            AND (E.numseqendusp = 1 AND E.codund = U.codund)
            AND L.codloc = E.codloc
        """
        return DB.fetch(query, {"codund": codund})

    @staticmethod
    def obter_local(codlocusp: int) -> Optional[Dict[str, Any]]:
        """
        Obtém todas as informações de um único local da tabela LOCALUSP.
        """
        query = "SELECT * FROM LOCALUSP WHERE codlocusp = CONVERT(int, :codlocusp)"
        return DB.fetch(query, {"codlocusp": codlocusp})

    @staticmethod
    def listar_locais_unidade(codund: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Lista todos os registros de local de uma unidade específica.
        """
        if not codund:
            codund = os.getenv('REPLICADO_CODUNDCLG')
            
        query = "SELECT * FROM LOCALUSP WHERE codund = CONVERT(int, :codund)"
        return DB.fetch_all(query, {"codund": codund})

    @staticmethod
    def procurar_local(part_codlocusp: str, codund: int = 0) -> List[Dict[str, Any]]:
        """
        Procura locais da Unidade por código parcial.
        """
        if codund == 0:
            env_cod = os.getenv('REPLICADO_CODUNDCLG', '0')
            filtro_codund = f"L.codund = {env_cod}"
        elif codund < 0:
            filtro_codund = "1 = 1"
        else:
            filtro_codund = f"L.codund = {codund}"

        query = f"""
            SELECT L.*, E.epflgr, E.numlgr, U.sglund
            FROM LOCALUSP L
            LEFT JOIN (
                SELECT codund, numseqendusp, MAX(epflgr) AS epflgr, MAX(numlgr) AS numlgr
                FROM ENDUSP
                GROUP BY codund, numseqendusp
                ) E  ON L.codund = E.codund AND L.numseqendusp = E.numseqendusp
            LEFT JOIN UNIDADE U ON L.codund = U.codund
            WHERE {filtro_codund}
                AND CONVERT(VARCHAR, L.codlocusp) LIKE :partCodlocusp
        """
        
        return DB.fetch_all(query, {"partCodlocusp": f"{part_codlocusp}%"})
