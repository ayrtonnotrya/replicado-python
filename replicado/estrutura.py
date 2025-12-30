import logging
import os
from typing import Any

from replicado.connection import DB

nlogger = logging.getLogger(__name__)


class Estrutura:
    """
    Classe para métodos relacionados à Estrutura (Setores, Unidades, Locais).
    """

    @staticmethod
    def dump(codset: int) -> dict[str, Any] | None:
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
    def listar_setores(codund: int | None = None) -> list[dict[str, Any]]:
        """
        Retorna todos os setores ativos de uma unidade.
        """
        if not codund:
            codund = os.getenv("REPLICADO_CODUNDCLG")

        query = """
            SELECT codset, tipset, nomabvset, nomset, codsetspe FROM SETOR
            WHERE codund = convert(int, :codund)
            AND dtadtvset IS NULL
            AND nomset NOT LIKE 'Inativo'
            ORDER BY codset ASC
        """
        return DB.fetch_all(query, {"codund": codund})

    @staticmethod
    def get_chefia_setor(codset: int, substitutos: bool = True) -> list[dict[str, Any]]:
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
    def listar_unidades() -> list[dict[str, Any]]:
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
    def obter_unidade(codund: int) -> dict[str, Any] | None:
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
    def obter_local(codlocusp: int) -> dict[str, Any] | None:
        """
        Obtém todas as informações de um único local da tabela LOCALUSP.
        """
        query = "SELECT * FROM LOCALUSP WHERE codlocusp = CONVERT(int, :codlocusp)"
        return DB.fetch(query, {"codlocusp": codlocusp})

    @staticmethod
    def listar_locais_unidade(codund: int | None = None) -> list[dict[str, Any]]:
        """
        Lista todos os registros de local de uma unidade específica.
        """
        if not codund:
            codund = os.getenv("REPLICADO_CODUNDCLG")

        query = "SELECT * FROM LOCALUSP WHERE codund = CONVERT(int, :codund)"
        return DB.fetch_all(query, {"codund": codund})

    @staticmethod
    def procurar_local(part_codlocusp: str, codund: int = 0) -> list[dict[str, Any]]:
        """
        Procura locais da Unidade por código parcial.
        """
        if codund == 0:
            env_cod = os.getenv("REPLICADO_CODUNDCLG", "0")
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

    @staticmethod
    def listar_colegiados(codund: int) -> list[dict[str, Any]]:
        """
        Retorna lista de órgãos colegiados ativos da unidade (Congregação, Conselhos, etc).
        """
        query = """
            SELECT c.codclg, c.sglclg, c.nomclg, c.tipclg
            FROM COLEGIADO c
            INNER JOIN UNIDCOLEG u ON c.codclg = u.codclg AND c.sglclg = u.sglclg
            WHERE u.codund = CONVERT(int, :codund)
            AND u.dtafimvinund IS NULL
            ORDER BY c.nomclg
        """
        return DB.fetch_all(query, {"codund": codund})

    @staticmethod
    def obter_dados_fiscais(codund: int) -> dict[str, Any] | None:
        """
        Retorna CNPJ e razão social fiscal da unidade.
        """
        query = """
            SELECT TOP 1 idfcgc, nomundfis
            FROM UNIDADEFISCAL
            WHERE codund = CONVERT(int, :codund)
            AND dtadtv IS NULL
            ORDER BY dtainival DESC
        """
        return DB.fetch(query, {"codund": codund})

    @staticmethod
    def listar_chefias_unidade(codund: int) -> list[dict[str, Any]]:
        """
        Retorna Diretor e Vice-Diretor da Unidade.
        Baseado na tabela LOCALIZAPESSOA, buscando por funções de confiança.
        """
        query = """
            SELECT l.codpes, l.nompes, l.nomfnc, l.dtainivin, l.codundclg, l.codema
            FROM LOCALIZAPESSOA l
            WHERE l.codundclg = CONVERT(int, :codund)
            AND (l.nomfnc LIKE 'Diretor%' OR l.nomfnc LIKE 'Vice-Diretor%')
            AND l.sitatl = 'A'
            AND l.tipvin = 'SERVIDOR'
            ORDER BY l.nomfnc
        """
        return DB.fetch_all(query, {"codund": codund})

    @staticmethod
    def listar_departamentos(codund: int) -> list[dict[str, Any]]:
        """
        Retorna especificamente os setores que são Departamentos de Ensino.
        """
        query = """
            SELECT codset, nomabvset, nomset, codema, numtelref
            FROM SETOR
            WHERE codund = CONVERT(int, :codund)
            AND tipset LIKE 'Departamento de Ensino'
            AND dtadtvset IS NULL
            ORDER BY nomset
        """
        return DB.fetch_all(query, {"codund": codund})

    @staticmethod
    def obter_contato_setor(codset: int) -> dict[str, Any] | None:
        """
        Retorna e-mail oficial e telefone de referência do setor.
        """
        query = """
            SELECT codset, nomset, codema, numtelref
            FROM SETOR
            WHERE codset = CONVERT(int, :codset)
        """
        return DB.fetch(query, {"codset": codset})

    @staticmethod
    def listar_servidores_setor(codset: int) -> list[dict[str, Any]]:
        """
        Lista todos os servidores ativos lotados em um setor.
        """
        query = """
            SELECT codpes, nompes, nomfnc, codema, numtelfmt
            FROM LOCALIZAPESSOA
            WHERE codset = CONVERT(int, :codset)
            AND sitatl = 'A'
            AND tipvin = 'SERVIDOR'
            ORDER BY nompes
        """
        return DB.fetch_all(query, {"codset": codset})
