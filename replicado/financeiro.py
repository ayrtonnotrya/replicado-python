import logging
import os
from typing import Any

from replicado.connection import DB

nlogger = logging.getLogger(__name__)


class Financeiro:
    """
    Classe para métodos relacionados a dados financeiros, patrimônio, almoxarifado e convênios.
    """

    @staticmethod
    def listar_centros_despesas() -> list[dict[str, Any]]:
        """
        Método que retorna os centros de despesa da unidade.
        Utiliza o REPLICADO_CODUNDCLG do ambiente se não houver parâmetro.
        """
        unidades = os.getenv("REPLICADO_CODUNDCLG", "")
        if not unidades:
            return []

        # Nota: Injetamos as unidades diretamente pois é uma lista de IDs.
        query = f"""
            SELECT etrhie, codunddsp, sglcendsp, nomcendsp
            FROM CENTRODESPHIERARQUIA
            WHERE dtadtv IS NULL
            AND codunddsp IN ({unidades})
            ORDER BY ordvrthie
        """
        return DB.fetch_all(query)

    @staticmethod
    def listar_estoque_unidade(codunddsp: int) -> list[dict[str, Any]]:
        """
        Lista o saldo atual de todos os itens no almoxarifado de uma unidade de despesa.

        :param codunddsp: Código da Unidade de Despesa.
        :return: Lista com itens em estoque.
        """
        query = """
            SELECT E.codbem, E.qtdatl, E.prcmed, C.nomgrpitmmat, C.nomsgpitmmat
            FROM ESTOQUE E
            INNER JOIN CLASSIFITEMMAT C ON E.codbem = C.coditmmat
            WHERE E.codunddsp = :codunddsp
            AND E.qtdatl > 0
        """
        params = {"codunddsp": codunddsp}
        return DB.fetch_all(query, params)

    @staticmethod
    def sugerir_reposicao(codunddsp: int) -> list[dict[str, Any]]:
        """
        Identifica itens onde a quantidade atual é menor ou igual à quantidade mínima.

        :param codunddsp: Código da Unidade de Despesa.
        :return: Lista de itens que precisam de reposição.
        """
        query = """
            SELECT E.codbem, E.qtdatl, E.qtdmin, C.nomgrpitmmat
            FROM ESTOQUE E
            INNER JOIN CLASSIFITEMMAT C ON E.codbem = C.coditmmat
            WHERE E.codunddsp = :codunddsp
            AND E.qtdatl <= E.qtdmin
            AND E.staetq = 'S'
        """
        params = {"codunddsp": codunddsp}
        return DB.fetch_all(query, params)

    @staticmethod
    def obter_preco_medio(codbem: int) -> float:
        """
        Retorna o preço médio contábil de um item em estoque (média entre unidades).

        :param codbem: Código do bem.
        :return: Preço médio.
        """
        query = "SELECT AVG(prcmed) as media FROM ESTOQUE WHERE codbem = :codbem"
        params = {"codbem": codbem}
        res = DB.fetch(query, params)
        return float(res["media"]) if res and res["media"] else 0.0

    @staticmethod
    def listar_bens_por_responsavel(codpes: int) -> list[dict[str, Any]]:
        """
        Lista todos os bens patrimoniados sob responsabilidade de um servidor.

        :param codpes: Código da pessoa (nº USP).
        :return: Lista de bens.
        """
        query = """
            SELECT numpat, codbem, stabem, codlocusp, sglcendsp
            FROM BEMPATRIMONIADO
            WHERE codpes = :codpes
            AND stabem = 'Ativo'
        """
        params = {"codpes": codpes}
        return DB.fetch_all(query, params)

    @staticmethod
    def obter_detalhes_bem(numpat: str) -> dict[str, Any] | None:
        """
        Retorna detalhes físicos e de conservação de um bem pelo número de patrimônio.

        :param numpat: Número do patrimônio.
        :return: Dados do bem ou None.
        """
        query = """
            SELECT B.numpat, B.codbem, B.stabem, B.estcsrbem, B.vlroribem, B.dtacad, T.tipbem
            FROM BEMPATRIMONIADO B
            INNER JOIN BEM T ON B.codbem = T.codbem
            WHERE B.numpat = :numpat
        """
        params = {"numpat": numpat}
        return DB.fetch(query, params)

    @staticmethod
    def listar_bens_por_local(codlocusp: int) -> list[dict[str, Any]]:
        """
        Lista todos os bens localizados em uma dependência física específica.

        :param codlocusp: Código do Local USP.
        :return: Lista de bens.
        """
        query = """
            SELECT numpat, codbem, stabem, sglcendsp
            FROM BEMPATRIMONIADO
            WHERE codlocusp = :codlocusp
            AND stabem = 'Ativo'
        """
        params = {"codlocusp": codlocusp}
        return DB.fetch_all(query, params)

    @staticmethod
    def listar_doacoes_recebidas(codund: int) -> list[dict[str, Any]]:
        """
        Lista bens incorporados à unidade via doação.

        :param codund: Código da unidade.
        :return: Lista de bens doados.
        """
        query = """
            SELECT B.numpat, D.nompes as doador, B.dtacad, B.vlroribem
            FROM BEMPATRIMONIADO B
            INNER JOIN BEMDOADO D ON B.numpat = D.numpat
            WHERE B.codunddsp = :codund
        """
        params = {"codund": codund}
        return DB.fetch_all(query, params)

    @staticmethod
    def contar_bens_por_status(codund: int) -> dict[str, int]:
        """
        Retorna estatísticas de quantidade de bens por status na unidade.

        :param codund: Código da unidade.
        :return: Dicionário com status e contagem.
        """
        query = """
            SELECT stabem, COUNT(*) as total
            FROM BEMPATRIMONIADO
            WHERE codunddsp = :codund
            GROUP BY stabem
        """
        params = {"codund": codund}
        res = DB.fetch_all(query, params)
        return {row["stabem"]: row["total"] for row in res}

    @staticmethod
    def obter_hierarquia_financeira(codunddsp: int) -> list[dict[str, Any]]:
        """
        Retorna a árvore hierárquica completa dos centros de despesa de uma unidade.

        :param codunddsp: Código da Unidade de Despesa.
        :return: Hierarquia.
        """
        query = """
            SELECT codhiecendsp, etrhie, nomcendsp, sglcendsp
            FROM CENTRODESPHIERARQUIA
            WHERE codunddsp = :codunddsp
            AND dtadtv IS NULL
            ORDER BY etrhie
        """
        params = {"codunddsp": codunddsp}
        return DB.fetch_all(query, params)

    @staticmethod
    def buscar_local_usp(termo: str) -> list[dict[str, Any]]:
        """
        Busca locais físicos por termo contido na identificação do local.

        :param termo: Termo para busca (ex: nome da sala).
        :return: Lista de locais.
        """
        query = """
            SELECT codlocusp, codund, tiplocusp, stiloc, idfloc
            FROM LOCALUSP
            WHERE idfloc LIKE :termo
            OR tiplocusp LIKE :termo
        """
        params = {"termo": f"%{termo}%"}
        return DB.fetch_all(query, params)

    @staticmethod
    def listar_convenios_financeiros(codund: int) -> list[dict[str, Any]]:
        """
        Lista convênios com repasse ou impacto financeiro na unidade.
        Nota: Usa BEMCONVENIO como proxy para vincular convênios à unidade.

        :param codund: Código da unidade.
        :return: Lista de convênios.
        """
        query = """
            SELECT DISTINCT C.codcvn, C.nomcvn, C.vlrtotcvn, C.dtaasicvn
            FROM CONVENIO C
            INNER JOIN BEMCONVENIO B ON C.codcvn = B.codcvn
            WHERE B.codunddsp = :codund
            AND C.dtadtvcvn IS NULL
        """
        params = {"codund": codund}
        return DB.fetch_all(query, params)

    @staticmethod
    def listar_organizacoes_convenio(codcvn: int) -> list[dict[str, Any]]:
        """
        Lista as organizações parceiras e financiadoras de um convênio.

        :param codcvn: Código do convênio.
        :return: Lista de organizações.
        """
        query = """
            SELECT O.nomrazsoc, O.idfcgcorg, C.stafnd, C.codptporg
            FROM CONVORGAN C
            INNER JOIN ORGANIZACAO O ON C.codorg = O.codorg
            WHERE C.codcvn = :codcvn
        """
        params = {"codcvn": codcvn}
        return DB.fetch_all(query, params)

    @staticmethod
    def buscar_organizacao_por_cnpj(cnpj: str) -> dict[str, Any] | None:
        """
        Localiza dados de uma organização externa via CNPJ (idfcgcorg).

        :param cnpj: CNPJ (apenas números).
        :return: Dados da organização ou None.
        """
        # No Sybase idfcgcorg é decimal
        query = """
            SELECT codorg, nomrazsoc, sglorg, tiporg, idfcgcorg
            FROM ORGANIZACAO
            WHERE idfcgcorg = :cnpj
        """
        params = {"cnpj": cnpj}
        return DB.fetch(query, params)

    @staticmethod
    def detalhar_item_material(coditmmat: int) -> dict[str, Any] | None:
        """
        Recupera a descrição e classificação técnica de um item de material.

        :param coditmmat: Código do item de material.
        :return: Detalhes ou None.
        """
        query = """
            SELECT coditmmat, tipitmmat, nomgrpitmmat, nomsgpitmmat
            FROM CLASSIFITEMMAT
            WHERE coditmmat = :coditmmat
        """
        params = {"coditmmat": coditmmat}
        return DB.fetch(query, params)

    @staticmethod
    def listar_atributos_material(codbem: int) -> list[dict[str, Any]]:
        """
        Lista os atributos específicos (características) de um bem.

        :param codbem: Código do bem.
        :return: Lista de características.
        """
        query = """
            SELECT nomcaritmmat, vlrcaritmmat
            FROM DESCRBEM
            WHERE codbem = :codbem
        """
        params = {"codbem": codbem}
        return DB.fetch_all(query, params)
