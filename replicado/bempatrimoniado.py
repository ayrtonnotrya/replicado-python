from typing import List, Dict, Any, Optional
from replicado.connection import DB

class Bempatrimoniado:
    """
    Classe para métodos relacionados a bens patrimoniados.
    """
    
    # Códigos de itens de material que são considerados informática
    BEM_INFORMATICAS = [12513, 51110, 354384, 354341, 162213, 9300, 45624, 57100]

    @staticmethod
    def dump(numpat: str, fields: List[str] = ['*']) -> Optional[Dict[str, Any]]:
        """
        Consulta a tabela BEMPATRIMONIADO.
        
        Args:
           numpat (str): Número de patrimônio.
           fields (list): Colunas para o SELECT.

        Returns:
           Optional[Dict[str, Any]]: Registro do bem.
        """
        numpat = numpat.replace('.', '')
        columns = ",".join(fields)
        
        # PHP usa CONVERT(decimal, :numpat), aqui vamos passar string mesmo ou int?
        # O SQL original usa decimal. Vamos manter a query.
        query = f"SELECT {columns} FROM BEMPATRIMONIADO WHERE numpat = convert(decimal, :numpat)"
        return DB.fetch(query, {"numpat": numpat})

    @staticmethod
    def verifica(numpat: str) -> bool:
        """
        Verifica se o bem está ativo.
        """
        result = Bempatrimoniado.dump(numpat, ['stabem'])
        if result and result['stabem'] == 'Ativo':
            return True
        return False

    @staticmethod
    def ativos(
        filtros: Dict[str, Any] = {}, 
        buscas: Dict[str, Any] = {}, 
        tipos: Dict[str, str] = {}, 
        limite: int = 2000
    ) -> List[Dict[str, Any]]:
        """
        Retorna todos bens patrimoniados ativos (com opção de filtros e buscas).
        """
        filtros['stabem'] = 'Ativo'
        return Bempatrimoniado.bens(filtros, buscas, tipos, limite)

    @staticmethod
    def is_informatica(numpat: str) -> bool:
        """
        Verifica se o bem é de informática.
        """
        result = Bempatrimoniado.dump(numpat)
        # result['coditmmat'] needs to be cast to int for comparison if returned as string from DB
        # clean_string might have cleaned it, but it stays string if Char/Varchar or becomes int?
        # SQLAlchemy usually returns native types. If 'coditmmat' is int in DB, it comes as int.
        # But if clean_string is applied to everything string-like.
        if result and 'coditmmat' in result:
             # Try convert to int just in case
             try:
                 cod = int(result['coditmmat'])
                 if cod in Bempatrimoniado.BEM_INFORMATICAS:
                     return True
             except ValueError:
                 pass
        return False

    @staticmethod
    def bens(
        filtros: Dict[str, Any] = {}, 
        buscas: Dict[str, Any] = {}, 
        tipos: Dict[str, str] = {}, 
        limite: int = 2000
    ) -> List[Dict[str, Any]]:
        """
        Retorna todos bens patrimoniados (com opção de filtros e buscas).
        """
        query = f"SELECT TOP {limite} * FROM BEMPATRIMONIADO "
        str_where, params = DB.cria_filtro_busca(filtros, buscas, tipos)
        
        query += str_where
        
        return DB.fetch_all(query, params)
