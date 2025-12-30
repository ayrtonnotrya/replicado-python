import logging
import os
from typing import Any

from replicado.connection import DB
from replicado.utils import clean_string

nlogger = logging.getLogger(__name__)


class Pessoa:
    """
    Classe para métodos relacionados a dados de pessoas (tabela PESSOA e satélites).
    """

    @staticmethod
    def dump(codpes: int, fields: list[str] = None) -> dict[str, Any] | None:
        """
        Retorna todos os campos da tabela PESSOA para o codpes informado.
        """
        if fields is None:
            fields = ["*"]
        columns = ",".join(fields)
        query = f"SELECT {columns} FROM PESSOA WHERE codpes = :codpes"
        return DB.fetch(query, {"codpes": codpes})

    @staticmethod
    def cracha(codpes: int) -> dict[str, Any] | None:
        """
        Retorna o cartão/crachá ativo da pessoa.
        """
        query = "SELECT * FROM CATR_CRACHA WHERE codpescra = :codpes"
        return DB.fetch(query, {"codpes": codpes})

    @staticmethod
    def listar_crachas(codpes: int) -> list[dict[str, Any]]:
        """
        Retorna todos os cartões USP ativos e dados de vínculo.
        """
        query = """
            SELECT C.*, T.* FROM CATR_CRACHA C
            INNER JOIN TIPOVINCULO T ON C.tipvinaux = T.tipvin
            WHERE codpescra = :codpes
        """
        return DB.fetch_all(query, {"codpes": codpes})

    @staticmethod
    def emails(codpes: int) -> list[str]:
        """
        Retorna lista de emails da pessoa.
        """
        query = "SELECT codema FROM EMAILPESSOA WHERE codpes = :codpes"
        result = DB.fetch_all(query, {"codpes": codpes})

        emails = []
        for row in result:
            email = row["codema"]
            if email and email not in emails:
                emails.append(email)
        return emails

    @staticmethod
    def email(codpes: int) -> str | None:
        """
        Retorna o email de correspondência (stamtr = 'S').
        """
        query = "SELECT codema FROM EMAILPESSOA WHERE codpes = :codpes AND stamtr = 'S'"
        result = DB.fetch(query, {"codpes": codpes})
        if result:
            return result["codema"]
        return None

    @staticmethod
    def telefones(codpes: int) -> list[str]:
        """
        Retorna lista de telefones da pessoa formatados.
        """
        query = "SELECT codddd, numtel FROM TELEFPESSOA WHERE codpes = :codpes"
        result = DB.fetch_all(query, {"codpes": codpes})

        telefones = []
        for row in result:
            ddd = clean_string(row["codddd"])
            num = clean_string(row["numtel"])
            fone = f"({ddd}) {num}"
            if fone not in telefones:
                telefones.append(fone)
        return telefones

    @staticmethod
    def procurar_por_nome(
        nome: str,
        fonetico: bool = True,
        ativos: bool = True,
        tipvin: str | None = None,
        codundclgs: str | None = None,
        tipvinext: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Busca pessoas por nome ou parte do nome.
        """
        params = {}

        if fonetico:
            query_busca = "P.nompesfon LIKE :nome"
            nome_busca = f"%{nome}%"
        else:
            query_busca = "UPPER(P.nompesttd) LIKE UPPER(:nome)"
            nome_busca = f"%{nome}%".replace(" ", "%")

        params["nome"] = nome_busca

        additional_filters = ""

        if tipvin:
            additional_filters += " AND L.tipvin = :tipvin"
            params["tipvin"] = tipvin

        if tipvinext:
            additional_filters += " AND L.tipvinext = :tipvinext"
            params["tipvinext"] = tipvinext

        if codundclgs:
            additional_filters += (
                f" AND L.codundclg IN ({codundclgs}) AND L.sitatl IN ('A', 'P')"
            )

        if ativos:
            sql = f"""
                SELECT P.*, L.* FROM PESSOA P
                INNER JOIN LOCALIZAPESSOA L on L.codpes = P.codpes
                WHERE L.tipdsg IS NULL
                AND {query_busca}
                {additional_filters}
                ORDER BY P.nompesttd ASC
            """
        else:
            sql = f"""
                SELECT DISTINCT P.* FROM PESSOA P
                LEFT JOIN LOCALIZAPESSOA L on L.codpes = P.codpes
                WHERE {query_busca}
                {additional_filters}
                ORDER BY P.nompesttd ASC
            """

        return DB.fetch_all(sql, params)

    @staticmethod
    def obter_nome(codpes: int | list[int]) -> str | dict[int, str] | None:
        """
        Retorna o nome completo (nompesttd).
        """
        is_list = isinstance(codpes, list)

        if is_list:
            if not codpes:
                return {}
            codpes_str = ",".join(map(str, codpes))
            query = f"SELECT codpes, nompesttd FROM PESSOA WHERE codpes IN ({codpes_str}) ORDER BY nompes"
            result = DB.fetch_all(query)
            return {row["codpes"]: row["nompesttd"] for row in result}
        else:
            query = "SELECT nompesttd FROM PESSOA WHERE codpes = :codpes"
            result = DB.fetch(query, {"codpes": codpes})
            if result:
                return result["nompesttd"]
            return None

    @staticmethod
    def obter_endereco(codpes: int) -> dict[str, Any] | None:
        """
        Retorna o endereço completo da pessoa.
        """
        query = """
            SELECT TL.nomtiplgr, EP.epflgr, EP.numlgr, EP.cpllgr, EP.nombro, L.cidloc, L.sglest, EP.codendptl
            FROM ENDPESSOA AS EP
            JOIN LOCALIDADE AS L ON EP.codloc = L.codloc
            JOIN TIPOLOGRADOURO AS TL ON EP.codtiplgr = TL.codtiplgr
            WHERE EP.codpes = :codpes
        """
        return DB.fetch(query, {"codpes": codpes})

    @staticmethod
    def listar_vinculos_ativos(
        codpes: int, designados: bool = True
    ) -> list[dict[str, Any]]:
        """
        Lista vinculos ativos da pessoa (LOCALIZAPESSOA).
        """
        designados_clause = "" if designados else "AND tipdsg IS NULL"

        sql = f"""
            SELECT *
            FROM LOCALIZAPESSOA
            WHERE codpes = :codpes
            {designados_clause}
        """
        return DB.fetch_all(sql, {"codpes": codpes})

    @staticmethod
    def total_vinculo(vinculo: str, codundclg: int) -> int:
        """
        Conta pessoas com determinado vínculo ativo na unidade.
        """
        query = """
            SELECT COUNT(codpes) as total FROM LOCALIZAPESSOA
            WHERE tipvinext = :vinculo
            AND sitatl = 'A'
            AND codundclg = :codundclg
        """
        result = DB.fetch(query, {"vinculo": vinculo, "codundclg": codundclg})
        return result["total"] if result else 0

    @staticmethod
    def listar_servidores(filtros: dict[str, Any] = None) -> list[dict[str, Any]]:
        """
        Retorna lista de servidores não docentes ativos na unidade.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")
        if not filtros:
            filtros = {}

        filtros_defaults = {
            "LOCALIZAPESSOA.tipvinext": "Servidor",
            "LOCALIZAPESSOA.sitatl": "A",
        }
        filtros.update(filtros_defaults)

        where_parts = [f"LOCALIZAPESSOA.codundclg IN ({codundclg})"]
        params = {}

        for k, v in filtros.items():
            param_name = k.replace(".", "_")
            where_parts.append(f"{k} = :{param_name}")
            params[param_name] = v

        where_clause = " AND ".join(where_parts)

        query = f"""
            SELECT LOCALIZAPESSOA.*, PESSOA.*
            FROM LOCALIZAPESSOA
            INNER JOIN PESSOA ON (LOCALIZAPESSOA.codpes = PESSOA.codpes)
            WHERE {where_clause}
            ORDER BY LOCALIZAPESSOA.nompes
        """
        return DB.fetch_all(query, params)

    @staticmethod
    def listar_estagiarios(codundclg: int) -> list[dict[str, Any]]:
        """
        Retorna estagiários ativos na unidade.
        """
        query = """
            SELECT LOCALIZAPESSOA.*, PESSOA.*
            FROM LOCALIZAPESSOA
            INNER JOIN PESSOA ON (LOCALIZAPESSOA.codpes = PESSOA.codpes)
            WHERE LOCALIZAPESSOA.tipvin LIKE 'ESTAGIARIORH'
            AND LOCALIZAPESSOA.codundclg = :codundclg
            AND LOCALIZAPESSOA.sitatl = 'A'
            ORDER BY LOCALIZAPESSOA.nompes
        """
        return DB.fetch_all(query, {"codundclg": codundclg})

    @staticmethod
    def listar_designados(categoria: int = 0) -> list[dict[str, Any]]:
        """
        Listar servidores designados ativos.
        """
        codundclg = os.getenv("REPLICADO_CODUNDCLG")

        if categoria == 1:
            tipvinext_filter = "'Servidor'"
        elif categoria == 2:
            tipvinext_filter = "'Docente'"
        else:
            tipvinext_filter = "'Servidor','Docente'"

        sql = f"""
            SELECT L.*, P.* FROM LOCALIZAPESSOA L
            INNER JOIN PESSOA P ON (L.codpes = P.codpes)
            WHERE L.tipvinext = 'Servidor Designado'
                AND L.codundclg IN ({codundclg})
                AND L.sitatl = 'A'
                AND L.codpes IN
                    (SELECT codpes
                    FROM LOCALIZAPESSOA L
                    WHERE L.tipvinext IN ({tipvinext_filter})
                        AND L.codundclg IN ({codundclg})
                        AND L.sitatl = 'A')
            ORDER BY L.nompes
        """
        return DB.fetch_all(sql)

    @staticmethod
    def listar_docentes(
        codset_list: str | None = None, sitatl_list: str = "A"
    ) -> list[dict[str, Any]]:
        """
        Lista docentes (ativos e/ou aposentados) da unidade.

        Args:
            codset_list (str, optional): Códigos de setor separados por vírgula.
            sitatl_list (str): Situação ('A', 'P' ou 'A,P'). Defaults to 'A'.

        Returns:
            List[Dict[str, Any]]: Lista de docentes.
        """
        unidades = os.getenv("REPLICADO_CODUNDCLG")
        where_setores = f"AND L.codset IN ({codset_list})" if codset_list else ""

        # Formata lista de situações para SQL (ex: 'A,P' -> "'A','P'")
        sitatl_parts = [f"'{s.strip()}'" for s in sitatl_list.split(",") if s.strip()]
        sitatl_in = ",".join(sitatl_parts)

        query = f"""
            SELECT * FROM LOCALIZAPESSOA L
            WHERE (L.tipvinext = 'Docente' OR L.tipvinext = 'Docente Aposentado')
                AND L.codundclg IN ({unidades})
                AND L.sitatl IN ({sitatl_in})
                {where_setores}
            ORDER BY L.nompes
        """

        return DB.fetch_all(query)


    @staticmethod
    def obter_situacao_vacinal(codpes: int) -> dict[str, Any] | None:
        """
        Retorna a situação vacinal COVID-19 da pessoa.
        
        Args:
            codpes (int): Número USP.
            
        Returns:
            dict | None: Dicionário com 'sitvcipes' (código) e descrição, ou None se não encontrado.
            Códigos comuns: 
            1 = 1ª Dose
            2 = 2ª Dose
            R = Reforço
            N = Não vacinado
            U = Dose única
            M = Restrição Médica
        """
        query = "SELECT * FROM PESSOAINFOVACINACOVID WHERE codpes = :codpes"
        result = DB.fetch(query, {"codpes": codpes})
        
        if result:
            status_map = {
                '1': '1ª Dose',
                '2': '2ª Dose (Ciclo Completo Inicial)',
                'R': 'Dose de Reforço',
                'U': 'Dose Única',
                'N': 'Não Vacinado',
                'M': 'Restrição Médica',
                'I': 'Invalidado'
            }
            # Adiciona descrição humanizada
            result['descricao'] = status_map.get(result.get('sitvcipes'), 'Desconhecido')
            return result
        return None
