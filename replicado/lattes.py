import json
import logging
import time
import xml.etree.ElementTree as ET
from typing import Any

from replicado.connection import DB
from replicado.utils import etree_to_dict, get_path, unzip

logger = logging.getLogger(__name__)


class Lattes:
    """
    Classe para métodos relacionados ao currículo Lattes.
    """

    _cache: dict[int, tuple[float, dict]] = {}
    _TTL: int = 3600

    @staticmethod
    def id(codpes: int) -> str | bool:
        """
        Recebe o número USP e retorna o ID Lattes da pessoa.
        """
        query = "SELECT idfpescpq from DIM_PESSOA_XMLUSP WHERE codpes = CONVERT(int, :codpes)"
        result = DB.fetch(query, {"codpes": codpes})
        if result:
            logger.debug(f"ID Lattes encontrado para {codpes}: {result['idfpescpq']}")
            return result["idfpescpq"]
        logger.debug(f"ID Lattes não encontrado para {codpes}")
        return False

    @staticmethod
    def retornar_codpes_por_id_lattes(idfpescpq: str) -> int | bool:
        """
        Recebe o ID Lattes e retorna o número USP da pessoa.
        """
        query = "SELECT codpes from DIM_PESSOA_XMLUSP WHERE idfpescpq = :idfpescpq"
        result = DB.fetch(query, {"idfpescpq": idfpescpq})
        return result["codpes"] if result else False

    @staticmethod
    def obter_zip(codpes: int) -> bytes | bool:
        """
        Recebe o número USP e retorna o binário zip do lattes.
        """
        # PHP has setConfig calls to handle encoding issues with Sybase,
        # but pymssql/sqlalchemy usually handles this better.
        query = "SELECT imgarqxml from DIM_PESSOA_XMLUSP WHERE codpes = CONVERT(int, :codpes)"
        result = DB.fetch(query, {"codpes": codpes})
        if result and result.get("imgarqxml"):
            logger.debug(f"Zip Lattes recuperado para {codpes}")
            return result["imgarqxml"]
        logger.warning(f"Zip Lattes não encontrado para {codpes}")
        return False

    @staticmethod
    def save_zip(codpes: int, to: str = "/tmp") -> bool:
        """
        Recebe o número USP e salva o zip do lattes.
        """
        content = Lattes.obter_zip(codpes)
        if content:
            try:
                with open(f"{to}/{codpes}.zip", "wb") as f:
                    f.write(content)
                return True
            except Exception:
                return False
        return False

    @staticmethod
    def obter_xml(codpes: int) -> str | bool:
        """
        Recebe o número USP e devolve XML do lattes.
        """
        zip_content = Lattes.obter_zip(codpes)
        if zip_content:
            xml_bytes = unzip(zip_content)
            if xml_bytes:
                # Try decoding commonly used encodings
                for encoding in ["utf-8", "iso-8859-1"]:
                    try:
                        content = xml_bytes.decode(encoding)
                        logger.debug(
                            f"XML Lattes decodificado para {codpes} usando {encoding}"
                        )
                        return content
                    except UnicodeDecodeError:
                        continue
                logger.warning(
                    f"Falha ao decodificar XML Lattes para {codpes} com codificações padrão"
                )
                return xml_bytes.decode("utf-8", errors="ignore")
            logger.error(f"Falha ao descompactar XML Lattes para {codpes}")
        return False

    @staticmethod
    def obter_json(codpes: int) -> str | bool:
        """
        Recebe o número USP e devolve json do lattes.
        """
        xml_content = Lattes.obter_xml(codpes)
        if xml_content:
            try:
                root = ET.fromstring(xml_content)
                d = etree_to_dict(root)
                # Remove root tag wrapper to match simplexml usually
                # etree_to_dict returns {tag: {contents}}. We want just {contents}.
                root_tag = root.tag
                if root_tag in d:
                    d = d[root_tag]
                logger.debug(f"JSON Lattes gerado para {codpes}")
                return json.dumps(d)
            except Exception as e:
                logger.error(
                    f"Erro ao converter XML para JSON Lattes para {codpes}: {e}"
                )
                return False
        return False

    @staticmethod
    def obter_array(codpes: int) -> dict[str, Any] | bool:
        """
        Recebe o número USP e devolve array (dict) do lattes.
        Usa cache em memória com TTL de 1 hora.
        """
        agora = time.time()
        if codpes in Lattes._cache:
            expira, dados = Lattes._cache[codpes]
            if agora < expira:
                logger.debug(f"Cache HIT para Lattes de {codpes}")
                return dados
            else:
                logger.debug(f"Cache expirado para Lattes de {codpes}")

        json_content = Lattes.obter_json(codpes)
        if json_content:
            dados = json.loads(json_content)
            Lattes._cache[codpes] = (agora + Lattes._TTL, dados)
            return dados
        return False

    @staticmethod
    def listar_premios(
        codpes: int, lattes_array: dict[str, Any] | None = None
    ) -> list | bool:
        """
        Recebe o número USP e devolve array dos prêmios e títulos com o respectivo ano de prêmiação.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes or "DADOS-GERAIS" not in lattes:
            return False

        premios_data = lattes["DADOS-GERAIS"]
        if "PREMIOS-TITULOS" in premios_data:
            premios = premios_data["PREMIOS-TITULOS"].get("PREMIO-TITULO")
            if not premios:
                return False

            if not isinstance(premios, list):
                premios = [premios]

            nome_premios = []
            for p in premios:
                attrs = p.get("@attributes")
                if not attrs or "NOME-DO-PREMIO-OU-TITULO" not in attrs:
                    return False
                nome_premios.append(
                    f"{attrs['NOME-DO-PREMIO-OU-TITULO']} - Ano: {attrs['ANO-DA-PREMIACAO']}"
                )
            return nome_premios
        return False

    @staticmethod
    def retornar_resumo_cv(
        codpes: int, idioma: str = "pt", lattes_array: dict[str, Any] | None = None
    ) -> str | bool:
        """
        Recebe o número USP e devolve o resumo do currículo do lattes.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        path = "DADOS-GERAIS.RESUMO-CV.@attributes.TEXTO-RESUMO-CV-RH"
        if idioma.lower() == "en":
            path += "-EN"

        # html_entity_decode in PHP. python: html.unescape?
        import html

        text = get_path(lattes, path, "")
        return html.unescape(text)

    @staticmethod
    def retornar_data_ultima_atualizacao(codpes: int) -> str | bool:
        """
        Recebe o número USP e devolve a data da última atualização do currículo do lattes.
        """
        query = """
            SELECT CONVERT(VARCHAR(10), dtaultalt ,103) dtaultalt
            FROM DIM_PESSOA_XMLUSP
            WHERE codpes = CONVERT(int, :codpes)
        """
        result = DB.fetch(query, {"codpes": codpes})
        return result["dtaultalt"] if result else False

    @staticmethod
    def listar_autores(array_autores: list) -> list:
        """
        Auxiliar para formatar autores.
        """
        aux_autores = []
        if array_autores:
            if not isinstance(array_autores, list):
                array_autores = [array_autores]

            for autor in array_autores:
                # Attributes can be directly in key or under @attributes depending on etree conversion?
                # My etree_to_dict puts them in @attributes.
                # PHP code handles both ways: "Arr::get($autor, '@attributes', false) ? ... : ..."
                # We will support our structure mostly.
                attrs = autor.get(
                    "@attributes", autor
                )  # Fallback to self if attributes mixed?

                nome_completo = attrs.get("NOME-COMPLETO-DO-AUTOR")
                nome_citacao = attrs.get("NOME-PARA-CITACAO")
                ordem = attrs.get("ORDEM-DE-AUTORIA")

                aux_autores.append(
                    {
                        "NOME-COMPLETO-DO-AUTOR": nome_completo,
                        "NOME-PARA-CITACAO": nome_citacao,
                        "ORDEM-DE-AUTORIA": ordem,
                    }
                )

            aux_autores.sort(
                key=lambda x: int(x["ORDEM-DE-AUTORIA"])
                if x["ORDEM-DE-AUTORIA"]
                else 999
            )
            return aux_autores
        return False

    @staticmethod
    def verificar_filtro(
        tipo: str, ano: int, limit_ini: int, limit_fim: int, i: int
    ) -> bool:
        """
        Verifica filtro de ano/registros.
        """
        if limit_ini == -1:
            return True

        from datetime import date

        current_year = date.today().year

        try:
            ano = int(ano)
        except (ValueError, TypeError):
            ano = 0

        if tipo == "registros":
            if i > limit_ini:
                return False
        elif tipo == "anual":
            if current_year - ano >= limit_ini:
                return False
        elif tipo == "periodo":
            if ano < limit_ini or ano > (limit_fim or 9999):
                return False
        return True

    @staticmethod
    def listar_artigos(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Listar artigos mais recentes.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes or "PRODUCAO-BIBLIOGRAFICA" not in lattes:
            return False

        artigos = get_path(
            lattes, "PRODUCAO-BIBLIOGRAFICA.ARTIGOS-PUBLICADOS.ARTIGO-PUBLICADO"
        )
        if not artigos:
            return False

        if not isinstance(artigos, list):
            artigos = [artigos]

        # Sort desc by ANO-DO-ARTIGO
        def sort_key(a):
            try:
                # Path: DADOS-BASICOS-DO-ARTIGO.@attributes.ANO-DO-ARTIGO
                return int(
                    get_path(a, "DADOS-BASICOS-DO-ARTIGO.@attributes.ANO-DO-ARTIGO", 0)
                )
            except Exception:
                return 0

        artigos.sort(key=sort_key, reverse=True)

        ultimos_artigos = []
        i = 0

        for art in artigos:
            i += 1
            # In PHP strict separate basicos/detalhamento/autores indices?
            # XML usually has tags DADOS-BASICOS-DO-ARTIGO, DETALHAMENTO-DO-ARTIGO, AUTORES.
            # My dict should have keys.

            dados_basicos = art.get("DADOS-BASICOS-DO-ARTIGO", {})
            detalhamento = art.get("DETALHAMENTO-DO-ARTIGO", {})
            autores = art.get("AUTORES", [])

            aux_autores = Lattes.listar_autores(autores)

            # Helper to get attr safely
            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            aux_artigo = {
                "SEQUENCIA-PRODUCAO": get_attr(art, "SEQUENCIA-PRODUCAO", 0),
                "TITULO-DO-ARTIGO": get_attr(dados_basicos, "TITULO-DO-ARTIGO"),
                "TITULO-DO-PERIODICO-OU-REVISTA": get_attr(
                    detalhamento, "TITULO-DO-PERIODICO-OU-REVISTA"
                ),
                "VOLUME": get_attr(detalhamento, "VOLUME"),
                "PAGINA-INICIAL": get_attr(detalhamento, "PAGINA-INICIAL"),
                "PAGINA-FINAL": get_attr(detalhamento, "PAGINA-FINAL"),
                "ANO": get_attr(dados_basicos, "ANO-DO-ARTIGO"),
                "ISSN": get_attr(detalhamento, "ISSN"),
                "AUTORES": aux_autores,
            }

            if not Lattes.verificar_filtro(
                tipo, aux_artigo["ANO"], limit_ini, limit_fim, i
            ):
                continue

            ultimos_artigos.append(aux_artigo)

        return ultimos_artigos

    @staticmethod
    def listar_linhas_pesquisa(
        codpes: int, lattes_array: dict[str, Any] | None = None
    ) -> list | bool:
        """
        Lista as linhas de pesquisa.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        linhas = []
        atuacoes = get_path(
            lattes, "DADOS-GERAIS.ATUACOES-PROFISSIONAIS.ATUACAO-PROFISSIONAL"
        )

        if atuacoes:
            if not isinstance(atuacoes, list):
                atuacoes = [atuacoes]

            for ap in atuacoes:
                pesquisas = get_path(
                    ap,
                    "ATIVIDADES-DE-PESQUISA-E-DESENVOLVIMENTO.PESQUISA-E-DESENVOLVIMENTO",
                )
                if pesquisas:
                    if not isinstance(pesquisas, list):
                        pesquisas = [pesquisas]

                    for p in pesquisas:
                        lps = p.get("LINHA-DE-PESQUISA")
                        if lps:
                            if not isinstance(lps, list):
                                lps = [lps]

                            for lp in lps:
                                titulo = get_path(
                                    lp, "@attributes.TITULO-DA-LINHA-DE-PESQUISA"
                                )
                                if titulo:
                                    linhas.append(titulo)
        return linhas

    @staticmethod
    def listar_livros_publicados(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Lista livros publicados.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        livros = get_path(
            lattes,
            "PRODUCAO-BIBLIOGRAFICA.LIVROS-E-CAPITULOS.LIVROS-PUBLICADOS-OU-ORGANIZADOS.LIVRO-PUBLICADO-OU-ORGANIZADO",
        )

        if not livros:
            return False

        if not isinstance(livros, list):
            livros = [livros]

        # Sort desc
        def sort_key(a):
            try:
                # Fallback attributes usually in @attributes
                return int(get_path(a, "@attributes.SEQUENCIA-PRODUCAO", 0))
            except Exception:
                return 0

        livros.sort(key=sort_key, reverse=True)

        ultimos_livros = []
        i = 0

        for liv in livros:
            i += 1
            dados_basicos = liv.get("DADOS-BASICOS-DO-LIVRO", {})
            detalhamento = liv.get("DETALHAMENTO-DO-LIVRO", {})
            autores = liv.get("AUTORES", [])

            aux_autores = Lattes.listar_autores(autores)

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            aux_livro = {
                "TITULO-DO-LIVRO": get_attr(dados_basicos, "TITULO-DO-LIVRO"),
                "ANO": get_attr(dados_basicos, "ANO"),
                "NUMERO-DE-PAGINAS": get_attr(detalhamento, "NUMERO-DE-PAGINAS"),
                "NOME-DA-EDITORA": get_attr(detalhamento, "NOME-DA-EDITORA"),
                "CIDADE-DA-EDITORA": get_attr(detalhamento, "CIDADE-DA-EDITORA"),
                "ISBN": get_attr(detalhamento, "ISBN"),
                "AUTORES": aux_autores,
            }

            if not Lattes.verificar_filtro(
                tipo, aux_livro["ANO"], limit_ini, limit_fim, i
            ):
                continue

            ultimos_livros.append(aux_livro)

        return ultimos_livros

    @staticmethod
    def listar_capitulos_livros(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Lista capítulos de livros publicados.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        capitulos = get_path(
            lattes,
            "PRODUCAO-BIBLIOGRAFICA.LIVROS-E-CAPITULOS.CAPITULOS-DE-LIVROS-PUBLICADOS.CAPITULO-DE-LIVRO-PUBLICADO",
        )

        if not capitulos:
            return False

        if not isinstance(capitulos, list):
            capitulos = [capitulos]

        # Sort desc by SEQUENCIA-PRODUCAO
        def sort_key(a):
            try:
                return int(get_path(a, "@attributes.SEQUENCIA-PRODUCAO", 0))
            except Exception:
                return 0

        capitulos.sort(key=sort_key, reverse=True)

        ultimos_capitulos = []
        i = 0

        for cap in capitulos:
            i += 1
            dados_basicos = cap.get("DADOS-BASICOS-DO-CAPITULO", {})
            detalhamento = cap.get("DETALHAMENTO-DO-CAPITULO", {})
            autores = cap.get("AUTORES", [])

            aux_autores = Lattes.listar_autores(autores)

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            aux_capitulo = {
                "TITULO-DO-CAPITULO-DO-LIVRO": get_attr(
                    dados_basicos, "TITULO-DO-CAPITULO-DO-LIVRO"
                ),
                "TITULO-DO-LIVRO": get_attr(detalhamento, "TITULO-DO-LIVRO"),
                "ISBN": get_attr(detalhamento, "ISBN"),
                "NUMERO-DE-VOLUMES": get_attr(detalhamento, "NUMERO-DE-VOLUMES"),
                "PAGINA-INICIAL": get_attr(detalhamento, "PAGINA-INICIAL"),
                "PAGINA-FINAL": get_attr(detalhamento, "PAGINA-FINAL"),
                "ANO": get_attr(dados_basicos, "ANO"),
                "NOME-DA-EDITORA": get_attr(detalhamento, "NOME-DA-EDITORA"),
                "CIDADE-DA-EDITORA": get_attr(detalhamento, "CIDADE-DA-EDITORA"),
                "AUTORES": aux_autores,
            }

            if not Lattes.verificar_filtro(
                tipo, aux_capitulo["ANO"], limit_ini, limit_fim, i
            ):
                continue

            ultimos_capitulos.append(aux_capitulo)

        return ultimos_capitulos

    @staticmethod
    def listar_trabalhos_anais(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Lista trabalhos publicados em eventos/anais.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        aux_trabalhos = get_path(
            lattes, "PRODUCAO-BIBLIOGRAFICA.TRABALHOS-EM-EVENTOS.TRABALHO-EM-EVENTOS"
        )

        if not aux_trabalhos:
            return False

        if not isinstance(aux_trabalhos, list):
            aux_trabalhos = [aux_trabalhos]

        trabalhos_anais = []
        i = 0

        for anais in aux_trabalhos:
            i += 1
            dados_basicos = anais.get("DADOS-BASICOS-DO-TRABALHO", {})
            detalhamento = anais.get("DETALHAMENTO-DO-TRABALHO", {})
            autores = anais.get("AUTORES", [])

            aux_autores = Lattes.listar_autores(autores)

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            aux_anais = {
                "TITULO": get_attr(dados_basicos, "TITULO-DO-TRABALHO"),
                "TIPO": get_attr(dados_basicos, "NATUREZA"),
                "SEQUENCIA-PRODUCAO": get_attr(anais, "SEQUENCIA-PRODUCAO"),
                "ANO": get_attr(dados_basicos, "ANO-DO-TRABALHO"),
                "NOME-DO-EVENTO": get_attr(detalhamento, "NOME-DO-EVENTO"),
                "TITULO-DOS-ANAIS-OU-PROCEEDINGS": get_attr(
                    detalhamento, "TITULO-DOS-ANAIS-OU-PROCEEDINGS"
                ),
                "CIDADE-DO-EVENTO": get_attr(detalhamento, "CIDADE-DO-EVENTO"),
                "CIDADE-DA-EDITORA": get_attr(detalhamento, "CIDADE-DA-EDITORA"),
                "NOME-DA-EDITORA": get_attr(detalhamento, "NOME-DA-EDITORA"),
                "ANO-DE-REALIZACAO": get_attr(detalhamento, "ANO-DE-REALIZACAO"),
                "PAGINA-INICIAL": get_attr(detalhamento, "PAGINA-INICIAL"),
                "PAGINA-FINAL": get_attr(detalhamento, "PAGINA-FINAL"),
                "AUTORES": aux_autores,
            }

            if not Lattes.verificar_filtro(
                tipo, aux_anais["ANO"], limit_ini, limit_fim, i
            ):
                continue
            trabalhos_anais.append(aux_anais)

        # Sort desc
        def sort_key(a):
            try:
                return int(a.get("SEQUENCIA-PRODUCAO", 0))
            except Exception:
                return 0

        trabalhos_anais.sort(key=sort_key, reverse=True)

        return trabalhos_anais

    @staticmethod
    def listar_trabalhos_tecnicos(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Lista trabalhos técnicos.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        trabalhos = get_path(lattes, "PRODUCAO-TECNICA.TRABALHO-TECNICO")

        if not trabalhos:
            return False

        if not isinstance(trabalhos, list):
            trabalhos = [trabalhos]

        trabalhos_tecnicos = []
        i = 0

        for t in trabalhos:
            i += 1
            dados_basicos = t.get("DADOS-BASICOS-DO-TRABALHO-TECNICO", {})
            detalhamento = t.get("DETALHAMENTO-DO-TRABALHO-TECNICO", {})
            autores = t.get("AUTORES", [])

            aux_autores = Lattes.listar_autores(autores)

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            aux_trabalho_tec = {
                "TITULO": get_attr(dados_basicos, "TITULO-DO-TRABALHO-TECNICO"),
                "TIPO": get_attr(dados_basicos, "NATUREZA"),
                "SEQUENCIA-PRODUCAO": get_attr(t, "SEQUENCIA-PRODUCAO"),
                "ANO": get_attr(dados_basicos, "ANO"),
                "INSTITUICAO-FINANCIADORA": get_attr(
                    detalhamento, "INSTITUICAO-FINANCIADORA"
                ),
                "AUTORES": aux_autores,
            }

            if not Lattes.verificar_filtro(
                tipo, aux_trabalho_tec["ANO"], limit_ini, limit_fim, i
            ):
                continue

            trabalhos_tecnicos.append(aux_trabalho_tec)

        # Sort desc
        def sort_key(a):
            try:
                return int(a.get("SEQUENCIA-PRODUCAO", 0))
            except Exception:
                return 0

        trabalhos_tecnicos.sort(key=sort_key, reverse=True)

        return trabalhos_tecnicos

    @staticmethod
    def listar_apresentacao_trabalho(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Lista apresentações de trabalhos técnicos.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        apresentacoes = get_path(
            lattes,
            "PRODUCAO-TECNICA.DEMAIS-TIPOS-DE-PRODUCAO-TECNICA.APRESENTACAO-DE-TRABALHO",
        )

        if not apresentacoes:
            return False

        if not isinstance(apresentacoes, list):
            apresentacoes = [apresentacoes]

        apresentacao_trabalhos = []
        i = 0

        for ap in apresentacoes:
            i += 1
            dados_basicos = ap.get("DADOS-BASICOS-DA-APRESENTACAO-DE-TRABALHO", {})
            autores = ap.get("AUTORES", [])

            aux_autores = Lattes.listar_autores(autores)

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            aux_ap = {
                "TITULO": get_attr(dados_basicos, "TITULO"),
                "TIPO": get_attr(dados_basicos, "NATUREZA"),
                "SEQUENCIA-PRODUCAO": get_attr(ap, "SEQUENCIA-PRODUCAO"),
                "ANO": get_attr(dados_basicos, "ANO"),
                "AUTORES": aux_autores,
            }

            if not Lattes.verificar_filtro(
                tipo, aux_ap["ANO"], limit_ini, limit_fim, i
            ):
                continue

            apresentacao_trabalhos.append(aux_ap)

        # Sort desc
        def sort_key(a):
            try:
                return int(a.get("SEQUENCIA-PRODUCAO", 0))
            except Exception:
                return 0

        apresentacao_trabalhos.sort(key=sort_key, reverse=True)

        return apresentacao_trabalhos

    @staticmethod
    def listar_organizacao_evento(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Lista organização de eventos.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        eventos_raw = get_path(
            lattes,
            "PRODUCAO-TECNICA.DEMAIS-TIPOS-DE-PRODUCAO-TECNICA.ORGANIZACAO-DE-EVENTO",
        )

        if not eventos_raw:
            return False

        if not isinstance(eventos_raw, list):
            eventos_raw = [eventos_raw]

        eventos = []
        i = 0

        for ev in eventos_raw:
            i += 1
            dados_basicos = ev.get("DADOS-BASICOS-DA-ORGANIZACAO-DE-EVENTO", {})
            detalhamento = ev.get("DETALHAMENTO-DA-ORGANIZACAO-DE-EVENTO", {})
            autores = ev.get("AUTORES", [])

            aux_autores = Lattes.listar_autores(autores)

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            aux_evento = {
                "TITULO": get_attr(dados_basicos, "TITULO"),
                "ANO": get_attr(dados_basicos, "ANO"),
                "TIPO": get_attr(dados_basicos, "TIPO"),
                "INSTITUICAO-PROMOTORA": get_attr(
                    detalhamento, "INSTITUICAO-PROMOTORA"
                ),
                "SEQUENCIA-PRODUCAO": get_attr(ev, "SEQUENCIA-PRODUCAO"),
                "AUTORES": aux_autores,
            }

            if not Lattes.verificar_filtro(
                tipo, aux_evento["ANO"], limit_ini, limit_fim, i
            ):
                continue

            eventos.append(aux_evento)

        return eventos

    @staticmethod
    def listar_outras_producoes_tecnicas(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Lista outras produções técnicas.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        outras_raw = get_path(
            lattes,
            "PRODUCAO-TECNICA.DEMAIS-TIPOS-DE-PRODUCAO-TECNICA.OUTRA-PRODUCAO-TECNICA",
        )

        if not outras_raw:
            return False

        if not isinstance(outras_raw, list):
            outras_raw = [outras_raw]

        outras = []
        i = 0

        for outro in outras_raw:
            i += 1
            dados_basicos = outro.get("DADOS-BASICOS-DE-OUTRA-PRODUCAO-TECNICA", {})
            autores = outro.get("AUTORES", [])

            aux_autores = Lattes.listar_autores(autores)

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            aux_outro = {
                "TITULO": get_attr(dados_basicos, "TITULO"),
                "NATUREZA": get_attr(dados_basicos, "NATUREZA"),
                "SEQUENCIA-PRODUCAO": get_attr(outro, "SEQUENCIA-PRODUCAO"),
                "ANO": get_attr(dados_basicos, "ANO"),
                "AUTORES": aux_autores,
            }

            if not Lattes.verificar_filtro(
                tipo, aux_outro["ANO"], limit_ini, limit_fim, i
            ):
                continue

            outras.append(aux_outro)

        return outras

    @staticmethod
    def listar_teses(
        codpes: int, tipo: str = "DOUTORADO", lattes_array: dict[str, Any] | None = None
    ) -> list | bool:
        """
        Lista teses defendidas (MESTRADO ou DOUTORADO).
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        formacao = get_path(lattes, f"DADOS-GERAIS.FORMACAO-ACADEMICA-TITULACAO.{tipo}")
        if not formacao:
            return False

        if not isinstance(formacao, list):
            formacao = [formacao]

        lista_teses = []
        for p in formacao:

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            # Keywords
            palavras = []
            pk_node = p.get("PALAVRAS-CHAVE", {})
            for i in range(1, 7):
                key = f"PALAVRA-CHAVE-{i}"
                val = get_attr(pk_node, key)
                if val:
                    palavras.append(val)
            palavras_str = "; ".join(palavras)

            titulo = get_attr(p, "TITULO-DA-DISSERTACAO-TESE")
            ano = get_attr(p, "ANO-DE-OBTENCAO-DO-TITULO")

            if titulo:
                lista_teses.append(
                    {
                        "TITULO": titulo,
                        "PALAVRAS-CHAVE": palavras_str,
                        "ANO-DE-OBTENCAO-DO-TITULO": ano,
                    }
                )

        return lista_teses if lista_teses else False

    @staticmethod
    def obter_livre_docencia(
        codpes: int, lattes_array: dict[str, Any] | None = None
    ) -> list | bool:
        """
        Retorna dados de Livre-Docência.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes or "DADOS-GERAIS" not in lattes:
            return False

        dados_gerais = lattes["DADOS-GERAIS"]
        if "FORMACAO-ACADEMICA-TITULACAO" not in dados_gerais:
            return False

        livre = get_path(dados_gerais, "FORMACAO-ACADEMICA-TITULACAO.LIVRE-DOCENCIA")
        if not livre:
            return False

        if not isinstance(livre, list):
            livre = [livre]

        result = []
        for p in livre:

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            titulo = get_attr(p, "TITULO-DO-TRABALHO")
            if titulo:
                result.append(titulo)

        return result if result else False

    @staticmethod
    def listar_cursos_curta_duracao(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Lista cursos de curta duração ministrados.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        cursos_raw = get_path(
            lattes,
            "PRODUCAO-TECNICA.DEMAIS-TIPOS-DE-PRODUCAO-TECNICA.CURSO-DE-CURTA-DURACAO-MINISTRADO",
        )

        if not cursos_raw:
            return False

        if not isinstance(cursos_raw, list):
            cursos_raw = [cursos_raw]

        cursos = []
        i = 0

        for c in cursos_raw:
            i += 1
            dados_basicos = c.get(
                "DADOS-BASICOS-DE-CURSOS-CURTA-DURACAO-MINISTRADO", {}
            )
            detalhamento = c.get("DETALHAMENTO-DE-CURSOS-CURTA-DURACAO-MINISTRADO", {})
            autores = c.get("AUTORES", [])

            aux_autores = Lattes.listar_autores(autores)

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            aux_curso = {
                "SEQUENCIA-PRODUCAO": get_attr(c, "SEQUENCIA-PRODUCAO"),
                "TITULO": get_attr(dados_basicos, "TITULO"),
                "ANO": get_attr(dados_basicos, "ANO"),
                "NIVEL-DO-CURSO": get_attr(dados_basicos, "NIVEL-DO-CURSO"),
                "INSTITUICAO-PROMOTORA-DO-CURSO": get_attr(
                    detalhamento, "INSTITUICAO-PROMOTORA-DO-CURSO"
                ),
                "AUTORES": aux_autores,
            }

            if not Lattes.verificar_filtro(
                tipo, aux_curso["ANO"], limit_ini, limit_fim, i
            ):
                continue

            cursos.append(aux_curso)

        return cursos

    @staticmethod
    def listar_relatorio_pesquisa(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Lista relatórios de pesquisa.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        relatorios_raw = get_path(
            lattes,
            "PRODUCAO-TECNICA.DEMAIS-TIPOS-DE-PRODUCAO-TECNICA.RELATORIO-DE-PESQUISA",
        )

        if not relatorios_raw:
            return False

        if not isinstance(relatorios_raw, list):
            relatorios_raw = [relatorios_raw]

        relatorios = []
        i = 0

        for rel in relatorios_raw:
            i += 1
            dados_basicos = rel.get("DADOS-BASICOS-DO-RELATORIO-DE-PESQUISA", {})
            autores = rel.get("AUTORES", [])

            aux_autores = Lattes.listar_autores(autores)

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            aux_relatorio = {
                "SEQUENCIA-PRODUCAO": get_attr(rel, "SEQUENCIA-PRODUCAO"),
                "TITULO": get_attr(dados_basicos, "TITULO"),
                "ANO": get_attr(dados_basicos, "ANO"),
                "AUTORES": aux_autores,
            }

            if not Lattes.verificar_filtro(
                tipo, aux_relatorio["ANO"], limit_ini, limit_fim, i
            ):
                continue

            relatorios.append(aux_relatorio)

        return relatorios

    @staticmethod
    def listar_material_didatico_instrucional(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Lista materiais didáticos ou instrucionais.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        materiais_raw = get_path(
            lattes,
            "PRODUCAO-TECNICA.DEMAIS-TIPOS-DE-PRODUCAO-TECNICA.DESENVOLVIMENTO-DE-MATERIAL-DIDATICO-OU-INSTRUCIONAL",
        )

        if not materiais_raw:
            return False

        if not isinstance(materiais_raw, list):
            materiais_raw = [materiais_raw]

        materiais = []
        i = 0

        for mat in materiais_raw:
            i += 1
            dados_basicos = mat.get(
                "DADOS-BASICOS-DO-MATERIAL-DIDATICO-OU-INSTRUCIONAL", {}
            )
            autores = mat.get("AUTORES", [])

            aux_autores = Lattes.listar_autores(autores)

            def get_attr(node, key, default=""):
                return (
                    node.get("@attributes", {}).get(key, default)
                    if isinstance(node, dict)
                    else default
                )

            aux_material = {
                "SEQUENCIA-PRODUCAO": get_attr(mat, "SEQUENCIA-PRODUCAO"),
                "TITULO": get_attr(dados_basicos, "TITULO"),
                "ANO": get_attr(dados_basicos, "ANO"),
                "NATUREZA": get_attr(dados_basicos, "NATUREZA"),
                "AUTORES": aux_autores,
            }

            if not Lattes.verificar_filtro(
                tipo, aux_material["ANO"], limit_ini, limit_fim, i
            ):
                continue

            materiais.append(aux_material)

        return materiais

    @staticmethod
    def listar_outras_producoes_bibliograficas(
        codpes: int,
        lattes_array: dict[str, Any] | None = None,
        tipo: str = "registros",
        limit_ini: int = 5,
        limit_fim: int | None = None,
    ) -> list | bool:
        """
        Lista outras produções bibliográficas.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        demais = get_path(
            lattes, "PRODUCAO-BIBLIOGRAFICA.DEMAIS-TIPOS-DE-PRODUCAO-BIBLIOGRAFICA"
        )
        if not demais:
            return False

        outras = []
        i = 0

        # 1. OUTRA-PRODUCAO-BIBLIOGRAFICA
        outras_prod_raw = demais.get("OUTRA-PRODUCAO-BIBLIOGRAFICA", [])
        if outras_prod_raw:
            if not isinstance(outras_prod_raw, list):
                outras_prod_raw = [outras_prod_raw]

            for o in outras_prod_raw:
                i += 1
                dados_basicos = o.get("DADOS-BASICOS-DE-OUTRA-PRODUCAO", {})
                detalhamento = o.get("DETALHAMENTO-DE-OUTRA-PRODUCAO", {})
                autores = o.get("AUTORES", [])

                aux_autores = Lattes.listar_autores(autores)

                def get_attr(node, key, default=""):
                    return (
                        node.get("@attributes", {}).get(key, default)
                        if isinstance(node, dict)
                        else default
                    )

                aux_o = {
                    "TITULO": get_attr(dados_basicos, "TITULO"),
                    "TIPO": get_attr(dados_basicos, "NATUREZA"),
                    "SEQUENCIA-PRODUCAO": get_attr(o, "SEQUENCIA-PRODUCAO"),
                    "ANO": get_attr(dados_basicos, "ANO"),
                    "EDITORA": get_attr(detalhamento, "EDITORA"),
                    "CIDADE-DA-EDITORA": get_attr(detalhamento, "CIDADE-DA-EDITORA"),
                    "AUTORES": aux_autores,
                }

                if not Lattes.verificar_filtro(
                    tipo, aux_o["ANO"], limit_ini, limit_fim, i
                ):
                    continue
                outras.append(aux_o)

        # 2. Special types: DA-TRADUCAO, DO-PREFACIO-POSFACIO, DA-PARTITURA
        tipo_outras = [
            {"path": "DA-TRADUCAO", "key": "TRADUCAO", "label": "Tradução"},
            {
                "path": "DO-PREFACIO-POSFACIO",
                "key": "PREFACIO-POSFACIO",
                "label": "Prefácio, Pósfacio",
            },
            {
                "path": "DA-PARTITURA",
                "key": "PARTITURA-MUSICAL",
                "label": "Partitura Musical",
            },
        ]

        for t_spec in tipo_outras:
            prods = demais.get(t_spec["key"], [])
            if prods:
                if not isinstance(prods, list):
                    prods = [prods]

                for p in prods:
                    i += 1
                    basicos_key = f"DADOS-BASICOS-{t_spec['path']}"
                    detalhamento_key = f"DETALHAMENTO-{t_spec['path']}"

                    dados_basicos = p.get(basicos_key, {})
                    detalhamento = p.get(detalhamento_key, {})
                    autores = p.get("AUTORES", [])

                    aux_autores = Lattes.listar_autores(autores)

                    def get_attr(node, key, default=""):
                        return (
                            node.get("@attributes", {}).get(key, default)
                            if isinstance(node, dict)
                            else default
                        )

                    node_tipo = get_attr(dados_basicos, "TIPO")
                    label_tipo = (
                        f"{t_spec['label']}/{node_tipo.capitalize()}"
                        if node_tipo
                        else t_spec["label"]
                    )

                    aux_p = {
                        "TITULO": get_attr(dados_basicos, "TITULO"),
                        "TIPO": label_tipo,
                        "SEQUENCIA-PRODUCAO": get_attr(p, "SEQUENCIA-PRODUCAO"),
                        "ANO": get_attr(dados_basicos, "ANO"),
                        "CIDADE-DA-EDITORA": get_attr(
                            detalhamento, "CIDADE-DA-EDITORA"
                        ),
                        "EDITORA": get_attr(detalhamento, f"EDITORA-{t_spec['path']}"),
                        "AUTORES": aux_autores,
                    }

                    if not Lattes.verificar_filtro(
                        tipo, aux_p["ANO"], limit_ini, limit_fim, i
                    ):
                        continue
                    outras.append(aux_p)

        # Sort desc
        def sort_key(a):
            try:
                return int(a.get("SEQUENCIA-PRODUCAO", 0))
            except Exception:
                return 0

        outras.sort(key=sort_key, reverse=True)

        return outras

    @staticmethod
    def retornar_banca_mestrado(
        codpes: int, lattes_array: dict[str, Any] | None = None
    ) -> list | bool:
        """
        Retorna array com os títulos das teses de mestrado onde o docente participou da banca.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        bancas = get_path(
            lattes,
            "DADOS-COMPLEMENTARES.PARTICIPACAO-EM-BANCA-TRABALHOS-CONCLUSAO.PARTICIPACAO-EM-BANCA-DE-MESTRADO",
        )
        if not bancas:
            return False

        if not isinstance(bancas, list):
            bancas = [bancas]

        nome_bancas = []
        for b in bancas:
            titulo = get_path(
                b,
                "DADOS-BASICOS-DA-PARTICIPACAO-EM-BANCA-DE-MESTRADO.@attributes.TITULO",
            )
            if titulo:
                nome_bancas.append(titulo)
        return nome_bancas if nome_bancas else False

    @staticmethod
    def retornar_banca_doutorado(
        codpes: int, lattes_array: dict[str, Any] | None = None
    ) -> list | bool:
        """
        Retorna array com os títulos das teses de doutorado onde o docente participou da banca.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        bancas = get_path(
            lattes,
            "DADOS-COMPLEMENTARES.PARTICIPACAO-EM-BANCA-TRABALHOS-CONCLUSAO.PARTICIPACAO-EM-BANCA-DE-DOUTORADO",
        )
        if not bancas:
            return False

        if not isinstance(bancas, list):
            bancas = [bancas]

        nome_bancas = []
        for b in bancas:
            titulo = get_path(
                b,
                "DADOS-BASICOS-DA-PARTICIPACAO-EM-BANCA-DE-DOUTORADO.@attributes.TITULO",
            )
            candidato = get_path(
                b,
                "DETALHAMENTO-DA-PARTICIPACAO-EM-BANCA-DE-DOUTORADO.@attributes.NOME-DO-CANDIDATO",
            )

            if titulo:
                res = titulo
                if candidato:
                    res += f"\n{candidato}"
                nome_bancas.append(res)
        return nome_bancas if nome_bancas else False

    @staticmethod
    def listar_artigos_com_qualis(codpes: int) -> list[dict[str, Any]] | bool:
        """
        Lista artigos e tenta enriquecer com o estrato Qualis.
        """
        artigos = Lattes.listar_artigos(codpes)
        if not artigos:
            return False

        # Busca todos os Qualis para agilizar (ou buscar por ISSN específico)
        issns = [f"'{a['ISSN'].replace('-', '')}'" for a in artigos if a.get("ISSN")]
        if not issns:
            return artigos

        issns_str = ",".join(set(issns))
        query = f"""
            SELECT numisnprd, clsqliprd
            FROM QUALISPERIODICO
            WHERE numisnprd IN ({issns_str})
        """
        try:
            qualis_results = DB.fetch_all(query)
            qualis_map = {r["numisnprd"]: r["clsqliprd"] for r in qualis_results}

            for art in artigos:
                issn_limpo = art.get("ISSN", "").replace("-", "")
                art["QUALIS"] = qualis_map.get(issn_limpo, "N/A")
        except Exception as e:
            logger.warning(f"Não foi possível recuperar Qualis: {e}")
            for art in artigos:
                art["QUALIS"] = "FALHA_DB"

        return artigos

    @staticmethod
    def obter_metricas_citacao(
        codpes: int, lattes_array: dict[str, Any] | None = None
    ) -> dict[str, Any] | bool:
        """
        Retorna métricas de citação (índice H, etc) extraídas do XML do Lattes.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False

        # As métricas de citação no XML do Lattes (formato CNPq) geralmente ficam em:
        # CITACOES (podem haver múltiplas entradas para ISI, SCOPUS, SCIELO)
        citacoes_node = lattes.get("CITACOES", [])
        if isinstance(citacoes_node, dict):
            citacoes_node = [citacoes_node]

        metrics = {
             "qtdcitpes": 0, # Total estimado
             "clsinh": 0,    # Maior H-Index encontrado
        }
        
        found = False
        for c in citacoes_node:
             attrs = c.get("@attributes", {})
             if not attrs:
                 continue
             
             # Exemplo de atributos: 'TOTAL-CITACOES', 'TOTAL-DE-TRABALHOS', 'INDICE-H'
             try:
                 total = int(attrs.get("TOTAL-CITACOES", 0))
                 h_index = int(attrs.get("INDICE-H", 0))
                 
                 metrics["qtdcitpes"] = max(metrics["qtdcitpes"], total)
                 metrics["clsinh"] = max(metrics["clsinh"], h_index)
                 found = True
             except Exception:
                 pass
        
        if found:
            return metrics
            
        return False

    @staticmethod
    def listar_citacoes_anual(codpes: int) -> list[dict[str, Any]] | bool:
        """
        Retorna histórico anual de citações.
        NOTA: O XML do Lattes padrão NÃO contém histórico ano a ano estruturado, 
        apenas totais. Retornará lista vazia para evitar erros, já que a tabela
        CITACAOPESSOAANUAL não existe.
        """
        # Como o XML não tem essa informação detalhada por ano (somente totais), 
        # e a tabela auxiliar não existe, retornamos falso/vazio.
        return []

    @staticmethod
    def listar_projetos_pesquisa(
        codpes: int, lattes_array: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """
        Lista projetos de pesquisa extraídos do XML do Lattes.
        Caminho: DADOS-GERAIS -> ATUACOES-PROFISSIONAIS -> ATUACAO-PROFISSIONAL ->
                 ATIVIDADES-DE-PARTICIPACAO-EM-PROJETO -> PROJETO-DE-PESQUISA
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
             return []

        projetos = []
        atuacoes = get_path(
             lattes, "DADOS-GERAIS.ATUACOES-PROFISSIONAIS.ATUACAO-PROFISSIONAL"
        )
        if not atuacoes:
             return []
             
        if not isinstance(atuacoes, list):
             atuacoes = [atuacoes]
             
        for atuacao in atuacoes:
             # Pode haver várias atividades de participação
             participacoes = get_path(atuacao, "ATIVIDADES-DE-PARTICIPACAO-EM-PROJETO.PROJETO-DE-PESQUISA")
             if not participacoes:
                 continue
                 
             if not isinstance(participacoes, list):
                 participacoes = [participacoes]
                 
             for proj in participacoes:
                 # Atributos básicos ficam em @attributes do nó PROJETO-DE-PESQUISA
                 attrs = proj.get("@attributes", {})
                 
                 # Participantes (Equipe)
                 equipe_node = proj.get("EQUIPE-DO-PROJETO", [])
                 if not isinstance(equipe_node, list):
                     equipe_node = [equipe_node]
                 
                 integrantes = []
                 for eq in equipe_node:
                      for integrante in eq.get("INTEGRANTES-DO-PROJETO", []):
                           if isinstance(integrante, dict): # Check safety
                                int_attrs = integrante.get("@attributes", {})
                                integrantes.append(int_attrs.get("NOME-COMPLETO", ""))
                 
                 projetos.append({
                     "ano_inicio": attrs.get("ANO-INICIO"),
                     "ano_fim": attrs.get("ANO-FIM"),
                     "nome_projeto": attrs.get("NOME-DO-PROJETO"),
                     "descricao": attrs.get("DESCRICAO-DO-PROJETO"),
                     "situacao": attrs.get("SITUACAO"),
                     "natureza": attrs.get("NATUREZA"),
                     "integrantes": integrantes
                 })
                 
        return projetos

    @staticmethod
    def obter_detalhes_pos_doutorado(
        codpes: int, lattes_array: dict[str, Any] | None = None
    ) -> list[dict[str, Any]] | bool:
        """
        Retorna detalhes de pós-doutorado extraídos da Formação Acadêmica do Lattes.
        """
        lattes = lattes_array if lattes_array else Lattes.obter_array(codpes)
        if not lattes:
            return False
            
        formacao = get_path(lattes, "DADOS-GERAIS.FORMACAO-ACADEMICA-TITULACAO.POS-DOUTORADO")
        if not formacao:
             return False
             
        if not isinstance(formacao, list):
             formacao = [formacao]
             
        pds = []
        for pd in formacao:
             attrs = pd.get("@attributes", {})
             pds.append({
                 "ano_inicio": attrs.get("ANO-DE-INICIO"),
                 "ano_conclusao": attrs.get("ANO-DE-CONCLUSAO"),
                 "instituicao": attrs.get("NOME-INSTITUICAO"),
                 "status": attrs.get("STATUS-DO-CURSO"),
                 "agencia_fomento": attrs.get("NOME-AGENCIA"), 
             })
             
        return pds

    @staticmethod
    def listar_areas_conhecimento(codpes: int) -> list[str]:
        """
        Mapeia áreas do Lattes usando AREACONHECIMENTOCNPQ.
        """
        lattes = Lattes.obter_array(codpes)
        if not lattes:
            return []

        areas_lattes = get_path(
            lattes, "DADOS-GERAIS.AREAS-DE-ATUACAO.AREA-DE-ATUACAO", []
        )
        if not isinstance(areas_lattes, list):
            areas_lattes = [areas_lattes]

        nomes_areas = []
        for area in areas_lattes:
            nome = area.get("@attributes", {}).get("NOME-DA-AREA-DO-CONHECIMENTO")
            if nome:
                nomes_areas.append(nome)

        return nomes_areas

    @staticmethod
    def retornar_genero_pesquisador(codpes: int) -> str | None:
        """
        Retorna o gênero do pesquisador da tabela PESSOA.
        """
        query = "SELECT sexpes FROM PESSOA WHERE codpes = :codpes"
        try:
            res = DB.fetch(query, {"codpes": codpes})
            return res["sexpes"] if res else None
        except Exception as e:
            logger.warning(f"Erro ao retornar gênero: {e}")
            return None
