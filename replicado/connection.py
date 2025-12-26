import os
from typing import Optional, List, Any, Dict, Tuple
from dotenv import load_dotenv
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker, Session
from .utils import clean_string

# Carrega variáveis de ambiente do .env
load_dotenv()

class DB:
    """
    Singleton para gerenciar a conexão com o banco de dados.
    """
    _engine: Optional[Engine] = None
    _session_factory: Optional[sessionmaker] = None

    @classmethod
    def get_engine(cls) -> Engine:
        """
        Retorna a engine do SQLAlchemy, criando-a se necessário.
        
        Returns:
            Engine: Objeto engine do SQLAlchemy.
            
        Raises:
            ValueError: Se as variáveis de ambiente obrigatórias não estiverem definidas.
        """
        if cls._engine is None:
            host = os.getenv("REPLICADO_HOST")
            port = os.getenv("REPLICADO_PORT")
            database = os.getenv("REPLICADO_DATABASE")
            user = os.getenv("REPLICADO_USERNAME")
            password = os.getenv("REPLICADO_PASSWORD")
            # O parâmetro REPLICADO_SYBASE pode ser usado no futuro para ajustes finos de charset,
            # mas o pymssql geralmente lida bem se configurado corretamente no freetds ou charset na string.
            # Por padrão, vamos assumir charset=UTF-8 na connection string se possível ou deixar o driver negociar.
            
            if not all([host, port, database, user, password]):
                raise ValueError("Variáveis de ambiente de conexão (REPLICADO_*) não definidas.")

            # Montagem da URL de conexão para MSSQL/Sybase via pymssql
            # Formato: mssql+pymssql://<username>:<password>@<host>:<port>/<database>?charset=utf8
            connection_string = (
                f"mssql+pymssql://{user}:{password}@{host}:{port}/{database}?charset=utf8"
            )

            cls._engine = create_engine(
                connection_string,
                pool_pre_ping=True, # Verifica se a conexão está viva antes de usar
                echo=False # Pode ser parametrizado futuramente
            )
        
        return cls._engine

    @classmethod
    def get_session(cls) -> Session:
        """
        Cria e retorna uma nova sessão do SQLAlchemy.
        
        Returns:
            Session: Sessão do SQLAlchemy.
        """
        if cls._session_factory is None:
            engine = cls.get_engine()
            cls._session_factory = sessionmaker(bind=engine)
        
        return cls._session_factory()

    @classmethod
    def execute(cls, query: str, params: Optional[dict] = None) -> Any:
        """
        Executa uma query raw.
        
        Args:
           query (str): Query SQL.
           params (dict, optional): Parâmetros para bind.

        Returns:
           ResultProxy do SQLAlchemy.
        """
        with cls.get_engine().connect() as conn:
            return conn.execute(text(query), params or {})

    @classmethod
    def fetch_all(cls, query: str, params: Optional[dict] = None) -> List[dict]:
        """
        Executa query e retorna todos os resultados como dicionários.
        
        Args:
            query (str): SQL Query.
            params (dict, optional): Parameters.

        Returns:
            List[dict]: Lista de resultados.
        """
        with cls.get_engine().connect() as conn:
            result = conn.execute(text(query), params or {})
            return [{k: clean_string(v) for k, v in row._mapping.items()} for row in result]

    @classmethod
    def fetch(cls, query: str, params: Optional[dict] = None) -> Optional[dict]:
        """
        Executa query e retorna o primeiro resultado.
        
        Args:
             query (str): SQL Query.
             params (dict, optional): Parameters.

        Returns:
            Optional[dict]: Resultado ou None.
        """
        with cls.get_engine().connect() as conn:
            result = conn.execute(text(query), params or {}).fetchone()
            if result:
                 return {k: clean_string(v) for k, v in result._mapping.items()}
            return None

    @classmethod
    def cria_filtro_busca(cls, filtros: Dict[str, Any], buscas: Dict[str, Any], tipos: Dict[str, str]) -> Tuple[str, Dict[str, Any]]:
        """
        Cria cláusula WHERE para filtros e buscas.
        
        Args:
            filtros (dict): campo => valor (AND)
            buscas (dict): campo => valor (OR entre buscas, AND com filtros)
            tipos (dict): campo => tipo sql (ex: 'int')

        Returns:
            Tuple[str, dict]: (Cláusula WHERE, params)
        """
        str_where = ""
        params = {}

        if filtros:
            str_where += " WHERE ("
            keys = list(filtros.keys())
            for i, coluna in enumerate(keys):
                sanitized = coluna.replace('.', '')
                if coluna in tipos:
                    # No python executamos a conversão no parametro se necessario, mas aqui mantemos o SQL 
                    # sqlsrv/mssql converte auto? O original usa CONVERT.
                    # Vamos manter simples: param bind
                    # Mas se o original forca convert no SQL, pode ser necessario.
                    # Ex: CONVERT(int, :param)
                    str_where += f" {coluna} = CONVERT({tipos[coluna]}, :{sanitized}) "
                else:
                    str_where += f" {coluna} = :{sanitized} "
                
                params[sanitized] = filtros[coluna]

                if i < len(keys) - 1:
                    str_where += " AND "
        
        if buscas:
            if str_where:
                str_where += ") AND ("
            else:
                str_where += " WHERE ("
            
            keys = list(buscas.keys())
            for i, coluna in enumerate(keys):
                sanitized = coluna.replace('.', '')
                str_where += f" {coluna} LIKE :{sanitized} "
                params[sanitized] = f"%{buscas[coluna]}%"

                if i < len(keys) - 1:
                    str_where += " OR "
                else:
                    str_where += ") "
        else:
            if str_where:
                str_where += ")"
        
        return str_where, params

