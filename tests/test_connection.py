import os
import pytest
from unittest.mock import patch, MagicMock
from replicado.connection import DB

def test_db_singleton():
    """Verifica se o DB se comporta como singleton."""
    # Reset state
    DB._engine = None
    
    with patch("replicado.connection.create_engine") as mock_create_engine:
        # Mock env vars
        with patch.dict(os.environ, {
            "REPLICADO_HOST": "localhost",
            "REPLICADO_PORT": "1433",
            "REPLICADO_DATABASE": "test_db",
            "REPLICADO_USERNAME": "user",
            "REPLICADO_PASSWORD": "pass"
        }):
            engine1 = DB.get_engine()
            engine2 = DB.get_engine()
            
            assert engine1 is engine2
            mock_create_engine.assert_called_once()

def test_missing_env_vars():
    """Verifica se lança erro quando variáveis estão faltando."""
    DB._engine = None
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="Variáveis de ambiente de conexão"):
            DB.get_engine()
