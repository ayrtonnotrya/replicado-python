import unittest
from unittest.mock import patch, MagicMock
from replicado.financeiro import Financeiro
import os

class TestFinanceiro(unittest.TestCase):
    
    @patch('replicado.connection.DB.fetch_all')
    @patch.dict(os.environ, {'REPLICADO_CODUNDCLG': '12,34'})
    def test_listar_centros_despesas(self, mock_fetch):
        mock_fetch.return_value = [{'etrhie': 'Centro A'}]
        res = Financeiro.listar_centros_despesas()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['etrhie'], 'Centro A')
        
        args, _ = mock_fetch.call_args
        query = args[0]
        self.assertIn('IN (12,34)', query)

    @patch.dict(os.environ, {'REPLICADO_CODUNDCLG': ''}, clear=True)
    def test_listar_centros_despesas_empty(self):
        res = Financeiro.listar_centros_despesas()
        self.assertEqual(res, [])
