import unittest
from unittest.mock import patch

from replicado.beneficio import Beneficio


class TestBeneficio(unittest.TestCase):
    @patch("replicado.connection.DB.fetch_all")
    def test_listar_beneficios(self, mock_fetch) -> None:
        mock_fetch.return_value = [{"tipbnfalu": "Bolsa", "nompesttd": "Fulano"}]
        res = Beneficio.listar_beneficios()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["tipbnfalu"], "Bolsa")

    @patch("replicado.connection.DB.fetch_all")
    def test_listar_monitores_pro_aluno(self, mock_fetch) -> None:
        mock_fetch.return_value = [{"codpes": 123}]
        res = Beneficio.listar_monitores_pro_aluno([1, 2])
        self.assertEqual(len(res), 1)
        # Check if query contains IN (:cod0, :cod1) logic roughly
        args, kwargs = mock_fetch.call_args
        query = args[0]
        self.assertIn("IN (:cod0, :cod1)", query)
        self.assertEqual(args[1]["cod0"], "1")
        self.assertEqual(args[1]["cod1"], "2")

    @patch("replicado.connection.DB.fetch_all")
    def test_listar_monitores_pro_aluno_string(self, mock_fetch) -> None:
        mock_fetch.return_value = [{"codpes": 123}]
        res = Beneficio.listar_monitores_pro_aluno("1, 2")
        self.assertEqual(len(res), 1)
        # Check call args
        args, kwargs = mock_fetch.call_args
        self.assertEqual(args[1]["cod0"], "1")
        self.assertEqual(args[1]["cod1"], "2")
