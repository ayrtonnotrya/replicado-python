import os
import unittest
from datetime import datetime
from unittest.mock import patch

from replicado.convenio import Convenio


class TestConvenio(unittest.TestCase):
    @patch("replicado.connection.DB.fetch_all")
    @patch.dict(os.environ, {"REPLICADO_CODUNDCLG": "12"})
    def test_listar_convenios(self, mock_fetch) -> None:
        # Mock calls:
        # 1. listar convenios
        # 2. listar coordenadores (for conv 1)
        # 3. listar organizacoes (for conv 1)

        conv_mock = {
            "codcvn": 1,
            "nomeConvenio": "Conv A",
            "dataInicio": datetime(2023, 1, 1),
            "dataFim": datetime(2023, 12, 31),
        }

        coord_mock = [{"nompesttd": "Coord A"}]
        org_mock = [{"nomeOrganizacao": "Org A"}]

        mock_fetch.side_effect = [[conv_mock], coord_mock, org_mock]

        res = Convenio.listar_convenios_academicos_internacionais()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["coordenadores"], "Coord A")
        self.assertEqual(res[0]["organizacoes"], "Org A")
        self.assertEqual(res[0]["dataInicio"], "01/01/2023")

    @patch("replicado.connection.DB.fetch_all")
    def test_listar_convenios_inativos(self, mock_fetch) -> None:
        mock_fetch.side_effect = [[], [], []]
        Convenio.listar_convenios_academicos_internacionais(ativos=False)
        args, _ = mock_fetch.call_args
        query = args[0]
        self.assertIn("c.dtadtvcvn < GETDATE()", query)
