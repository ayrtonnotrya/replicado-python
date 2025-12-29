import os
import unittest
from unittest.mock import patch

from replicado.ceu import CEU


class TestCEU(unittest.TestCase):
    @patch("replicado.connection.DB.fetch_all")
    @patch.dict(os.environ, {"REPLICADO_CODUNDCLG": "12,34"})
    def test_listar_cursos(self, mock_fetch) -> None:
        # First call: listarCursos main query
        # Second call: ministrantes for course 1

        curso_mock = {"codcurceu": 1, "codedicurceu": 1, "nomcurceu": "Curso Teste"}

        ministrante_mock = [{"nompes": "Ministrante A"}]

        mock_fetch.side_effect = [[curso_mock], ministrante_mock]

        cursos = CEU.listar_cursos(2023, 2023)
        self.assertEqual(len(cursos), 1)
        self.assertEqual(cursos[0]["nomcurceu"], "Curso Teste")
        self.assertEqual(cursos[0]["ministrantes"], "Ministrante A")

    @patch("replicado.connection.DB.fetch_all")
    def test_listar_cursos_deptos(self, mock_fetch) -> None:
        mock_fetch.side_effect = [[], []]
        CEU.listar_cursos(deptos=[1, 2])
        args, _ = mock_fetch.call_args
        query = args[0]
        self.assertIn("AND C.codsetdep IN (1,2)", query)
