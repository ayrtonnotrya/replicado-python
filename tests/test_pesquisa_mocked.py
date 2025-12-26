import unittest
from unittest.mock import patch, MagicMock
from replicado.pesquisa import Pesquisa
import os

class TestPesquisa(unittest.TestCase):
    
    @patch('replicado.connection.DB.fetch_all')
    @patch('replicado.connection.DB.fetch')
    @patch('replicado.pessoa.Pessoa.retornar_curso_por_codpes')
    @patch.dict(os.environ, {'REPLICADO_CODUNDCLG': '12'})
    def test_listar_iniciacao_cientifica(self, mock_curso, mock_fetch, mock_fetch_all):
        # 1. Main query
        # 2. Scholarship check (fetch)
        
        ic_mock = {
            'cod_projeto': 1,
            'aluno': 123,
            'nome_aluno': 'Aluno A'
        }
        mock_fetch_all.return_value = [ic_mock]
        
        mock_curso.return_value = {'codcurgrd': 10, 'nomcur': 'Curso A'}
        mock_fetch.return_value = {'codctgedi': '1'} # PIBIC
        
        res = Pesquisa.listar_iniciacao_cientifica()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['codcur'], 10)
        self.assertEqual(res[0]['codctgedi'], 'PIBIC')
        self.assertEqual(res[0]['bolsa'], 'true')

    @patch('replicado.connection.DB.fetch_all')
    def test_listar_pesquisadores_colaboradores_ativos(self, mock_fetch):
        mock_fetch.return_value = [{'codpes': 1}]
        res = Pesquisa.listar_pesquisadores_colaboradores_ativos()
        self.assertEqual(len(res), 1)

    @patch('replicado.connection.DB.fetch_all')
    @patch('replicado.connection.DB.fetch')
    @patch.dict(os.environ, {'REPLICADO_CODUNDCLG': '12'})
    def test_listar_pesquisa_pos_doutorandos(self, mock_fetch, mock_fetch_all):
        # 1. Main query (fetchAll)
        # 2. Supervisor (fetch)
        # 3. Scholarship (fetch)
        
        pd_mock = {'codpes': 456, 'codprj': 2, 'nome_aluno': 'PD A'}
        mock_fetch_all.return_value = [pd_mock]
        
        # side_effect for two fetch calls
        mock_fetch.side_effect = [
            {'nompes': 'Supervisor S'}, # Supervisor
            {'codpes': 456} # Scholarship exists
        ]
        
        res = Pesquisa.listar_pesquisa_pos_doutorandos()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['supervisor'], 'Supervisor S')
        self.assertEqual(res[0]['bolsa'], 'true')

    @patch('replicado.connection.DB.fetch_all')
    @patch.dict(os.environ, {'REPLICADO_CODUNDCLG': '12'})
    def test_contar_pd_por_ano(self, mock_fetch):
        # 1. Statuses autodetect (if none provided)
        # 2. Main query
        
        mock_fetch.side_effect = [
            [{'staatlprj': 'Ativo'}], # Autodetect statuses
            [{'Ano': 2023, 'qtdProjetosAtivos': 5}] # Main count
        ]
        
        res = Pesquisa.contar_pd_por_ano()
        self.assertEqual(res[2023], 5)

    @patch('replicado.connection.DB.fetch_all')
    @patch.dict(os.environ, {'REPLICADO_CODUNDCLG': '12'})
    def test_contar_pd_por_ultimos_12_meses(self, mock_fetch):
        mock_fetch.side_effect = [
            [{'staatlprj': 'Ativo'}],
            [{'AnoMes': '2023-01', 'qtdProjetosAtivos': 10}]
        ]
        res = Pesquisa.contar_pd_por_ultimos_12_meses()
        self.assertEqual(res['2023-01'], 10)
