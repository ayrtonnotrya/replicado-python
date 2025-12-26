import unittest
from unittest.mock import patch, MagicMock
from replicado.lattes import Lattes

class TestLattes(unittest.TestCase):
    
    @patch('replicado.connection.DB.fetch')
    def test_id(self, mock_fetch):
        mock_fetch.return_value = {'idfpescpq': '12345'}
        self.assertEqual(Lattes.id(123), '12345')
        
    @patch('replicado.connection.DB.fetch')
    def test_retornar_codpes(self, mock_fetch):
        mock_fetch.return_value = {'codpes': 123}
        self.assertEqual(Lattes.retornar_codpes_por_id_lattes('12345'), 123)
        
    @patch('replicado.lattes.Lattes.obter_array')
    def test_listar_premios(self, mock_array):
        # Mocking the dictionary structure expected
        mock_array.return_value = {
            'DADOS-GERAIS': {
                'PREMIOS-TITULOS': {
                    'PREMIO-TITULO': [
                        {'@attributes': {'NOME-DO-PREMIO-OU-TITULO': 'Prêmio A', 'ANO-DA-PREMIACAO': '2020'}},
                        {'@attributes': {'NOME-DO-PREMIO-OU-TITULO': 'Prêmio B', 'ANO-DA-PREMIACAO': '2021'}}
                    ]
                }
            }
        }
        premios = Lattes.listar_premios(123)
        self.assertEqual(len(premios), 2)
        self.assertIn('Prêmio A - Ano: 2020', premios)

    @patch('replicado.lattes.Lattes.obter_array')
    def test_listar_artigos(self, mock_array):
        mock_array.return_value = {
            'PRODUCAO-BIBLIOGRAFICA': {
                'ARTIGOS-PUBLICADOS': {
                    'ARTIGO-PUBLICADO': [
                        {
                            'DADOS-BASICOS-DO-ARTIGO': {'@attributes': {'TITULO-DO-ARTIGO': 'Artigo 1', 'ANO-DO-ARTIGO': '2021'}},
                            'DETALHAMENTO-DO-ARTIGO': {'@attributes': {'TITULO-DO-PERIODICO-OU-REVISTA': 'Revista X'}},
                            'AUTORES': [{'@attributes': {'NOME-COMPLETO-DO-AUTOR': 'Autor 1', 'ORDEM-DE-AUTORIA': '1'}}]
                        },
                         {
                            'DADOS-BASICOS-DO-ARTIGO': {'@attributes': {'TITULO-DO-ARTIGO': 'Artigo 2', 'ANO-DO-ARTIGO': '2020'}}
                        }
                    ]
                }
            }
        }
        # Default sort desc by year
        artigos = Lattes.listar_artigos(123)
        self.assertEqual(len(artigos), 2)
        self.assertEqual(artigos[0]['TITULO-DO-ARTIGO'], 'Artigo 1')
        self.assertEqual(artigos[0]['ANO'], '2021')
        
    @patch('replicado.lattes.Lattes.obter_array')
    def test_retornar_resumo_cv(self, mock_array):
         mock_array.return_value = {
             'DADOS-GERAIS': {
                 'RESUMO-CV': {
                     '@attributes': {'TEXTO-RESUMO-CV-RH': 'Resumo &amp; Teste'}
                 }
             }
         }
         resumo = Lattes.retornar_resumo_cv(123)
         self.assertEqual(resumo, 'Resumo & Teste')

    def test_verificar_filtro(self):
        # registros
        self.assertTrue(Lattes.verificar_filtro('registros', 0, 5, 0, 1))
        self.assertFalse(Lattes.verificar_filtro('registros', 0, 5, 0, 6))
        
    @patch('replicado.lattes.Lattes.obter_array')
    def test_listar_linhas_pesquisa(self, mock_array):
        mock_array.return_value = {
            'DADOS-GERAIS': {
                'ATUACOES-PROFISSIONAIS': {
                    'ATUACAO-PROFISSIONAL': {
                        'ATIVIDADES-DE-PESQUISA-E-DESENVOLVIMENTO': {
                            'PESQUISA-E-DESENVOLVIMENTO': {
                                'LINHA-DE-PESQUISA': {'@attributes': {'TITULO-DA-LINHA-DE-PESQUISA': 'Linha 1'}}
                            }
                        }
                    }
                }
            }
        }
        linhas = Lattes.listar_linhas_pesquisa(123)
        self.assertEqual(linhas, ['Linha 1'])

    def test_listar_capitulos_livros(self):
        mock_array = {
            'PRODUCAO-BIBLIOGRAFICA': {
                'LIVROS-E-CAPITULOS': {
                    'CAPITULOS-DE-LIVROS-PUBLICADOS': {
                        'CAPITULO-DE-LIVRO-PUBLICADO': {
                            '@attributes': {'SEQUENCIA-PRODUCAO': '1'},
                            'DADOS-BASICOS-DO-CAPITULO': {'@attributes': {'TITULO-DO-CAPITULO-DO-LIVRO': 'CapITULO A', 'ANO': '2023'}},
                            'DETALHAMENTO-DO-CAPITULO': {'@attributes': {'TITULO-DO-LIVRO': 'Livro A'}},
                            'AUTORES': [{'@attributes': {'NOME-COMPLETO-DO-AUTOR': 'Autor A', 'ORDEM-DE-AUTORIA': '1'}}]
                        }
                    }
                }
            }
        }
        res = Lattes.listar_capitulos_livros(123, lattes_array=mock_array)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['TITULO-DO-CAPITULO-DO-LIVRO'], 'CapITULO A')

    def test_listar_trabalhos_anais(self):
        mock_array = {
            'PRODUCAO-BIBLIOGRAFICA': {
                'TRABALHOS-EM-EVENTOS': {
                    'TRABALHO-EM-EVENTOS': {
                        '@attributes': {'SEQUENCIA-PRODUCAO': '1'},
                        'DADOS-BASICOS-DO-TRABALHO': {'@attributes': {'TITULO-DO-TRABALHO': 'Trabalho A', 'ANO-DO-TRABALHO': '2023'}},
                        'DETALHAMENTO-DO-TRABALHO': {'@attributes': {'NOME-DO-EVENTO': 'Evento A'}},
                        'AUTORES': []
                    }
                }
            }
        }
        res = Lattes.listar_trabalhos_anais(123, lattes_array=mock_array)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['TITULO'], 'Trabalho A')

    def test_listar_trabalhos_tecnicos(self):
        mock_array = {
            'PRODUCAO-TECNICA': {
                'TRABALHO-TECNICO': {
                    '@attributes': {'SEQUENCIA-PRODUCAO': '1'},
                    'DADOS-BASICOS-DO-TRABALHO-TECNICO': {'@attributes': {'TITULO-DO-TRABALHO-TECNICO': 'Tecnico A', 'ANO': '2023'}},
                    'AUTORES': []
                }
            }
        }
        res = Lattes.listar_trabalhos_tecnicos(123, lattes_array=mock_array)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['TITULO'], 'Tecnico A')

    def test_listar_outras_producoes_bibliograficas(self):
        mock_array = {
            'PRODUCAO-BIBLIOGRAFICA': {
                'DEMAIS-TIPOS-DE-PRODUCAO-BIBLIOGRAFICA': {
                    'OUTRA-PRODUCAO-BIBLIOGRAFICA': {
                        '@attributes': {'SEQUENCIA-PRODUCAO': '1'},
                        'DADOS-BASICOS-DE-OUTRA-PRODUCAO': {'@attributes': {'TITULO': 'Outra B', 'ANO': '2023'}},
                        'AUTORES': []
                    },
                    'TRADUCAO': {
                        '@attributes': {'SEQUENCIA-PRODUCAO': '2'},
                        'DADOS-BASICOS-DA-TRADUCAO': {'@attributes': {'TITULO': 'Traducao A', 'ANO': '2022', 'TIPO': 'LIVRO'}},
                        'AUTORES': []
                    }
                }
            }
        }
        res = Lattes.listar_outras_producoes_bibliograficas(123, lattes_array=mock_array)
        # Sorted desc so Traducao (2) comes first
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['TITULO'], 'Traducao A')
        self.assertEqual(res[1]['TITULO'], 'Outra B')

    def test_listar_teses_e_livre_docencia(self):
        mock_array = {
            'DADOS-GERAIS': {
                'FORMACAO-ACADEMICA-TITULACAO': {
                    'DOUTORADO': {
                        '@attributes': {'TITULO-DA-DISSERTACAO-TESE': 'Tese DR', 'ANO-DE-OBTENCAO-DO-TITULO': '2023'},
                        'PALAVRAS-CHAVE': {'@attributes': {'PALAVRA-CHAVE-1': 'K1'}}
                    },
                    'LIVRE-DOCENCIA': {
                        '@attributes': {'TITULO-DO-TRABALHO': 'Tese LD', 'ANO-DE-OBTENCAO-DO-TITULO': '2022'}
                    }
                }
            }
        }
        res_tese = Lattes.listar_teses(123, tipo='DOUTORADO', lattes_array=mock_array)
        self.assertEqual(res_tese[0]['TITULO'], 'Tese DR')
        self.assertEqual(res_tese[0]['PALAVRAS-CHAVE'], 'K1')
        
        res_ld = Lattes.obter_livre_docencia(123, lattes_array=mock_array)
        self.assertEqual(res_ld[0], 'Tese LD')

    def test_retornar_bancas(self):
        mock_array = {
            'DADOS-COMPLEMENTARES': {
                'PARTICIPACAO-EM-BANCA-TRABALHOS-CONCLUSAO': {
                    'PARTICIPACAO-EM-BANCA-DE-MESTRADO': {
                        'DADOS-BASICOS-DA-PARTICIPACAO-EM-BANCA-DE-MESTRADO': {'@attributes': {'TITULO': 'Banca MS'}}
                    },
                    'PARTICIPACAO-EM-BANCA-DE-DOUTORADO': {
                        'DADOS-BASICOS-DA-PARTICIPACAO-EM-BANCA-DE-DOUTORADO': {'@attributes': {'TITULO': 'Banca DR'}},
                        'DETALHAMENTO-DA-PARTICIPACAO-EM-BANCA-DE-DOUTORADO': {'@attributes': {'NOME-DO-CANDIDATO': 'Cand A'}}
                    }
                }
            }
        }
        res_ms = Lattes.retornar_banca_mestrado(123, lattes_array=mock_array)
        self.assertEqual(res_ms[0], 'Banca MS')
        
        res_dr = Lattes.retornar_banca_doutorado(123, lattes_array=mock_array)
        self.assertIn('Banca DR', res_dr[0])
        self.assertIn('Cand A', res_dr[0])
