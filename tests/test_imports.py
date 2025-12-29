from replicado import Bempatrimoniado, Estrutura, Graduacao, Pessoa, Posgraduacao
from replicado.utils import clean_string, dia_semana, remove_accents


def test_imports() -> None:
    assert Pessoa is not None
    assert Graduacao is not None
    assert Posgraduacao is not None
    assert Bempatrimoniado is not None
    assert Estrutura is not None
    assert callable(clean_string)
    assert callable(remove_accents)
    assert callable(dia_semana)
