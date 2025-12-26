from replicado import Pessoa
from replicado import Graduacao
from replicado import Posgraduacao
from replicado import Bempatrimoniado
from replicado import Estrutura
from replicado.utils import clean_string, remove_accents, dia_semana

def test_imports():
    assert Pessoa is not None
    assert Graduacao is not None
    assert Posgraduacao is not None
    assert Bempatrimoniado is not None
    assert Estrutura is not None
    assert callable(clean_string)
    assert callable(remove_accents)
    assert callable(dia_semana)
