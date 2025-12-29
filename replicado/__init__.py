import logging

from .bempatrimoniado import Bempatrimoniado as Bempatrimoniado
from .estrutura import Estrutura as Estrutura
from .graduacao import Graduacao as Graduacao
from .pessoa import Pessoa as Pessoa
from .posgraduacao import Posgraduacao as Posgraduacao

# Configuração do logger da biblioteca
logger = logging.getLogger("replicado")
logger.addHandler(logging.NullHandler())
