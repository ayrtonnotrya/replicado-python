import logging

# Configuração do logger da biblioteca
logger = logging.getLogger('replicado')
logger.addHandler(logging.NullHandler())

from .pessoa import Pessoa
from .graduacao import Graduacao
from .posgraduacao import Posgraduacao
from .bempatrimoniado import Bempatrimoniado
from .estrutura import Estrutura