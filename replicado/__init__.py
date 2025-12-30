import logging

from .aex import AEX as AEX
from .bempatrimoniado import Bempatrimoniado as Bempatrimoniado
from .beneficio import Beneficio as Beneficio
from .cartao import CartaoUSP as CartaoUSP
from .ceu import CEU as CEU
from .convenio import Convenio as Convenio
from .estrutura import Estrutura as Estrutura
from .financeiro import Financeiro as Financeiro
from .graduacao import Graduacao as Graduacao
from .lattes import Lattes as Lattes
from .pesquisa import Pesquisa as Pesquisa
from .pessoa import Pessoa as Pessoa
from .posgraduacao import Posgraduacao as Posgraduacao

# Configuração do logger da biblioteca
logger = logging.getLogger("replicado")
logger.addHandler(logging.NullHandler())
