# Replicado Python

**Uma re-implementação idiomática para Python da biblioteca [uspdev/replicado](https://github.com/uspdev/replicado).**

---

## 1. Introdução

O **Replicado Python** é uma biblioteca fundamental para projetos de Ciência de Dados e aplicações corporativas na Universidade de São Paulo. Seu objetivo é abstrair a complexidade de conexão e consulta aos bancos de dados legados (Sybase ASE / MSSQL) que compõem a réplica local da USP.

Enquanto a versão original em PHP foca em arrays associativos, esta versão Python tira proveito do **SQLAlchemy 2.0** para fornecer:
*   **Mapeamento Objeto-Relacional (ORM):** Trabalhe com objetos Python em vez de strings SQL puras.
*   **Segurança:** Prevenção automática contra *SQL Injection*.
*   **Compatibilidade:** Suporte nativo para conversão de *charsets* (ISO-8859-1 para UTF-8), crucial para dados legados da universidade.
*   **Integração:** Pronto para uso com Django 5.2, FastAPI ou scripts de automação.

---

## 2. Guia de Instalação

### Pré-requisitos de Sistema (Drivers Sybase)
Antes de instalar o pacote Python, você deve garantir que as bibliotecas de sistema para comunicação com Sybase/MSSQL (FreeTDS) estejam instaladas.

**Debian/Ubuntu/Docker:**
```bash
sudo apt-get update && sudo apt-get install -y \
    freetds-dev \
    freetds-bin \
    tdsodbc
```

**macOS (Homebrew):**
```bash
brew install freetds
```

### Instalação do Pacote (Via Poetry)
No diretório raiz do seu projeto, execute:

```bash
poetry add replicado-python
```

*Isso instalará automaticamente o `SQLAlchemy` e o driver `pymssql` compatível.*

---

## 3. Configuração de Ambiente (.env)

A biblioteca segue o padrão *The Twelve-Factor App*. Configure as variáveis abaixo no seu arquivo `.env`. Elas mantêm compatibilidade total com os nomes usados na versão PHP.

| Variável | Descrição | Exemplo |
| :--- | :--- | :--- |
| `REPLICADO_HOST` | Endereço IP ou Hostname do servidor da réplica. | `192.168.0.10` |
| `REPLICADO_PORT` | Porta de conexão (Geralmente 1433 para MSSQL ou 5000 para Sybase). | `5000` |
| `REPLICADO_DATABASE` | Nome do banco de dados principal. | `replicacao` |
| `REPLICADO_USERNAME` | Usuário de leitura fornecido pela STI. | `leitor_replicado` |
| `REPLICADO_PASSWORD` | Senha de acesso. | `s3cr3t_usp` |
| `REPLICADO_CODUNDCLG`| Código da Unidade (Colegiado). | `8` (FFLCH), `18` (ICMC) |
| `REPLICADO_SYBASE` | **Booleano (1/0)**. Se `1`, força conversão UTF-8 e ajustes específicos para driver Sybase antigo. | `1` |

---

## 4. Exemplos de Uso

### 4.1. Inicialização

Geralmente feito no `settings.py` (Django) ou num módulo `config.py`.

```python
from replicado.connection import DB

# Inicializa o Singleton de conexão (lê automaticamente do .env)
# O parâmetro echo=True exibe o SQL gerado no console (útil para debug)
database = DB(echo=False)
```

### 4.2. Consultas com a Classe `Pessoa`

Abaixo, um exemplo de como substituir uma consulta SQL crua por um método estático tipado, padrão que deve ser seguido por todo o projeto.

```python
from typing import Optional
from sqlalchemy import text
from replicado.connection import DB
from replicado.models import PessoaModel # Modelo SQLAlchemy fictício

class Pessoa:
    """
    Classe de domínio para operações relacionadas a pessoas (tabela PESSOA).
    """

    @staticmethod
    def obter_nome(codpes: int) -> Optional[str]:
        """
        Obtém o nome completo de uma pessoa pelo N.USP.
        
        Args:
            codpes (int): Número USP.
            
        Returns:
            Optional[str]: Nome da pessoa ou None se não encontrado.
        """
        # Exemplo utilizando Core (SQL puro seguro) se não houver Model mapeado
        query = text("SELECT nompes FROM PESSOA WHERE codpes = :codpes")
        
        with DB.engine.connect() as conn:
            result = conn.execute(query, {"codpes": codpes}).fetchone()
            
            if result:
                return str(result.nompes) # O driver já trata o decode UTF-8 se configurado
            return None

    @staticmethod
    def obter_vinculos(codpes: int) -> list[dict]:
        """
        Retorna lista de vínculos ativos (exemplo com ORM/Model).
        """
        session = DB.get_session()
        # Supondo que existe um mapeamento para a view VINCULOPESSOAUSP
        # ... lógica de consulta SQLAlchemy ...
        return []
```

### 4.3 Uso no Django View

```python
# views.py
from django.http import JsonResponse
from myapp.replicado_utils import Pessoa

def busca_servidor(request, codpes):
    nome = Pessoa.obter_nome(codpes)
    if nome:
        return JsonResponse({"codpes": codpes, "nome": nome})
    return JsonResponse({"error": "Pessoa não encontrada"}, status=404)
```