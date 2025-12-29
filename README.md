# Replicado Python

**Uma re-implementação idiomática para Python da biblioteca [uspdev/replicado](https://github.com/uspdev/replicado).**

---

## 🚀 1. Introdução

O **Replicado Python** é uma biblioteca de integração com os bancos de dados legados da Universidade de São Paulo (Sybase ASE / MSSQL), abstraindo a complexidade de conexão e consulta à réplica local da USP.

Esta versão Python traz modernidade e segurança para o ecossistema USP:
*   **Abstração SQL**: Utiliza **SQLAlchemy 2.0** com parâmetros nomeadores (:bind_params) para evitar SQL Injection.
*   **Type Hinting Estrito**: Compatível com IDEs e Agentes de IA para autocompletar e validação estática.
*   **Tratamento de Dados**: Strings são automaticamente normalizadas (`.strip()`) para remover espaços em branco comuns em colunas `CHAR` do Sybase.
*   **Logging Integrado**: Monitoramento detalhado de cada execução SQL.

---

## 🛠 2. Instalação e Configuração

### Pré-requisitos
*   **Python 3.14**: O projeto utiliza a versão 3.14 (gerenciada via `asm`).
*   **FreeTDS**: Necessário para comunicação com Sybase/MSSQL (no Debian/Ubuntu: `sudo apt-get install freetds-dev freetds-bin tdsodbc`).

### Instalação
```bash
poetry add replicado-python
```

### Configuração (.env)
A biblioteca inicializa automaticamente através de variáveis de ambiente:

> [!IMPORTANT]
> Certifique-se de que sua Unidade (`REPLICADO_CODUNDCLG`) está configurada corretamente para filtrar resultados automáticos em vários métodos.

| Variável | Exemplo |
| :--- | :--- |
| `REPLICADO_HOST` | `10.0.0.1` |
| `REPLICADO_DATABASE` | `replicacao` |
| `REPLICADO_USERNAME` | `seu_usuario` |
| `REPLICADO_PASSWORD` | `sua_senha` |
| `REPLICADO_CODUNDCLG` | `45` (IME), `18` (ICMC) |

---

## 📖 3. Guia de Referência

### Módulos Portados
A biblioteca é organizada em classes estáticas que agrupam funcionalidades de negócio:

| Classe | Descrição | Exemplos de Métodos |
| :--- | :--- | :--- |
| **`Pessoa`** | Dados pessoais e institucionais | `dump`, `email`, `listar_docentes`, `telefones` |
| **`Lattes`** | Extração de currículos Lattes (XML) | `obter_json`, `listar_artigos`, `listar_teses` |
| **`Graduacao`** | Vida acadêmica graduação | `verificar_aluno`, `obter_media_ponderada` |
| **`Posgraduacao`** | Pós-graduação e Defesas | `programas`, `listar_defesas`, `orientadores` |
| **`Pesquisa`** | Iniciação Científica e Pós-Doutorado | `listar_iniciacao_cientifica`, `contar_pd_por_ano` |
| **`Estrutura`** | Unidade, Setores e Chefias | `listar_setores`, `get_chefia_setor`, `obter_unidade` |
| **`CEU`** | Cursos de Extensão | `listar_cursos` |
| **`Convenio`** | Acordos Internacionais | `listar_convenios_academicos_internacionais` |
| **`Financeiro`** | Centros de Despesa | `listar_centros_despesas` |
| **`Bempatrimoniado`** | Ativos e Patrimônios | `ativos`, `is_informatica` |

---

## 🤖 4. Para Agentes de IA (System Prompt Integration)

Se você estiver integrando este pacote a um Agente de IA, estas diretrizes ajudarão o agente a realizar consultas sem alucinações:

*   **Identificador Único**: Use sempre o `codpes` (N.USP) como chave de busca principal.
*   **Resultados Vazios**: Se um dado não existe na réplica, a biblioteca retornará `None`, `False` ou uma lista vazia `[]`. Instrua o agente a tratar esses casos como "Dado não disponível no momento".
*   **Mapeamento Lattes**: O método `Lattes.obter_json(codpes)` retorna uma string JSON contendo o currículo completo. Utilize as ferramentas de parsing do seu agente para navegar por essa estrutura baseada no schema oficial do CNPq.
*   **Logging para Debug**: Para ver a query SQL exata que está sendo gerada, o agente pode configurar o logging para `DEBUG`.

---

## 💻 5. Exemplos de Código

### Consulta Simples
```python
from replicado import Pessoa

# Recupera email principal
email = Pessoa.email(123456)
if email:
    print(f"Email: {email}")
```

### Extração de Produção Acadêmica (Lattes)
```python
from replicado import Lattes

# Listar os últimos 5 artigos
artigos = Lattes.listar_artigos(123456, limite=5)
for art in artigos:
    print(f"{art['ANO']} - {art['TITULO']}")
```

### Ativação de Logs (Debug)
```python
import logging

# Habilita logs para ver as queries geradas no console
logging.basicConfig(level=logging.DEBUG)
```

---

## 🛠 6. Desenvolvimento

Para contribuir com o projeto, utilize o **Poetry** para gerenciar dependências e o **Ruff** para manter a qualidade do código.

### Linter e Formatador
```bash
# Verificar erros e aplicar correções automáticas
poetry run ruff check . --fix

# Formatar o código
poetry run ruff format .
```

---

## ⚖ 7. Licença
Este projeto é licenciado sob a licença MIT. Para detalhes sobre o banco de dados e políticas de acesso, consulte a [STI USP](https://sti.usp.br).