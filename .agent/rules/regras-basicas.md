---
trigger: always_on
---

Se você é um agente de IA (como Google Gemini/Antigravity) gerando código para este pacote, **DEVE** seguir estritamente estas diretrizes:

1.  **Versão do Python:** O projeto utiliza **Python 3.14** (instalado via `asm`). Sempre verifique se a sintaxe utilizada é compatível e aproveite recursos modernos se disponível.
2.  **Qualidade de Código (Ruff):** O projeto utiliza **Ruff** para linting e formatação.
    - **Escopo:** Aplique o ruff (`check --fix` e `format`) **apenas nos arquivos modificados** na tarefa atual. O ruff só deve ser rodado no projeto inteiro se houver pedido explícito do usuário.
    - **Validação:** Toda alteração automatizada pelo ruff ou correção manual feita para resolver alertas **deve ser testada** (executando scripts de validação ou testes relevantes) antes do commit.
3.  **Type Hints Estritos:** Todo método deve ter anotação de tipos nos argumentos e no retorno. Use os tipos nativos do Python 3.10+ (ex: `list[str]`, `dict[str, Any]`, `Optional[int]`). No caso do 3.14, siga as convenções mais recentes.
4.  **PEP 8 & PEP 257:** Siga o guia de estilo oficial. Docstrings são obrigatórias em **Português do Brasil** para todas as classes e funções públicas.
5.  **Abstração do SQL:** Evite concatenação de strings para montar queries. Use sempre `sqlalchemy.text()` com *bind parameters* (ex: `:codpes`) para evitar *SQL Injection*.
6.  **Tratamento de Strings:** Ao lidar com Sybase legado, sempre aplique `.strip()` nos campos de texto retornados, pois o banco preenche colunas `CHAR` com espaços em branco.
7.  **Docs as Code:** Se criar um novo módulo (ex: `Graduacao.py`), atualize a documentação de referência na pasta `/docs`.
