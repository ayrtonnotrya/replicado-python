---
trigger: always_on
---

Se você é um agente de IA (como Google Gemini/Antigravity) gerando código para este pacote, **DEVE** seguir estritamente estas diretrizes:

1.  **Type Hints Estritos:** Todo método deve ter anotação de tipos nos argumentos e no retorno. Use `Optional`, `List`, `Dict` do módulo `typing` ou as sintaxes nativas do Python 3.10+.
2.  **PEP 8 & PEP 257:** Siga o guia de estilo oficial. Docstrings são obrigatórias em **Português do Brasil** para todas as classes e funções públicas.
3.  **Abstração do SQL:** Evite concatenação de strings para montar queries. Use sempre `sqlalchemy.text()` com *bind parameters* (ex: `:codpes`) para evitar *SQL Injection*.
4.  **Tratamento de Strings:** Ao lidar com Sybase legado, sempre aplique `.strip()` nos campos de texto retornados, pois o banco preenche colunas `CHAR` com espaços em branco.
5.  **Docs as Code:** Se criar um novo módulo (ex: `Graduacao.py`), atualize a documentação de referência na pasta `/docs`.