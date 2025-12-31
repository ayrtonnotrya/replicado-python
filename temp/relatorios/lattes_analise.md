# Análise e Proposta: Módulo Lattes

Este relatório apresenta o diagnóstico do esquema e a proposta de novos métodos para o módulo `lattes` do pacote Replicado-USP.

## Diagnóstico de Replicação (Inconsistências de Esquema)

As tabelas a seguir constam na documentação de visão (`vulatt`, `vuprod`, `vupesq`), mas não foram encontradas ou estão inacessíveis na réplica local durante o teste de verificação:

| Tabela | Status | Campos Testados |
| :--- | :--- | :--- |
| `CITACAOPESSOA` | ❌ Faltante | `codpes`, `idcfteifm`, `idfpesfte`, `qtdpdupes`, `clsinh` |
| `CITACAOPESSOAANUAL` | ❌ Faltante | `codpes`, `idcfteifm`, `anoref`, `qtdcitpes` |
| `QUALISPERIODICO` | ❌ Faltante | `anobseavl`, `numisnprd`, `titprd`, `clsqliprd` |
| `ACIPROJETO` | ❌ Faltante | `codprj`, `anoprj`, `titprjaco`, `sitprj` |
| `PDPROJETO` | ❌ Faltante | `codprj`, `anoprj`, `titprj`, `sitprj` |
| `ICTPROJETO` | ❌ Faltante | `codprj`, `anoprj`, `titprj`, `sitprj` |
| `AREACONHECIMENTOCNPQ` | ❌ Faltante | `codare`, `nomare` |
| `PDPROGRAMA` | ❌ Faltante | `codpgm`, `nompgm` |

**Nota:** Apenas a tabela `DIM_PESSOA_XMLUSP` foi verificada com sucesso.

---

## Métodos Propostos

Dada a riqueza de informações contidas no XML do Lattes e a existência de tabelas de suporte na visão (ainda que pendentes de replicação), propõe-se a implementação dos seguintes métodos:

### 1. Produção Científica e Qualis

| Método | Argumentos | Retorno | Justificativa |
| :--- | :--- | :--- | :--- |
| `listar_artigos_com_qualis` | `codpes: int` | `list[dict]` | Enriquece a listagem de artigos extraída do XML com a classificação Qualis (A1, A2, etc.) da tabela `QUALISPERIODICO`. |
| `contar_producao_por_estrato` | `codpes: int, ano_ini: int, ano_fim: int` | `dict[str, int]` | Contagem de artigos publicados por estrato Qualis em um determinado período. |

### 2. Métricas de Citação

| Método | Argumentos | Retorno | Justificativa |
| :--- | :--- | :--- | :--- |
| `obter_metricas_citacao` | `codpes: int` | `dict` | Retorna o índice H e total de citações (Scopus, Scholar, WoS) conforme registrado na tabela `CITACAOPESSOA`. |
| `listar_citacoes_anual` | `codpes: int, fonte: str` | `list[dict]` | Retorna o histórico anual de citações de uma fonte específica usando `CITACAOPESSOAANUAL`. |

### 3. Projetos de Pesquisa e Pós-Doutorado

| Método | Argumentos | Retorno | Justificativa |
| :--- | :--- | :--- | :--- |
| `listar_projetos_pesquisa` | `codpes: int` | `list[dict]` | Centraliza a listagem de projetos de pesquisa (IC, PD, Externos) usando tabelas como `ACIPROJETO` e `PDPROJETO`. |
| `obter_detalhes_pos_doutorado` | `codpes: int` | `dict` | Fornece dados detalhados sobre o programa de Pós-Doutorado (supervisor, financiamento) via `PDPROJETO`. |

### 4. Estrutra e Categorização

| Método | Argumentos | Retorno | Justificativa |
| :--- | :--- | :--- | :--- |
| `listar_areas_conhecimento` | `codpes: int` | `list[str]` | Mapeia os códigos de área do XML para nomes amigáveis usando a tabela `AREACONHECIMENTOCNPQ`. |
| `retornar_genero_pesquisador` | `codpes: int` | `str` | Retorna o gênero (M/F) conforme extraído da tabela `PESSOA` (usada como suporte em `vuprod`). |

---

### Observações sobre Proporcionalidade
Embora o módulo `lattes` dependa fortemente do XML, a inclusão de métodos baseados em tabelas relacionais (`vuprod`, `vupesq`) permite que os sistemas USP consultem dados agregados e métricas de impacto sem a necessidade de reprocessar o XML em cada requisição, aumentando significativamente a performance e utilidade do pacote.

---

## Impacto na Implementação (Atualização 31/12/2025)

Após investigação aprofundada e implementação, conclui-se que:

1.  **Adoção do XML como Fonte Única**: Devido à ausência total das tabelas `CITACAOPESSOA`, `ACIPROJETO` e `PDPROJETO`, a refatoração priorizou a extração de dados diretamente do XML (`DIM_PESSOA_XMLUSP`), utilizando um cache em memória (TTL 1h) para mitigar problemas de performance.
2.  **Impossibilidade de Histórico Detalhado**:
    *   A tabela `CITACAOPESSOAANUAL` (prevista na visão) forneceria o histórico de citações ano a ano.
    *   Como ela não existe, tentou-se extrair essa informação do XML.
    *   **Limitação**: O XML padrão do Lattes (CNPq) contido na réplica *não possui* dados estruturados de citações por ano, apenas um resumo em texto ou totais gerais em atributos específicos (quando preenchidos manualmente).
    *   **Consequência**: O método `listar_citacoes_anual` foi mantido para compatibilidade, mas retorna uma lista vazia, pois a informação não existe na fonte de dados disponível.
3.  **Ação Recomendada**: Comunicar aos responsáveis pela replicação a necessidade de disponibilizar as tabelas de métricas (`CITACAOPESSOA*`) ou integrar uma fonte externa (ex: Scopus API), já que o XML sozinho é insuficiente para análises bibliométricas temporais.
