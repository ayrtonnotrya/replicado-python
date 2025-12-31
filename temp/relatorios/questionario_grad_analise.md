# Relatório de Proposta e Diagnóstico de Esquema - Módulo: Questionário Grad

Este relatório apresenta a análise das tabelas relacionadas ao módulo de Questionário da Graduação, baseando-se na visão de replicação `VUques`, e o diagnóstico de sua existência na réplica local.

## 1. Métodos Propostos

Com base na estrutura de questionários, questões, alternativas e respostas, propomos a implementação dos seguintes métodos no novo módulo `replicado/questionario_grad.py`:

| Método | Argumentos | Retorno | Justificativa |
| :--- | :--- | :--- | :--- |
| `listar_questionarios` | `staati: Optional[str] = None` | `list[dict[str, Any]]` | Lista os questionários disponíveis, permitindo filtrar por status (Ex: Ativos). |
| `obter_questionario` | `codqtn: int` | `Optional[dict[str, Any]]` | Retorna os detalhes de um questionário específico. |
| `listar_questoes` | `codqtn: int` | `list[dict[str, Any]]` | Lista todas as perguntas associadas a um questionário. |
| `listar_alternativas` | `codqtn: int, codqst: int` | `list[dict[str, Any]]` | Lista as opções de resposta para uma determinada questão. |
| `listar_respostas_pessoa` | `codpes: int, codqtn: Optional[int] = None` | `list[dict[str, Any]]` | Recupera as respostas fornecidas por uma pessoa, opcionalmente filtrada por questionário. |
| `obter_resposta_pessoa_questao` | `codpes: int, codqtn: int, codqst: int` | `Optional[dict[str, Any]]` | Retorna a resposta de uma pessoa para uma pergunta específica. |
| `listar_alunos_que_responderam` | `codqtn: int` | `list[dict[str, Any]]` | Lista os identificadores (codpes) dos alunos que participaram de uma pesquisa. |
| `contar_respostas_per_alternativa` | `codqtn: int, codqst: int` | `list[dict[str, Any]]` | Gera estatísticas de quantas vezes cada alternativa foi escolhida em uma questão. |
| `obter_pontuacao_socio_economica` | `codpes: int, codqtn: int` | `float` | Calcula a soma de `qtdptosoceco` das alternativas escolhidas para definir o perfil sócio-econômico. |

## 2. Diagnóstico de Replicação (Inconsistências)

O script de verificação `scripts/verify_schema_questionario_grad.py` identificou que as tabelas específicas deste módulo **NÃO ESTÃO PRESENTES** na réplica local utilizada.

### Tabelas/Campos Não Localizados:

*   **Tabela `QUESTIONARIO`**: Não encontrada na réplica.
*   **Tabela `QUESTOESPESQUISA`**: Não encontrada na réplica.
*   **Tabela `ALTERNATIVAQUESTAO`**: Não encontrada na réplica.
*   **Tabela `RESPOSTASQUESTAO`**: Não encontrada na réplica.

> [!WARNING]
> A ausência destas tabelas impede a implementação imediata dos métodos. Recomenda-se solicitar a inclusão destas na rotina de replicação junto ao responsável pelo banco de dados.

### Campos Verificados (Todos Inexistentes Localmente):
- `QUESTIONARIO`: `codqtn`, `nompsq`, `dscpsq`, `staati`, `dtainiqtn`, `dtafimqtn`, `dtacad`, `codpescad`
- `QUESTOESPESQUISA`: `codqtn`, `codqst`, `dscqst`, `eplqst`, `stamlpsel`, `dscqstigl`, `dscqstepa`, `dscqstfcs`
- `ALTERNATIVAQUESTAO`: `codqtn`, `codqst`, `numatnqst`, `dscatn`, `statxtcpl`, `qtdptosoceco`, `dscatnigl`, `dscatnepa`, `dscatnfcs`
- `RESPOSTASQUESTAO`: `codqtn`, `codqst`, `numatnqst`, `codpes`, `numseqsrv`, `rpaatn`, `txtcplrpa`, `dtarpa`

## 3. Considerações Técnicas
- As tabelas utilizam campos `CHAR` que exigirão `.strip()` conforme as regras básicas do projeto.
- A tabela `PESSOA` (mencionada na visão) é global e já está implementada no módulo `pessoa.py`.
