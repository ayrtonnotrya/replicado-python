# AGENTS.md — replicado-python

Reimplementação Python da biblioteca uspdev/replicado (integração com as
réplicas Sybase/MSSQL da USP). Ver README.md para instalação.

## Conexão com o Replicado (operacional)

- O `.env` aponta para `127.0.0.1:1433`: depende de um **túnel SSH** ativo
  (`ssh -p <PORTA_SSH> -L 127.0.0.1:1433:<HOST_REPLICADO>:1433 <USUARIO>@<HOST_SSH>`).
  O túnel é instável — se der `connection timed out`, peça ao usuário para
  reiniciá-lo em vez de investigar o código.
- O acesso direto ao host do Replicado (sem túnel) **não** é alcançável
  desta máquina.
- Use sempre `DB.fetch_all(...)` / `DB.fetch(...)`. `DB.execute(...)` retorna
  um cursor cuja conexão fecha ao sair do `with` — iterar fora retorna vazio.
- Rode scripts com `poetry run python scripts/<script>.py`.

## Pipeline de dados (turmas/matrículas da graduação IME)

Ordem de execução:

1. `scripts/extrair_cache_replicado.py` — baixa TURMAGR completa e
   HISTESCOLARGR 2010-2026 (fatiada por ano) para `temp/cache_maquina_tempo/`.
   Idempotente; use `--forcar` para reextrair. **Todas as análises rodam
   sobre o cache, sem bater no banco.**
2. `scripts/maquina_tempo_estmtr.py` — reconstrói o `estmtr` histórico e
   valida contra `temp/turmas_internas.csv` (ground truth 2022-2026).
3. `scripts/alvo_pico_ocupacao.py` — constrói o alvo `T_pico` (pico de
   ocupação nas 3 primeiras semanas) para o modelo de alocação de salas.
4. `scripts/build_dataset.py` / `replicado.dataset_alocacao` — **fornecedor
   do dataset linha-por-turma** (features + alvos) pronto para treino.
   Agnóstico à unidade: escopo via `DatasetConfig` (`.env`
   `REPLICADO_CODUNDCLG`, `REPLICADO_PREFIXOS_DISC`, etc.) ou flags do CLI.
   Extrai tabelas auxiliares (GRADECURRICULAR, OCUPTURMA, PROGRAMAGR,
   HABILPROGGR, HABILITACAOGR, MINISTRANTE, DETTURMAGR, DISCIPLINAGR,
   DISCIPGRCODIGO, PERIODOHORARIO, CURSOGR) com streaming tqdm (COUNT +
   chunks via `DB.iter_chunks`) e cacheia em
   `temp/cache_maquina_tempo/aux_*_<cod>.pkl`.
   Gera 3 alvos: `nummtr` (soma das 5 vias), `pico_max` (T_pico) e `estmtr`
   (proxy/baseline).Features avançadas: espaço de fase (resíduo/volatilidade
   lagged), pressão de represamento, métricas topológicas (betweenness/
   PageRank do grafo de pré-requisitos), concorrência horária e sincronia de
   bloco. Sem vazamento: contadores consolidados (numins*, nummtr* cru,
   ocup_d+*) são descartados via `COLUNAS_VAZAMENTO`. Features de vagas do
   curso (`vagas_curso_<codcur>` e `vagas_curso_<codcur>_faltam`): só cursos
   ativos hoje recebem colunas; vagas reconstruídas de `HABILITACAOGR` por
   datas de vigência (`dtaatvhab`/`dtadtvhab` em 1/jul do ano da turma),
   somando `numvaghab+numvaghabcpl+numvaghabcvn` de todos `codhab` vigentes
   — dá continuidade ao passado mesmo onde `HABILVAGA` tem buracos
   (2014-2023). `_faltam = max(0, vagas_curso - estmtr)` sinaliza alunos
   ainda não inscritos no Dia D (sem vazar o alvo `delta`).

## Regras descobertas (engenharia reversa — NÃO re-derivar)

### estmtr (estimativa de matrículas do Júpiter)

- **Regra**: `estmtr = | HISTESCOLARGR.dtacrihst <= dtainitur - 5 dias |`,
  SEM filtro de `stamtr` (registros hoje excluídos estavam ativos no corte).
- Offset 4-5 dias cai na "janela morta" entre o fim da matrícula de veteranos
  e a carga dos calouros; offsets <= 3 capturam a carga (MAE explode no 1S).
- Calouros (FUVEST/SISU) são injetados por carga (`aplori='C'`) ~0-1 dias
  antes das aulas — por isso o estmtr não os conta (efeito temporal, não
  filtro do Júpiter).
- Validação: MAE 2.66, corr 0.980, 70% com |erro|<=2 (turmas sufixo >= 40).
- Piso irredutível: ~15% das turmas têm `estmtr` > total de registros
  existentes hoje (inscrições rejeitadas são fisicamente deletadas).
- Taxonomia completa dos outliers no docstring de `maquina_tempo_estmtr.py`.

### Alvo para alocação de salas: T_pico

- `T_pico = max ocupacao(D), D ∈ [dtainitur, dtainitur+21]`, onde
  `ocupacao(D) = criados até D − excluídos (stamtr E/R) com dtaultalt <= D`.
- O argmax modal é D+0 (turma nasce cheia); `nummtr` consolidado subestima o
  pico em >10% em ~19% das turmas → não usar como alvo.
- Trancamentos deixam rastro (`stamtr='E'`, `rstfim='T'`, `dtaultalt`) —
  reconstruível desde 2010 sem piso irredutível.

### Filtros de escopo obrigatórios

- **Disciplinas IME**: prefixos `45`, `MAC`, `MAE`, `MAT`, `MAP`, `MPM`.
  `43xxxxx` = Física (IFUSP) ministrada no IME — excluir (pedido do usuário).
  Cobertura HIST/TURMAGR = 1.00 nesses prefixos.
- **Sufixo do codtur >= 40**: turmas reais de oferecimento; sufixos menores
  foram inseridos manualmente no ground truth.
- **Anos 2010+**: TURMAGR vai a ~1980, mas HISTESCOLARGR local só 2010+.
- **Vazamento 2023-2024**: nesses anos a TURMAGR local tem turmas de TODA a
  USP (~18k em 2023) sem cobertura no HISTESCOLARGR — filtrar pelo escopo IME.
- **Semestre em andamento**: excluir das estatísticas (consolidação
  incompleta).

### Ground truth `temp/turmas_internas.csv`

- Contém linhas sem `estmtr` (turmas manuais), anotações com `*` no coddis,
  case misto e codtur fora do padrão — normalizações em
  `carregar_ground_truth()`.
- Valores de estmtr capturados em datas diferentes por semestre (alguns ~45
  dias antes das aulas): ruído documentado, não calibrar offset por semestre.

## Convenções

- Cache local: `temp/cache_maquina_tempo/` (pickles; NÃO usar parquet —
  pyarrow não é dependência). O glob `histescolar_*.pkl` pegaria também o
  `histescolar_gt.pkl` do script do estmtr — por isso o alvo carrega anos
  explicitamente.
- Saídas de análise: `temp/validacao_estmtr.csv`, `temp/alvo_pico_ocupacao.csv`,
  `temp/dataset_alocacao.csv` (dataset de treino).
- Config agnóstica à unidade: `DatasetConfig.from_env()` lê
  `REPLICADO_CODUNDCLG`, `REPLICADO_PREFIXOS_DISC` (CSV), `REPLICADO_SUFIXO_MIN`,
  `REPLICADO_ANO_MIN/MAX`, `REPLICADO_DIAS_CORTE`, `REPLICADO_PISO_VAGAS`,
  `REPLICADO_TOP_CURSOS`. Para outra unidade, defina no `.env` ou passe
  `--codundclg`/`--prefixos` no CLI.
- Deps novas do provider: `tqdm` (barra de progresso de extração) e
  `networkx` (grafo de pré-requisitos).
