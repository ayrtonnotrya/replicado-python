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

### Alunos ativos no Dia D (pipeline Micro Targeting — `dataset_aluno.py`)

- **Regra (corte temporal point-in-time)**: um aluno é "ativo no Dia D"
  sse, no snapshot de `HISTPROGGR` (último `stapgm` por `(codpes, codpgm)`
  com `dtaoco <= dta_corte`), o status **NÃO** é um status morto
  (`{E, T, S}` → `MORTO_STAPGM`). Lógica por **exclusão**, não inclusão: a
  HISTPROGGR é um log de eventos, e o último registro de quase todo
  veterano é `H` (Histórico/Habilitação) ou `EH` (encerramento de
  habilitação anterior ao trocar de ênfase) — o programa **continua
  ativo**. Exigir `stapgm ∈ {A,R}` (versão rejeitada) deletava os
  veteranos reais e desabava y=1 de ~1.500 para ~500/semestre. Precedente:
  `replicado.graduacao` consulta `stapgm IN ('A','H','R')` para
  "programas vivos". Piso `dtaclcgru` (colação) ≥ Dia D permanece como
  backstop; alunos sem HISTPROGGR ≤ Dia D (cache parcial) ficam pelo piso.
- **Bug histórico corrigido** (`_alunos_ativos`): a condicional anterior
  `(dtaclcgru isna | dtaclcgru > dta_corte)` IGNORAVA evasão
  (jubilamento/encerramento sem colação), retendo ~15 anos de evadidos da
  **1ª graduação** como ativos — origem do sintoma "~85% de alunos com 0
  matrículas reais". Diagnóstico: `scripts/diagnostico_alunos_ativos.py`
  (roda sobre cache, sem túnel): 2018.1 removeu 2.120 evadidos/trancados
  (E/T); 2022.2 análogo. Todos os fantasmas são `codpgm == 1` com `stapgm
  ∈ {E,T}`. y=1 cai só ~1% (1.465→1.452 em 2018.1) — veteranos com `H`
  preservados.
- **`codpgm` é NÚMERO de (re)ingresso, não tipo de programa**: 1=1º
  ingresso, 2=reingresso/2ª graduação, etc. Alunos `codpgm >= 2` são
  LEGITIMAMENTE ativos no Dia D (reentradas) e **não** devem ser
  filtrados. O cache de `habilprog` já é restrito à graduação da unidade
  via `INNER JOIN CURSOGR WHERE codclg = {cod}` (CURSOGR = graduação; pós
  vive em outra família de tabelas).
- **Cache de `histprog_unidade`/`req_unidade`/`requer` deve incluir TODOS
  os `codpgm`**: as queries de extração em `dataset_macrosensores.py` e
  `dataset_aluno.py` **não** filtram `codpgm = 1` (filtro removido) — o
  `INNER JOIN CURSOGR` já assegura graduação da unidade. Limitar
  `codpgm = 1` no SQL quebrava o PIT dos reingressos (codpgm>=2 sumiam do
  cache e caíam no fallback `isna`). **Exige re-extração do cache** (os
  pickles atuais só têm codpgm=1): após alterar o SQL, rode
  `build_dataset_aluno.py --forcar-extracao` com o túnel SSH ativo.
- **Negativos legítimos ≠ leakage**: alunos ativos no Dia D que não se
  matriculam no semestre são `y=0` válidos (negativo que o modelo aprende
  a prever), não "fantasmas" a corrigir. O leakage corrigido é só a
  população (1) evadidos rotulados de ativos; a (2) ativos-sem-matrícula
  é label bom.

### Flags exploratórias do dataset de aluno (research, defaults OFF)

Implementadas para análise de impacto **sem destruir o baseline**; defaults
`False` mantêm `temp/dataset_aluno.csv` bit-idêntico ao anterior. A saída
sempre recebe sufixo curto conforme a combinação de flags (`_sf`, `_bl`,
`_sf_bl`) via `_saida_com_flags` — nunca sobrescreve o baseline.

- **`excluir_fantasmas`** (`--excluir-fantasmas`, env
  `REPLICADO_ALUNO_EXCLUIR_FANTASMAS`, sufixo `_sf`): remove dos **negativos**
  (y=0) os alunos ativos com **zero matrículas no semestre**
  (`codpes_matriculados = set(pos["codpes"])`; `pos` fica intacto → y=1
  preservado). É **selection-on-outcome intencional**: no Dia D não se sabe
  ainda quem terá zero matrículas, então o dataset `_sf` NÃO deve ser usado
  como "limpo" em produção — serve para medir o impacto de restringir a
  população a matriculados (objetivo condicional vs populacional).
- **`balancear_l`** (`--balancear-l`, env `REPLICADO_ALUNO_BALANCEAR_L`,
  sufixo `_bl`): amostra negativos L (livres) até `pos_L/neg_L` atingir a
  razão média de O e E/C. Decisões de design (ver histórico):
  - **Pool** = alunos com ≥1 matrícula no semestre (qualquer tipo) × coddis
    ofertadas **fora do currículo elegível** do aluno (não em `necess`), não
    aprovadas, ainda não em `pos`/`neg` (incl. REQUERIMENTOGR).
  - **Razão-alvo** = média das razões `pos/neg` de O e E/C, calculada **sobre
    matriculados** (mesmo universo do pool) e **por semestre**.
  - **Volume** controlado pelo alvo; `max_neg_turmas_por_disc` NÃO se aplica
    a L (decisão 5.B).
  - Amostragem com `random_state` derivado de `(codundclg, sem_alvo)` →
    reprodutível.
- **Bug latente corrigido no `from_env`**: o padrão antigo
  `env_bool(os.getenv("X") or "")` passava o **valor** como `name`
  (virava `os.getenv("1")`) e retornava sempre `None` — as flags por env
  nunca funcionavam (incluindo `REPLICADO_ALUNO_USAR_INTENCAO`). Agora usa
  `env_bool("REPLICADO_ALUNO_EXCLUIR_FANTASMAS")` diretamente.
- Validação (sem túnel, sobre cache, semestre 20181): y=1 preservado
  (4.817 em todas as combinações); sem linhas duplicadas; `sf` removeu
  O-neg 117.225→15.977 e E/C-neg 53.849→13.083; `bl` levou L-neg a `round(
  pos_L / razão média O/E/C)`.

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
