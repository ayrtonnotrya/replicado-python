
import json
import zlib
from replicado.connection import DB
from replicado.lattes import Lattes

# Configurações
CODPES = 6901698

print(f"--- Investigando Lattes para Codpes: {CODPES} ---")

# 1. Baixar e Decodificar RAW
print("1. Obtendo dados brutos...")
lattes_dict = Lattes.obter_array(CODPES)

if not lattes_dict:
    print("ERRO: Não foi possível obter o array do Lattes.")
    exit(1)

# 2. Inspecionar Chaves Principais
print(f"2. Chaves Raiz: {list(lattes_dict.keys())}")

# 3. Investigar Citações
print("\n3. Investigando nó 'CITACOES':")
citacoes = lattes_dict.get('CITACOES', [])
if isinstance(citacoes, dict):
    citacoes = [citacoes]

for i, c in enumerate(citacoes):
    print(f"  Item {i}: {c}")


# 4. Investigar Produção para ver se tem algo relacionado
print("\n4. Investigando estrutura geral (chaves de primeiro nível):")
# Vamos salvar um JSON de exemplo para o user ver a estrutura, se precisar
# mas por agora, vamos printar chaves de segundo nível
for k, v in lattes_dict.items():
    if isinstance(v, dict):
        print(f"  [{k}]: {list(v.keys())}")
    elif isinstance(v, list):
        print(f"  [{k}]: (Lista com {len(v)} itens)")
    else:
        print(f"  [{k}]: (Valor direto)")

print("\n--- Fim da Análise ---")
