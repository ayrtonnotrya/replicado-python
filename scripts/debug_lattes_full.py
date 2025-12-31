
import json
import os
from replicado.lattes import Lattes

# Configurações
CODPES = 6901698
OUTPUT_FILE = f"temp/lattes_{CODPES}_full.json"

print(f"--- Investigando Lattes para Codpes: {CODPES} ---")

# 1. Baixar e Decodificar
print("1. Obtendo dados brutos...")
lattes_dict = Lattes.obter_array(CODPES)

if not lattes_dict:
    print("ERRO: Não foi possível obter o array do Lattes.")
    exit(1)

# 2. Salvar Full JSON
print(f"2. Salvando dump em {OUTPUT_FILE}...")
os.makedirs("temp", exist_ok=True)
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(lattes_dict, f, indent=2, ensure_ascii=False)

print("Dump concluído.")
print("Procurando por chaves relacionadas a citacoes...")

def recursive_search(d, term, path=""):
    if isinstance(d, dict):
        for k, v in d.items():
            current_path = f"{path}.{k}" if path else k
            if term.lower() in k.lower():
                print(f"FOUND KEY: {current_path} = {v}")
            recursive_search(v, term, current_path)
    elif isinstance(d, list):
        for i, item in enumerate(d):
            recursive_search(item, term, f"{path}[{i}]")

recursive_search(lattes_dict, "CITACO")
print("--- Fim da Busca ---")
