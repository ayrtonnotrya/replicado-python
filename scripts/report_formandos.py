import csv
import os
import sys

# Adiciona o diretório raiz ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
from replicado.graduacao import Graduacao
from replicado.connection import DB

def main():
    load_dotenv()
    
    input_file = "temp/formandos_raw.txt"
    output_file = "formandos_medias.csv"
    
    print(f"Lendo dados de {input_file}...")
    
    alunos = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            # Pula linhas de cabeçalho ou títulos de seção
            if "n. USP" in line or "Alunos" in line or "E-mail" in line:
                continue
                
            parts = line.split('\t')
            # Alguns podem não ter tabs se for copy-paste mal formatado, 
            # mas vamos assumir tab primeiro baseado no visual.
            # Se falhar, tentamos heurística.
            
            if len(parts) < 2:
                # Tenta splitar por espaços duplos ou algo assim se não for tab
                # Mas o visual do prompt parece tab.
                # Se for apenas espaços, o nome pdoe ter espaços.
                # Regex heuristic: Nome (texto) espaço CodPes (número) espaço ...
                import re
                match = re.search(r'([^\t]+)\t(\d+)\t([^\t]+)\t(.*)', line)
                if match:
                    nome = match.group(1).strip()
                    codpes = match.group(2).strip()
                    curso = match.group(3).strip()
                else:
                    # Fallback para regex de espaço se não for tab
                    match_space = re.search(r'^(.*?)\s+(\d{6,})\s+(\w+)\s+(.*?)$', line)
                    if match_space:
                        nome = match_space.group(1).strip()
                        codpes = match_space.group(2).strip()
                        curso = match_space.group(3).strip()
                    else:
                        print(f"Ignorando linha (formato desconhecido): {line}")
                        continue
            else:
                 nome = parts[0].strip()
                 codpes = parts[1].strip()
                 curso = parts[2].strip() if len(parts) > 2 else ""

            if codpes.isdigit():
                alunos.append({
                    "codpes": int(codpes),
                    "nome": nome,
                    "curso": curso
                })

    print(f"Processando {len(alunos)} alunos...")
    
    results = []
    for aluno in alunos:
        codpes = aluno['codpes']
        try:
            # Obtém média suja
            media = Graduacao.obter_media_ponderada_suja(codpes)
            aluno['media_suja'] = media
            print(f"✅ {aluno['nome']} ({codpes}): {media}")
            results.append(aluno)
        except Exception as e:
            print(f"❌ Erro para {codpes}: {e}")
            aluno['media_suja'] = "ERRO"
            results.append(aluno)

    # Escreve CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['codpes', 'nome', 'curso', 'media_suja']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for data in results:
            writer.writerow(data)
            
    print(f"\nRelatório salvo em {output_file}")

if __name__ == "__main__":
    main()
