import os
import sys

from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from replicado.estrutura import Estrutura


def test_estrutura():
    load_dotenv()
    print("=== Verificando Módulo Estrutura ===")

    # 1. Listar Unidades
    print("\n1. Listando Unidades...")
    unidades = Estrutura.listar_unidades()
    print(f"Total de unidades encontradas: {len(unidades)}")

    if not unidades:
        print("Nenhuma unidade encontrada. Abortando.")
        return

    # Pick a random unit or first one (preferable one with data)
    # Let's try to find 'Faculdade de Filosofia, Letras e Ciências Humanas' or similar broad unit,
    # or just pick the first valid one.
    target_und = unidades[0]
    codund = target_und["codund"]
    nomund = target_und["nomund"]
    print(f"--> Usando Unidade para Testes: {codund} - {nomund}")

    # 2. Dados Fiscais
    print(f"\n2. Obtendo Dados Fiscais (codund={codund})...")
    fiscais = Estrutura.obter_dados_fiscais(codund)
    if fiscais:
        print(f"[OK] Dados Fiscais: {fiscais}")
    else:
        print(
            "[WARN] Dados fiscais não encontrados (pode ser normal para algumas unidades)"
        )

    # 3. Chefias
    print(f"\n3. Listando Chefias (codund={codund})...")
    chefias = Estrutura.listar_chefias_unidade(codund)
    print(f"Total de chefias encontradas: {len(chefias)}")
    if chefias:
        for c in chefias[:3]:
            print(f"   - {c['nomfnc']}: {c['nompes']}")

    # 4. Colegiados
    print(f"\n4. Listando Colegiados (codund={codund})...")
    colegiados = Estrutura.listar_colegiados(codund)
    print(f"Total de colegiados encontrados: {len(colegiados)}")
    if colegiados:
        for c in colegiados[:5]:
            print(f"   - [{c['sglclg']}] {c['nomclg']}")

    # 5. Departamentos
    print(f"\n5. Listando Departamentos (codund={codund})...")
    depts = Estrutura.listar_departamentos(codund)
    print(f"Total de departamentos encontrados: {len(depts)}")

    target_set = None
    if depts:
        for d in depts[:3]:
            print(f"   - {d['nomset']} (Tel: {d['numtelref']})")
        target_set = depts[0]

    # 6. Setor (Contato e Servidores)
    if not target_set and len(unidades) > 0:
        # Fallback: list sectors if no departments
        setores = Estrutura.listar_setores(codund)
        if setores:
            target_set = setores[0]

    if target_set:
        codset = target_set["codset"]
        nomset = target_set["nomset"]
        print(f"\n--> Usando Setor para Testes: {codset} - {nomset}")

        # Contato
        print("6. Obtendo contato do setor...")
        contato = Estrutura.obter_contato_setor(codset)
        print(f"   Contato: {contato}")

        # Servidores
        print("7. Listando servidores do setor...")
        servidores = Estrutura.listar_servidores_setor(codset)
        print(f"   Total de servidores: {len(servidores)}")
        if servidores:
            for s in servidores[:3]:
                print(f"   - {s['nompes']} ({s['nomfnc']})")
    else:
        print("\n[WARN] Nenhum setor encontrado para testes detalhados.")


if __name__ == "__main__":
    test_estrutura()
