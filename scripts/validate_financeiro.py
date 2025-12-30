import os
import sys

from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.getcwd())

from replicado import Financeiro
from replicado.connection import DB

load_dotenv()


def validate_financeiro() -> None:
    print("--- Iniciando Validação: Módulo Financeiro ---")

    # 1. listar_centros_despesas
    print("\n1. listar_centros_despesas()")
    centros = Financeiro.listar_centros_despesas()
    print(f"   Encontrados: {len(centros)}")
    if centros:
        print(f"   Exemplo: {centros[0]}")

    # Encontrar dados para testes
    try:
        # Pega uma unidade de despesa ativa
        query_und = (
            "SELECT TOP 1 codunddsp FROM CENTRODESPHIERARQUIA WHERE dtadtv IS NULL"
        )
        res_und = DB.fetch(query_und)
        codunddsp = int(res_und["codunddsp"]) if res_und else 45

        # Pega um bem patrimoniado ativo
        query_pat = (
            "SELECT TOP 1 numpat, codlocusp, codunddsp, codpes, codbem "
            "FROM BEMPATRIMONIADO WHERE stabem = 'Ativo'"
        )
        res_pat = DB.fetch(query_pat)
        numpat = str(res_pat["numpat"]) if res_pat else None
        codlocusp = (
            int(res_pat["codlocusp"]) if res_pat and res_pat["codlocusp"] else None
        )
        codpes = int(res_pat["codpes"]) if res_pat and "codpes" in res_pat else None

        # 2. listar_estoque_unidade
        print(f"\n2. listar_estoque_unidade({codunddsp})")
        estoque = Financeiro.listar_estoque_unidade(codunddsp)
        print(f"   Itens em estoque: {len(estoque)}")

        # 3. sugerir_reposicao
        print(f"\n3. sugerir_reposicao({codunddsp})")
        reposicao = Financeiro.sugerir_reposicao(codunddsp)
        print(f"   Itens para reposição: {len(reposicao)}")

        if res_pat:
            codbem = int(res_pat["codbem"]) if "codbem" in res_pat else None
            if not codbem:
                res_bem = DB.fetch(
                    f"SELECT codbem FROM BEMPATRIMONIADO WHERE numpat = '{numpat}'"
                )
                codbem = int(res_bem["codbem"]) if res_bem else None

            # 4. obter_preco_medio
            if codbem:
                print(f"\n4. obter_preco_medio({codbem})")
                preco = Financeiro.obter_preco_medio(codbem)
                print(f"   Preço médio: {preco}")

            # 5. listar_bens_por_responsavel
            if codpes:
                print(f"\n5. listar_bens_por_responsavel({codpes})")
                bens_resp = Financeiro.listar_bens_por_responsavel(codpes)
                print(f"   Bens encontrados: {len(bens_resp)}")

            # 6. obter_detalhes_bem
            if numpat:
                print(f"\n6. obter_detalhes_bem('{numpat}')")
                detalhes = Financeiro.obter_detalhes_bem(numpat)
                print(f"   Detalhes: {detalhes}")

            # 7. listar_bens_por_local
            if codlocusp:
                print(f"\n7. listar_bens_por_local({codlocusp})")
                bens_local = Financeiro.listar_bens_por_local(codlocusp)
                print(f"   Bens no local: {len(bens_local)}")

        # 8. listar_doacoes_recebidas
        print(f"\n8. listar_doacoes_recebidas({codunddsp})")
        doacoes = Financeiro.listar_doacoes_recebidas(codunddsp)
        print(f"   Doações: {len(doacoes)}")

        # 9. contar_bens_por_status
        print(f"\n9. contar_bens_por_status({codunddsp})")
        stats = Financeiro.contar_bens_por_status(codunddsp)
        print(f"   Estatísticas: {stats}")

        # 10. obter_hierarquia_financeira
        print(f"\n10. obter_hierarquia_financeira({codunddsp})")
        hierarquia = Financeiro.obter_hierarquia_financeira(codunddsp)
        print(f"   Níveis hierárquicos: {len(hierarquia)}")

        # 11. buscar_local_usp
        print("\n11. buscar_local_usp('SALA')")
        locais = Financeiro.buscar_local_usp("SALA")
        print(f"   Locais encontrados: {len(locais)}")

        # 12. listar_convenios_financeiros
        print(f"\n12. listar_convenios_financeiros({codunddsp})")
        convenios = Financeiro.listar_convenios_financeiros(codunddsp)
        print(f"   Convênios: {len(convenios)}")

        if convenios:
            codcvn = convenios[0]["codcvn"]
            # 13. listar_organizacoes_convenio
            print(f"\n13. listar_organizacoes_convenio({codcvn})")
            orgs = Financeiro.listar_organizacoes_convenio(codcvn)
            print(f"   Organizações: {len(orgs)}")

            if orgs and orgs[0]["idfcgcorg"]:
                cnpj = str(int(orgs[0]["idfcgcorg"]))
                # 14. buscar_organizacao_por_cnpj
                print(f"\n14. buscar_organizacao_por_cnpj('{cnpj}')")
                org_cnpj = Financeiro.buscar_organizacao_por_cnpj(cnpj)
                print(
                    f"   Org encontrada: {org_cnpj['nomrazsoc'] if org_cnpj else 'Não'}"
                )

        # 15. detalhar_item_material
        res_itm = DB.fetch("SELECT TOP 1 coditmmat FROM CLASSIFITEMMAT")
        if res_itm:
            coditm = int(res_itm["coditmmat"])
            print(f"\n15. detalhar_item_material({coditm})")
            item = Financeiro.detalhar_item_material(coditm)
            print(f"   Item: {item}")

        # 16. listar_atributos_material
        if res_pat and "codbem" in res_pat:
            codbem = int(res_pat["codbem"])
            print(f"\n16. listar_atributos_material({codbem})")
            attrs = Financeiro.listar_atributos_material(codbem)
            print(f"   Atributos encontrados: {len(attrs)}")

    except Exception as e:
        print(f"\n[ERRO DURANTE VALIDAÇÃO] {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    validate_financeiro()
