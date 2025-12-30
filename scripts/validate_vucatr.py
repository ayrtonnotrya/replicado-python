import os
import sys

from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.getcwd())

load_dotenv()

from replicado import CartaoUSP, Pessoa


def verify_vucatr() -> None:
    # Test codpes (needs to be a real valid codpes for full verification)
    # Using a dummy or trying to find one from existing searches if possible
    # For safe testing without knowing a specific number, we try to fetch a known person or list from DB

    print("--- Verifying CartaoUSP ---")

    # Try to find someone with an active card
    try:
        from replicado.connection import DB

        # Find a random person with active card
        query = "SELECT TOP 1 codpescra FROM CATR_CRACHA WHERE sitpescra = 'A'"
        result = DB.fetch(query)
        if result:
            codpes = int(result["codpescra"])
            print(f"Testing with codpes: {codpes}")

            acesso = CartaoUSP.verificar_acesso(codpes)
            print(f"CartaoUSP.verificar_acesso({codpes}): {acesso}")

            cracha = CartaoUSP.buscar_cracha_ativo(codpes)
            print(f"CartaoUSP.buscar_cracha_ativo({codpes}): {cracha}")

            solicitacoes = CartaoUSP.listar_solicitacoes(codpes)
            print(
                f"CartaoUSP.listar_solicitacoes({codpes}): Found {len(solicitacoes)} requests"
            )
        else:
            print("No active card found in DB to test.")

    except Exception as e:
        print(f"Error testing CartaoUSP: {e}")

    print("\n--- Verifying Pessoa.obter_situacao_vacinal ---")
    try:
        # Find someone with vaccine info
        query = "SELECT TOP 1 codpes FROM PESSOAINFOVACINACOVID"
        result = DB.fetch(query)
        if result:
            codpes = int(result["codpes"])
            print(f"Testing with codpes: {codpes}")
            vacina = Pessoa.obter_situacao_vacinal(codpes)
            print(f"Pessoa.obter_situacao_vacinal({codpes}): {vacina}")
        else:
            print("No vaccination info found in DB.")

    except Exception as e:
        print(f"Error testing Pessoa vaccination: {e}")


if __name__ == "__main__":
    verify_vucatr()
