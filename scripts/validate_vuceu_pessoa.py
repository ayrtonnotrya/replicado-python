
import os
import sys
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.getcwd())

load_dotenv()

from replicado import Pessoa
from replicado.connection import DB

def verify_vuceu_pessoa():
    print("--- Verifying Pessoa.listar_aex ---")
    try:
        # Find someone with AEX active/completed
        query = "SELECT TOP 1 codpes FROM AEXINSCRICAO"
        result = DB.fetch(query)
        if result:
            codpes = int(result['codpes'])
            print(f"Testing AEX with codpes: {codpes}")
            aex_list = Pessoa.listar_aex(codpes)
            print(f"Found {len(aex_list)} AEX activities.")
            if len(aex_list) > 0:
                print(f"Sample: {aex_list[0]}")
        else:
            print("No AEX inscriptions found in DB to test.")
    except Exception as e:
        print(f"Error testing AEX: {e}")

    print("\n--- Verifying Pessoa.listar_cursos_extensao ---")
    try:
        # Find someone with CEU courses
        query = "SELECT TOP 1 codpes FROM MATRICULACURSOCEU"
        result = DB.fetch(query)
        if result:
            codpes = int(result['codpes'])
            print(f"Testing CEU Courses with codpes: {codpes}")
            courses = Pessoa.listar_cursos_extensao(codpes)
            print(f"Found {len(courses)} CEU courses.")
            if len(courses) > 0:
                print(f"Sample: {courses[0]}")
        else:
            print("No CEU matriculations found in DB to test.")
    except Exception as e:
        print(f"Error testing CEU Courses: {e}")

if __name__ == "__main__":
    verify_vuceu_pessoa()
