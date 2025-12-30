import os
import sys

from dotenv import load_dotenv

sys.path.append(os.getcwd())
load_dotenv()

from replicado import AEX, CEU
from replicado.connection import DB


def verify_aex_ceu() -> None:
    print("--- Verifying AEX Class ---")

    # 1. List Activities
    activities = AEX.listar_atividades()
    print(f"AEX.listar_atividades(): Found {len(activities)}")
    if activities:
        first_aex = activities[0]
        print(f"Sample: {first_aex}")
        codaex = first_aex["codaex"]

        # 2. Get Details
        print(f"Testing details for codaex={codaex}")
        details = AEX.buscar_por_codigo(codaex)
        print(f"AEX.buscar_por_codigo({codaex}): {'Found' if details else 'Not Found'}")

        # Test Filtering if we found an activity
        if details and "codclgaex" in details:
            codclg = details["codclgaex"]
            print(f"Testing filtering with codundclg={codclg}")
            fil = AEX.listar_atividades(codundclg=codclg)
            print(f"AEX.listar_atividades({codclg}): Found {len(fil)} (Should be > 0)")

        # 3. List Inscribed
        print(f"Testing inscritos for codaex={codaex}")
        inscritos = AEX.listar_inscritos(codaex)
        print(f"AEX.listar_inscritos({codaex}): Found {len(inscritos)}")

    print("\n--- Verifying CEU New Methods ---")

    # 1. List Active Courses
    active_courses = CEU.listar_cursos_ativos()
    print(f"CEU.listar_cursos_ativos(): Found {len(active_courses)}")

    if active_courses:
        sample_course = active_courses[0]
        print(f"Sample Active Course: {sample_course}")
    else:
        # Fallback to verify detalhes_curso with a known course if no active ones
        print(
            "No active courses found (expected depending on date). Checking arbitrary course with editions."
        )
        query = "SELECT TOP 1 codcurceu FROM EDICAOCURSOOFECEU"
        res = DB.fetch(query)
        if res:
            codcurceu = res["codcurceu"]
            print(f"Testing details for codcurceu={codcurceu}")
            det = CEU.detalhes_curso(codcurceu)
            print(f"CEU.detalhes_curso({codcurceu}): {'Found' if det else 'Not Found'}")


if __name__ == "__main__":
    verify_aex_ceu()
