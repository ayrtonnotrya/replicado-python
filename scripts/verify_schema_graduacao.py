import os
import sys

from dotenv import load_dotenv

# Adiciona a raiz do projeto ao python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from replicado.connection import DB


def verify_graduacao_tables() -> None:
    load_dotenv()

    # Tabelas mapeadas de vugr e vuques
    tables_to_check = [
        "ALUNOGR",
        "ALUNOVINCULOUSP",
        "BOLSISTA",
        "CAMPUS",
        "CARGAHORARIA",
        "CICLO",
        "COLEGIADO",
        "CONVENIO",
        "CREDITO",
        "CURSOGR",
        "CURRICULOGR",
        "CURSOHABILITACAOGR",
        "CURSOPROGRAMA",
        "DETTURMAGR",
        "DISCIPEXTGR",
        "DISCIPLINAGR",
        "DOCENTE",
        "EQUIVALENCIAGR",
        "EQUIVEXTERNAGR",
        "ESTRUTVINCHABILIT",
        "ESTRUTVINCTURMAGR",
        "GRADECURRICULAR",
        "GRUPOEQUIVGR",
        "GRUPOREQUISITO",
        "HABILDEMANDA",
        "HABILDEPEND",
        "HABILDURACAO",
        "HABILIDADEDOC",
        "HABILITACAOGR",
        "HABILITACAOGRCOLEG",
        "HABILITATUALWEB",
        "HABILPROGGR",
        "HABILTURMA",
        "HABILVAGA",
        "HISTCURSOGR",
        "HISTESCOLAREXTGR",
        "HISTESCOLARGR",
        "HISTPROGGR",
        "INSCRESPGR",
        "MINISTRANTE",
        "NORMARECONHECHABILGR",
        "NOTASINGRESSOGR",
        "OCUPTURMA",
        "OPCAOPROGRAMA",
        "PERIODOHORARIO",
        "PESSOA",
        "PREFIXODISCIP",
        "PROGRAMAGR",
        "REQUERHISTESC",
        "REQUERIMENTOGR",
        "REQUISITOGR",
        "TIPOATIVDIDATICA",
        "TIPOCONCEITOING",
        "TIPODATAGR",
        "TIPOENCERRAMENTOGR",
        "TIPOHABILITGR",
        "TIPOINGRESSOGR",
        "TIPOMATERIAING",
        "TIPOMOTIVOREQUERGR",
        "TIPOREQUERGR",
        "TIPOTURMAGR",
        "TURMAGR",
        "TURPRATICA",
        "ALTERNATIVAQUESTAO",
        "QUESTIONARIO",
        "QUESTOESPESQUISA",
        "RESPOSTASQUESTAO",
    ]

    print(f"Verificando {len(tables_to_check)} tabelas dos esquemas 'vugr' e 'vuques'...")

    existing_tables = []
    missing_tables = []

    for table in tables_to_check:
        try:
            # Tenta um SELECT TOP 1 para verificar se a tabela existe de fato
            query = f"SELECT TOP 1 1 FROM {table}"
            DB.fetch_all(query)
            existing_tables.append(table)
            print(f"[OK] Tabela '{table}' encontrada.")
        except Exception:
            missing_tables.append(table)
            print(f"[ERRO] Tabela '{table}' NÃO encontrada ou sem acesso.")

    print("\n" + "=" * 50)
    print("RESUMO DA VERIFICAÇÃO")
    print("=" * 50)
    print(f"Total de tabelas verificadas: {len(tables_to_check)}")
    print(f"Encontradas: {len(existing_tables)}")
    print(f"Faltantes/Erro: {len(missing_tables)}")

    if missing_tables:
        print("\nTabelas Faltantes:")
        for t in missing_tables:
            print(f"- {t}")


if __name__ == "__main__":
    verify_graduacao_tables()
