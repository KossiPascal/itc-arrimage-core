import os
from utils.db import get_connection
from routes.run_sql_routes import start_execute_sql
from utils.config import config

def read_sql_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_sql_file(path: str, content: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def build_materialize_view(mat_view_file_name: str = None, mat_view_sql: str = None):
    """
    Charge une vue matérialisée depuis un fichier SQL ou une chaîne SQL,
    puis l'exécute dans Postgres.
    """
    try:
        # 1️⃣ SI SQL DIRECTEMENT DONNÉ → PAS BESOIN DE FICHIER
        if mat_view_sql:
            sql_to_run = mat_view_sql

        else:
            # 2️⃣ SINON → CHARGER LE FICHIER
            if not mat_view_file_name:
                mat_view_file_name = f"{config.MATVIEW_NAME}.sql"

            # Normaliser le nom
            mat_view_file_name_clean = (
                mat_view_file_name
                .replace("../", "")
                .replace("./", "")
                .replace("postgresql/", "")
                .replace(".sql", "")
            )

            # Construire le chemin correct depuis utils/
            # ATTENTION: utils -> revenir à racine -> entrer dans postgresql/
            file_path = os.path.join(
                os.path.dirname(__file__), 
                "..", "postgresql", f"{mat_view_file_name_clean}.sql"
            )

            file_path = os.path.abspath(file_path)

            # Vérification
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"SQL file not found: {file_path}")

            sql_to_run = read_sql_file(file_path)

        # 3️⃣ Exécuter la vue dans Postgres
        conn = get_connection()
        result, status = start_execute_sql(
            conn,
            sql_to_run,
            max_rows=None,
            explain=False
        )

        success = (status == 200)
        return (result, success)

    except Exception as e:
        return ({"error": str(e)}, False)


