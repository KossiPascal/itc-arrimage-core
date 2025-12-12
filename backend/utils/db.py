from psycopg2 import connect, sql, OperationalError
from utils.config import config

def get_connection():
    """
    Crée une connexion à la base de données PostgreSQL.
    """
    try:
        conn = connect(
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            # dbname=config.POSTGRES_DB,
            database=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD,
        )
        conn.autocommit = True
        return conn
    except OperationalError as e:
        print(f"❌❌❌Erreur de connexion à PostgreSQL: {e}")
        return None
    