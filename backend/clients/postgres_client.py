from time import sleep
from typing import Any
from psycopg2 import sql, OperationalError, DatabaseError, InterfaceError
from psycopg2.extras import Json, execute_values, RealDictCursor
from utils.config import config
from datetime import datetime, date, timezone, time
from utils.db import get_connection
from utils.functions import to_datetime
from utils.hasher_uitls import hash_password

from utils.logger import get_logger
logger = get_logger(__name__)

DHIS2_TABLE_KEY = {
    "events": "id",
    "attributes": "id",
    "enrollments": "id",
    "dataElements": "id",
    "organisationUnits": "id",
    "trackedEntityInstances": "id",
}


class PostgresClient:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        self.force_init = config.FORCE_INIT_CLASS

        if self._initialized and self.force_init is not True:
            return

        self.conn = get_connection()
        if not self.conn:
            raise ValueError("❌ Erreur de connexion à PostgreSQL : connexion nulle")

        # Caches pour éviter de refaire les vérifications
        self._verified_tables = set()
        self._verified_columns = {}  # table_name -> set(columns)
        self._verified_pk = set()    # (table_name, id_field)

        self.ensure_tables()
        self.create_default_admin()

        self._initialized = True


    def normalize_tablename(self, tablename: str) -> str:
        """
        Convert table or column names to lowercase
        and remove double quotes to avoid case-sensitive identifiers.
        """

        # # --- Validation sécurisée ---
        # if not isinstance(tablename, str):
        #     raise ValueError("table must be a string")
        
        # if not tablename:
        #     return tablename
        # tablename = tablename.replace('"', '')
        # return tablename.lower()
        return tablename

    def convert_value_for_pg(self, value):
        """Convertit automatiquement toutes les valeurs en formats compatibles PostgreSQL."""
        # JSONB
        if isinstance(value, (dict, list, set, tuple)):
            return Json(value)
        # Dates, datetimes → string ISO (psycopg2 gère ensuite)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return value

    def guess_pg_type(self, value, colname=None, id_field=None):
        """
        Détecte automatiquement le type SQL PostgreSQL pour une valeur donnée.
        - id_field DHIS2 → TEXT
        - bool → BOOLEAN
        - int → BIGINT
        - float → DOUBLE PRECISION
        - list/dict → JSONB
        - datetime/date/time → TIMESTAMP WITH TIME ZONE ou DATE
        - Autres → TEXT
        """

        # ID DHIS2 ou valeur None → TEXT
        if colname is not None and colname == id_field or value is None:
            return "TEXT"
        if isinstance(value, bool): return "BOOLEAN"
        if isinstance(value, int): return "BIGINT"
        if isinstance(value, float): return "DOUBLE PRECISION"
        if isinstance(value, (list, dict, set, tuple)): return "JSONB"
        if isinstance(value, datetime): return "TIMESTAMP WITH TIME ZONE"
        if isinstance(value, date): return "DATE"
        if isinstance(value, time): return "TIME"
        # Tout le reste (str, etc.)
        return "TEXT"
    
    def ensure_tables(self) -> bool:
        if "base_tables" in self._verified_tables and self.force_init is not True:
            return
        
        try:
            with self.conn.cursor() as cur:
                # Data_elements
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS "dataElements" (
                        id TEXT PRIMARY KEY,
                        name TEXT,
                        shortname TEXT,
                        synced_at TIMESTAMP DEFAULT now()
                    );
                """)

                # Users
                # role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin', 'superadmin')),
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        fullname TEXT,
                        username TEXT UNIQUE NOT NULL,
                        password TEXT NOT NULL,
                        role TEXT NOT NULL DEFAULT 'user',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                """)

                # Refresh tokens
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS refresh_tokens (
                        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
                        token TEXT UNIQUE NOT NULL,
                        expires_at TIMESTAMP NOT NULL,
                        revoked BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS "organisationUnits" (
                        id TEXT PRIMARY KEY,
                        name TEXT,
                        shortname TEXT,
                        parent JSONB,
                        level BIGINT,
                        synced_at TIMESTAMP DEFAULT now()
                    );
                """)
                    
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS sync_state (
                        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        last_sync TIMESTAMP WITH TIME ZONE
                    );
                    --INSERT INTO sync_state (last_sync) SELECT now() - INTERVAL '90 days' WHERE NOT EXISTS (SELECT 1 FROM sync_state);
                """)

            
            self.conn.commit()  # <- commit après création
            self._verified_tables.add("base_tables")
        except Exception as e:
            self.conn.rollback()
            logger.exception(f"Erreur création tables de base: {e}")
            raise
    
    # --------------------------
    # CREATION ADMIN PAR DEFAUT
    # --------------------------
    def create_default_admin(self):
        if "admin_created" in self._verified_tables and self.force_init is not True:
            return
        
        try:
            DFA = config.DEFAULT_ADMIN
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Vérifier si un admin existe déjà
                # cur.execute("SELECT * FROM users WHERE role='superadmin' LIMIT 1;")
                cur.execute("SELECT 1 FROM users WHERE username = %s LIMIT 1;", (DFA["username"],))
                if not cur.fetchone():
                    pw_hash = hash_password(DFA["password"])
                    cur.execute("""
                        INSERT INTO users (fullname, username, password, role)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id, fullname, username, role;
                    """, (DFA["fullname"], DFA["username"], pw_hash, DFA["role"]))
                    self.conn.commit()
                    logger.info("✅ Admin par défaut créé avec succès")
            self._verified_tables.add("admin_created")
        except Exception as e:
            self.conn.rollback()
            logger.exception(f"Erreur création admin: {e}")
            raise

    def ensure_table_exist_create_if_not(self, table: str, data: dict, id_field:str)->bool:
        """
        Vérifie si la table existe, sinon la crée automatiquement selon data.
        data est un dictionnaire avec un exemple de clé/valeur pour détecter le type.
        """

        table = self.normalize_tablename(table)
        if table in self._verified_tables and self.force_init is not True:
            return True
        
        try:
            is_dict_data = True if data and isinstance(data, dict) else False

            if is_dict_data:
                object_id = data.get(id_field) if id_field else None
                if object_id is None:
                    raise ValueError(f"❌ Missing id_field '{id_field}'")
                
            with self.conn.cursor() as cur:
                # Vérifie si la table existe
                # cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);", (table,))
                cur.execute("SELECT to_regclass(%s);", (f'"{table}"',))  # IMPORTANT : regclass doit recevoir "NomExact"

                if cur.fetchone()[0]:
                    self._verified_tables.add(table)
                    return True

                if is_dict_data:
                    # Crée la table si elle n'existe pas
                    columns = []
                    for col, val in data.items():
                        col_type = self.guess_pg_type(val)
                        # On ajoute 'PRIMARY KEY' pour l'ID si la clé est 'id_field'
                        primary_key_type = "PRIMARY KEY" if col == f'"{id_field}"' else ""
                        # # On ajoute 'NOT NULL' pour l'ID si la clé est 'id_field'
                        # not_null = "NOT NULL" if col == f'"{id_field}"' else ""
                        columns.append(f'"{col}" {col_type} {primary_key_type}'.strip())

                    create_query = f'CREATE TABLE "{table}" ({", ".join(columns)});'
                    cur.execute(create_query)
                    self.conn.commit()

                    logger.info(f"🆕 Table '{table}' créée avec succès.")
                    self._verified_tables.add(table)
            return False
        except Exception as e:
            self.conn.rollback()
            logger.exception(f"Erreur dans 'ensure_table_exist_create_if_not' : {e}")
            raise

    def check_if_exists(self, table:str, id_field:str, object_id:str):
        """
        Vérifie si une entrée existe dans la base de données en fonction de l'ID.
        """
        try:
            table = self.normalize_tablename(table)

            if not id_field:
                raise ValueError(f"❌ Missing id_field '{id_field}'")
            
            if not object_id:
                raise ValueError(f"❌ Missing object_id '{object_id}'")
            
            with self.conn.cursor() as cursor:
                # cursor.execute(f"SELECT 1 FROM {table} WHERE {id_field} = %s LIMIT 1",(object_id,))
                query = sql.SQL("SELECT 1 FROM {} WHERE {} = %s").format(
                    sql.Identifier(table), sql.Identifier(id_field)
                )
                cursor.execute(query, (object_id,))
                return cursor.fetchone() is not None
        except Exception as e:
            logger.exception(f"Erreur lors de la vérification de l'existence : {e}")
            return False

    def ensure_columns_exist(self, table:str, data:dict, id_field:str):
        """
        Vérifie que toutes les colonnes existent dans la table.
        Si une colonne n'existe pas -> elle est ajoutée automatiquement.
        """
        table = self.normalize_tablename(table)
        if table not in self._verified_columns:
            self._verified_columns[table] = set()

        try:
            # Initialiser le cache si absent
            if data and isinstance(data, dict):
                missing_columns = [c for c in data if c not in self._verified_columns[table]]
                if not missing_columns:
                    return

                object_id = data.get(id_field) if id_field else None
                if object_id is None:
                    raise ValueError(f"❌ Missing id_field '{id_field}'")
        
                with self.conn.cursor() as cursor:
                    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table,))
                    existing_columns = {row[0] for row in cursor.fetchall()}
                    
                    for column, value in data.items():
                        if column not in existing_columns:
                            col_type = self.guess_pg_type(value)
                            cursor.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {col_type};')
                            logger.info(f"➕ Colonne ajoutée: {column} ({col_type})")
                            self.conn.commit()
                        self._verified_columns[table].add(column)

        except Exception as e:
            self.conn.rollback()
            logger.exception("❌ Error with 'ensure_columns_exist' for %s: %s", object_id, e)


    def ensure_pk_or_unique(self, table, id_field):
        """
        Vérifie que la colonne id_field est PRIMARY KEY ou UNIQUE.
        La crée automatiquement si manquante.
        """
        key = (table, id_field)
        if key in self._verified_pk and self.force_init is not True:
            return

        query_check = """
            SELECT constraint_type
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
            WHERE tc.table_name = %s AND ccu.column_name = %s AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE');
        """

        with self.conn.cursor() as cur:
            cur.execute(query_check, (table, id_field))
            if not cur.fetchone(): # PK/unique non existant
                constraint = f"{table}_{id_field}_unique"
                alter = sql.SQL('ALTER TABLE {table} ADD CONSTRAINT {constraint} UNIQUE ({col});').format(
                    table=sql.Identifier(table),
                    constraint=sql.Identifier(constraint),
                    col=sql.Identifier(id_field)
                )
                cur.execute(alter)
                self.conn.commit()
                logger.info(f"✔ Contrainte UNIQUE créée sur {table}.{id_field}")

        self._verified_pk.add(key)


    def _insert_or_update(self, table:str, data:dict, id_field:str):
        """
        Insert or update PostgreSQL record.
        Automatically adds missing columns before upsert.
        """
        is_dict_data = True if data and isinstance(data, dict) else False

        if is_dict_data:
            object_id = data.get(id_field) if id_field else None
            if object_id is None:
                raise ValueError(f"  {table} -> ❌ Missing id_field '{id_field}' in payload")
        else:
            raise ValueError(f"  {table} -> ❌ data must be a dict")
     
        table = self.normalize_tablename(table)

        retries = 0
        while retries < config.MAX_RETRIES:
            try:
                # 1️⃣ Créer la table si nécessaire avec les noms EXACTS
                self.ensure_table_exist_create_if_not(table, data, id_field)

                # 🔥 Auto-create columns if missing
                # 2️⃣ Ajouter colonnes manquantes sans modifier la casse
                self.ensure_columns_exist(table, data, id_field)

                # 3️⃣ Construire la requête en respectant exactement les key names
                columns = list(data.keys())
                # values = list(data.values())
                values = [self.convert_value_for_pg(data[c]) for c in columns]   # ✔ Correction clé

                # 2️⃣ UPDATE ou INSERT
                exists = self.check_if_exists(table, id_field, object_id)

                if exists:
                    # ——— UPDATE ———
                    set_clause = ", ".join([f'"{c}" = %s' for c in columns])
                    query = f'UPDATE "{table}" SET {set_clause} WHERE "{id_field}" = %s'
                    params = values + [object_id]
                    op = f"✔ UPDATE"

                else:
                    # ——— INSERT ———
                    column_list = ", ".join([f'"{c}"' for c in columns])
                    placeholders = ", ".join(["%s"] * len(values))
                    query = f'INSERT INTO "{table}" ({column_list}) VALUES ({placeholders})'
                    params = values
                    op = f"✅ INSERT"


                with self.conn.cursor() as cursor:
                    cursor.execute(query, params)
                    self.conn.commit()

                # print(f"{op} ✔ {object_id}")
                logger.info(f"  {table} -> {op} réussi pour {object_id}")
                return True

            except Exception as e:
                self.conn.rollback()   # 🔥 IMPORTANT : éviter de bloquer la connexion
                retries += 1
                logger.exception(f"  {table} -> ❌ Insert/update failed for %s: %s", object_id, e)

                if retries < config.MAX_RETRIES:
                    logger.warning(f"⏳ Tentative {retries}/{config.MAX_RETRIES} échouée. Nouvelle tentative dans {config.RETRY_DELAY}s...")
                    sleep(config.RETRY_DELAY)
                else:
                    logger.error(f"  {table} -> ⛔ Échec définitif pour {object_id} après {config.MAX_RETRIES} tentatives")
                    return False

    def _bulk_insert_or_update(self, table: str, data: list, id_field: str):
        """
        Bulk UPSERT optimisé (insert or update).
        - auto-création table
        - auto-création colonnes manquantes
        - transactions par batch
        - retry intelligent
        - millions de lignes supportés
        """
        try:
            # 🔍 0. Validation entrée
            if not isinstance(data, list):
                logger.error(f"  {table} -> ❌ data must be a list of dict")
                return False

            if len(data) == 0:
                logger.warning(f"Aucune donnée à insérer pour {table}")
                return False

            if not isinstance(data[0], dict):
                logger.error(f"  {table} -> ❌ Chaque élément de data doit être un dict")
                return False

            table = self.normalize_tablename(table)

            # ✅ 1. Préparer structure
            sample = data[0]

            if id_field not in sample:
                logger.error(f"  {table} -> ❌ Missing id_field '{id_field}' in payload records")
                return False

            # 🔧 Création auto table + colonnes
            self.ensure_table_exist_create_if_not(table, sample, id_field)
            self.ensure_columns_exist(table, sample, id_field)
            self.ensure_pk_or_unique(table, id_field)

            columns = list(sample.keys())
            pg_columns = ', '.join(f'"{c}"' for c in columns)

            # 🔥 Colonnes à update (toutes sauf id_field)
            update_columns = [c for c in columns if c != id_field]
            update_clause = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in update_columns])

            # Requête UPSERT (INSERT ... ON CONFLICT)
            base_query = (f'INSERT INTO "{table}" ({pg_columns}) VALUES %s '
                        f'ON CONFLICT ("{id_field}") DO UPDATE SET {update_clause};')

            total_rows = len(data)
            logger.info(f"🚀 BULK UPSERT de {total_rows} lignes → {table}")

            cur = self.conn.cursor()


            # 🚚 2. Process par batch
            for start in range(0, total_rows, config.BATCH_SIZE):

                batch = data[start:start + config.BATCH_SIZE]
                batch_tuples = [
                    tuple(self.convert_value_for_pg(row.get(c)) for c in columns)
                    for row in batch
                ]

                retries = 0
                batch_num = (start // config.BATCH_SIZE) + 1

                while retries <= config.MAX_RETRIES:
                    try:
                        execute_values(cur, base_query, batch_tuples)
                        self.conn.commit()
                        logger.info(f"✔ Batch {batch_num} ({len(batch)} rows) upserted")
                        break

                    except (OperationalError, InterfaceError) as e:
                        self.conn.rollback()
                        retries += 1

                        if retries > config.MAX_RETRIES:
                            logger.error(f"  {table} -> ❌ ÉCHEC FINAL batch {batch_num} après retries. Erreur: {e}")
                            return False

                        logger.warning(
                            f"⚠ Erreur temporaire batch {batch_num}: {e}. "
                            f"Retry {retries}/{config.MAX_RETRIES} dans {config.RETRY_DELAY}s"
                        )
                        time.sleep(config.RETRY_DELAY)

                    except DatabaseError as e:
                        # Erreur SQL (colonne, type, table) → non récupérable
                        self.conn.rollback()
                        logger.error(f"  {table} -> ⛔ ERREUR SQL batch {batch_num}: {e}")
                        logger.error("📌 Type erreur : %s", type(e))
                        logger.error("📌 Détails : %s", e)

                        # Extract full PG error context if available
                        if hasattr(e, "pgerror"):
                            logger.error("📌 PG Error : %s", e.pgerror)

                        if hasattr(e, "diag"):
                            logger.error("📌 PG diag : %s", e.diag.message_primary)
                            logger.error("📌 Hint : %s", getattr(e.diag, "hint", None))
                            logger.error("📌 Detail : %s", getattr(e.diag, "detail", None))

                        logger.error("📌 Requête SQL : %s", base_query)

                        # Dump the tuple that causes the crash
                        logger.error("📌 Exemple valeurs : %s", batch_tuples[:3])

                        return False

                    except Exception as e:
                        # Erreur inconnue → non récupérable
                        self.conn.rollback()
                        logger.error(f"❌ ERREUR inconnu batch {batch_num}: {e}")
                        return False

            cur.close()

            logger.info(f"  {table} -> 🏁 Bulk UPSERT terminé : {total_rows} lignes → {table}")
            return True

        except Exception as e:
            logger.error(f"  {table} -> ❌ ERREUR critique Bulk UPSERT: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return False


    # Stockage en base de donnée
    def upsert_data(self, table:str, data:dict) -> bool:
        id_field = DHIS2_TABLE_KEY[table]
        object_id = (data or {}).get(id_field) if id_field else None
        try:
            table = self.normalize_tablename(table)
            data['synced_at'] = datetime.now(timezone.utc)
            return self._insert_or_update(table, data, id_field)
        except Exception as e:
            logger.exception(f"  {table} -> ❌ Insert/update failed for %s: %s", object_id, e)
            return False
        
    # Stockage en bulk en base de donnée
    def bulk_upsert_data(self, table:str, dataList:list) -> bool:
        # 🔍 0. Validation entrée
        if not isinstance(dataList, list):
            logger.error(f"  {table} -> ❌ data must be a list of dict")
            return False
        
        if len(dataList) > 0:
            id_field = DHIS2_TABLE_KEY[table]
            try:
                table = self.normalize_tablename(table)
                synced_at = datetime.now(timezone.utc)
                for data in dataList:
                    data['synced_at'] = synced_at
                return self._bulk_insert_or_update(table, dataList, id_field)
            except Exception as e:
                logger.exception(f"  {table} -> ❌ Insert/update failed: %s", e)
                return False
        
    # Suppression de donnée
    def delete_data(self, table: str, record_id: str) -> bool:
        """
        Supprime un enregistrement dans `table` basé sur `id_field = record_id`.

        Retourne :
            True  → si un enregistrement a été supprimé
            False → si rien n'a été supprimé ou en cas d'erreur
        """

        id_field = DHIS2_TABLE_KEY[table]

        if not table or not record_id or not id_field:
            raise ValueError(f"❌ Arguments invalides : table: {table}, record_id: {record_id} et id_field: {id_field} sont obligatoires.")

        try:
            table = self.normalize_tablename(table)

            with self.conn.cursor() as cursor:
                delete_query = sql.SQL('DELETE FROM "{}" WHERE {} = %s').format(sql.Identifier(table), sql.Identifier(id_field))

                cursor.execute(delete_query, (record_id,))
                deleted_rows = cursor.rowcount  # Nombre de lignes supprimées

            # Commit de la transaction
            self.conn.commit()

            return deleted_rows > 0

        except Exception as e:
            print(f"❌ Erreur lors de la suppression dans '{table}': {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return False

    # Suppression  en bulk de donnée
    def bulk_delete_data(self, table: str, record_ids: list) -> bool:
        """
        Suppression massive (bulk delete) d'enregistrements DHIS2.
        - Haute performance
        - Gestion des erreurs + retries
        - Suppression par batch
        - Auto-détection du champ clé via DHIS2_TABLE_KEY[table]

        Args:
            table (str): Nom de la table PostgreSQL
            record_ids (list): Liste d'IDs à supprimer (valeurs simples)

        Returns:
            bool: True si suppression réussie, False en cas d'échec.
        """

        # --- 🔍 Validation ---
        if not table or table not in DHIS2_TABLE_KEY:
            raise ValueError(f"❌ Table inconnue ou invalide : '{table}'")

        if not isinstance(record_ids, list) or len(record_ids) == 0:
            logger.warning(f"Aucun ID fourni pour suppression dans {table}")
            return False

        id_field = DHIS2_TABLE_KEY[table]

        table = self.normalize_tablename(table)

        logger.info(f"🗑️ Bulk delete → {table}: {len(record_ids)} IDs")

        batch_size = getattr(config, "BATCH_SIZE", 5000)

        try:
            cur = self.conn.cursor()

            # --- 🚀 Suppression par batch ---
            for start in range(0, len(record_ids), batch_size):
                batch = record_ids[start:start + batch_size]

                retries = 0
                batch_num = start // batch_size + 1

                delete_query = sql.SQL('DELETE FROM "{}" WHERE {} = ANY(%s)').format(sql.Identifier(table), sql.Identifier(id_field))

                while retries <= config.MAX_RETRIES:
                    try:
                        cur.execute(delete_query, (batch,))
                        self.conn.commit()

                        logger.info(
                            f"✔ Batch {batch_num} DELETE ({cur.rowcount} lignes supprimées)"
                        )
                        break

                    except (OperationalError, InterfaceError) as e:
                        self.conn.rollback()
                        retries += 1

                        if retries > config.MAX_RETRIES:
                            raise

                        logger.warning(
                            f"⚠ Erreur réseau batch {batch_num}: {e}. "
                            f"Retry {retries}/{config.MAX_RETRIES} dans {config.RETRY_DELAY}s"
                        )
                        time.sleep(config.RETRY_DELAY)

                    except DatabaseError as e:
                        self.conn.rollback()
                        logger.error(f"⛔ Erreur SQL batch {batch_num}: {e}")
                        raise

                    except Exception as e:
                        self.conn.rollback()
                        logger.error(f"❌ Erreur inconnue batch {batch_num}: {e}")
                        raise

        finally:
            try:
                cur.close()
            except:
                pass

        logger.info(f"🏁 Bulk DELETE terminé → {table}: {len(record_ids)} IDs supprimés")
        return True

    # Récupération en base de donnée
    def _list_data(self, table: str,fields: list | tuple | None = None,*,limit: int | None = None,offset: int | None = None,filters: dict | None = None,order_by: str = "synced_at",order_dir: str = "DESC") -> list[dict[str, Any]]:
        """
        Récupère une liste générique depuis n'importe quelle table PostgreSQL.

        Params:
            table       : nom de la table
            fields      : liste des colonnes à sélectionner (default: *)
            limit       : nombre max de lignes
            offset      : décalage (pagination)
            filters     : dict {col: value} pour WHERE
            order_by    : colonne de tri
            order_dir   : ASC ou DESC
        """

        # --- Validation sécurisée ---
        if not isinstance(table, str):
            raise ValueError("table must be a string")
        
        table = self.normalize_tablename(table)

        if fields and not isinstance(fields, (list, tuple)):
            raise ValueError("fields must be list or tuple")

        if order_dir.upper() not in ("ASC", "DESC"):
            raise ValueError("order_dir must be 'ASC' or 'DESC'")

        # --- Colonnes à sélectionner (SELECT)---
        if fields:
            fields_sql = ", ".join([f'"{c}"' for c in fields])
        else:
            fields_sql = "*"

        # --- Construction dynamique du WHERE ---
        where_clauses = []
        values = []

        if filters:
            for col, val in filters.items():
                where_clauses.append(f'"{col}" = %s')
                values.append(val)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # --- ORDER BY ---
        order_sql = f'ORDER BY "{order_by}" {order_dir}'

        # --- LIMIT / OFFSET ---
        limit_sql = ""
        if limit and limit > 0:
            limit_sql += " LIMIT %s"
            values.append(limit)

        if offset and offset > 0:
            limit_sql += " OFFSET %s"
            values.append(offset)

        # --- SQL finale ---
        query = f'SELECT {fields_sql} FROM "{table}" {where_sql} {order_sql} {limit_sql}'

        # --- Execution ---
        with self.conn.cursor() as cur:
            cur.execute(query, tuple(values))
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

        # --- conversion: tuple → dict ---
        result = []
        for row in rows:
            row_dict = {}
            for col, val in zip(colnames, row):
                # Convertir datetime → ISO
                if hasattr(val, "isoformat"):
                    row_dict[col] = val.isoformat()
                else:
                    row_dict[col] = val
            result.append(row_dict)

        return result

    def list_orgunits(self, level:int=None, only_ids:bool=False) -> list[dict[str, Any]]:
        # id_field = 'id'
        isGoodLevel = level is not None and isinstance(level,int) and level > 0
        filters={"level": level} if isGoodLevel else None
        
        orgunits = self._list_data("organisationUnits", filters=filters)

        if only_ids is True:
            orgunits_ids = [ou["id"] for ou in orgunits]
            return orgunits_ids
        
        return orgunits
        
    def list_dataelement(self) -> list[dict[str, Any]]:
        # id_field = 'id'
        # fields=["id","name","code","shortName", "created", "synced_at"]
        fields=["id","name","shortName", "synced_at"]
        return self._list_data("dataElements", fields=fields)

    def list_tei(self) -> list[dict[str, Any]]:
        # id_field = 'trackedEntityInstance'
        return self._list_data("trackedEntityInstances")
    
    def list_enrollment(self) -> list[dict[str, Any]]:
        # id_field = 'enrollment'
        return self._list_data("enrollments")

    def list_event(self) -> list[dict[str, Any]]:
        # id_field = 'event'
        return self._list_data("events")
    
    def list_attributes(self) -> list[dict[str, Any]]:
        # id_field = 'event'
        return self._list_data("attributes")
    
    def get_last_sync(self):
        default_date = to_datetime("2022-01-01T00:00:00")
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT last_sync FROM sync_state ORDER BY id DESC LIMIT 1;")
                row = cur.fetchone()
                # Aucun enregistrement
                if not row or not row[0]:
                    return default_date
                value = row[0]
                # Si la valeur est déjà un datetime PostgreSQL → OK
                if isinstance(value, datetime):
                    return value
                # Sinon convertir (si string)
                return to_datetime(value)
        except Exception:
            return default_date


    def update_last_sync(self, new_dt: datetime):
        try:
            with self.conn.cursor() as cur:
                # Always ensure new_dt is a valid datetime object  
                if not isinstance(new_dt, datetime):
                    raise ValueError("new_dt must be a datetime object")
                # Check if a row already exists
                cur.execute("SELECT id FROM sync_state ORDER BY id DESC LIMIT 1;")
                row = cur.fetchone()
                if row:
                    # Update last_sync in the existing row
                    cur.execute("UPDATE sync_state SET last_sync = %s WHERE id = %s;",(new_dt, row[0]))
                else:
                    # Insert a new sync_state row
                    cur.execute("INSERT INTO sync_state (last_sync) VALUES (%s);",(new_dt,))
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()   # IMPORTANT: ensures DB is not left in a bad state
            return False
