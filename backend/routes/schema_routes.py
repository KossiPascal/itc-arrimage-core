# backend/sql_routes.py
from flask import Blueprint, jsonify
from utils.auth import require_auth
from utils.db import get_connection
import psycopg2.extras


EXCLUDES_TABLE = ["users","refresh_tokens","saved_queries"]

schema_bp = Blueprint("schema", __name__, url_prefix="/api/schema")

@schema_bp.route("/schema_info", methods=["GET"])
@require_auth
def get_schema_info():
    conn = None
    try:
        conn = get_connection()
        if not conn:
            return jsonify({"error": "PostgreSQL connection failed"}), 500

        result = {
            "schemas": [],
            "tables": [],
            "views": [],
            "matviews": [],
            "sequences": [],
            "indexes": [],
            "constraints": [],
            "functions": [],
            "triggers": []
        }

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # -- Schemas --
            cur.execute("""
                SELECT schema_name
                FROM information_schema.schemata
                ORDER BY schema_name;
            """)
            result["schemas"] = [row["schema_name"] for row in cur.fetchall()]

            # -- Tables --
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public' AND table_type='BASE TABLE'
                ORDER BY table_name;
            """)
            tables = [row["table_name"] for row in cur.fetchall() if row["table_name"] not in EXCLUDES_TABLE]

            for table in tables:
                cur.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=%s
                    ORDER BY ordinal_position;
                """, (table,))
                columns = [dict(row) for row in cur.fetchall()]

                cur.execute("""
                    SELECT
                        kcu.column_name,
                        tc.constraint_type,
                        tc.constraint_name,
                        ccu.table_name AS foreign_table,
                        ccu.column_name AS foreign_column
                    FROM information_schema.table_constraints tc
                    LEFT JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                     AND tc.table_name = kcu.table_name
                    LEFT JOIN information_schema.constraint_column_usage ccu
                      ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.table_schema='public' AND tc.table_name=%s;
                """, (table,))
                constraints = [dict(row) for row in cur.fetchall()]

                result["tables"].append({
                    "table_name": table,
                    "columns": columns,
                    "constraints": constraints
                })

            # -- Views --
            cur.execute("""
                SELECT table_name, view_definition
                FROM information_schema.views
                WHERE table_schema='public'
                ORDER BY table_name;
            """)
            result["views"] = [{"view_name": row["table_name"], "definition": row["view_definition"]}
                               for row in cur.fetchall()]

            # -- Materialized Views --
            cur.execute("""
                SELECT matviewname AS matview_name, definition
                FROM pg_catalog.pg_matviews
                WHERE schemaname='public'
                ORDER BY matviewname;
            """)
            result["matviews"] = [{"matview_name": row["matview_name"], "definition": row["definition"]}
                                  for row in cur.fetchall()]

            # -- Sequences --
            cur.execute("""
                SELECT sequence_name
                FROM information_schema.sequences
                WHERE sequence_schema='public'
                ORDER BY sequence_name;
            """)
            result["sequences"] = [row["sequence_name"] for row in cur.fetchall()]

            # -- Indexes --
            cur.execute("""
                SELECT tablename, indexname, indexdef
                FROM pg_indexes
                WHERE schemaname='public'
                ORDER BY tablename, indexname;
            """)
            result["indexes"] = [dict(row) for row in cur.fetchall()]

            # -- Functions / Stored Procedures --
            cur.execute("""
                SELECT routine_name, routine_type, data_type
                FROM information_schema.routines
                WHERE specific_schema='public'
                ORDER BY routine_name;
            """)
            result["functions"] = [dict(row) for row in cur.fetchall()]

            # -- Triggers --
            cur.execute("""
                SELECT trigger_name, event_manipulation, event_object_table, action_statement
                FROM information_schema.triggers
                WHERE trigger_schema='public'
                ORDER BY trigger_name;
            """)
            result["triggers"] = [dict(row) for row in cur.fetchall()]

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass








# # backend/sql_routes.py
# from flask import Blueprint, request, jsonify
# from auth import require_auth
# from utils.db import get_connection
# import time
# import psycopg2
# import psycopg2.extras
# from decimal import Decimal
# from datetime import datetime, date
# from uuid import UUID

# schema_bp = Blueprint("schema", __name__, url_prefix="/api/schema")

# @schema_bp.route("/tables", methods=["GET"])
# @require_auth
# def get_tables():
#     conn = None
#     try:
#         conn = get_connection()
#         if not conn:
#             return jsonify({"error": "PostgreSQL connection failed"}), 500
#         with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
#             cur.execute("""
#                 SELECT table_name 
#                 FROM information_schema.tables
#                 WHERE table_schema='public' AND table_type='BASE TABLE'
#                 ORDER BY table_name;
#             """)
#             tables = [row["table_name"] for row in cur.fetchall()]
#             cur.execute("""
#                 SELECT table_name 
#                 FROM information_schema.views
#                 WHERE table_schema='public'
#                 ORDER BY table_name;
#             """)
#             views = [row["table_name"] for row in cur.fetchall()]
#             cur.execute("""
#                 SELECT 
#                     schemaname AS table_schema, 
#                     matviewname AS table_name,
#                     definition
#                 FROM pg_catalog.pg_matviews
#                 WHERE schemaname='public'
#                 ORDER BY matviewname;
#             """)
#             matviews = [row["table_name"] for row in cur.fetchall()]
#         return jsonify({"tables": tables, "views": views, "matviews": matviews})
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500
#     finally:
#         if conn:
#             try:
#                 conn.close()
#             except:
#                 pass





# # backend/sql_routes.py
# from flask import Blueprint, request, jsonify
# from auth import require_auth
# from utils.db import get_connection
# import psycopg2
# import psycopg2.extras
# schema_bp = Blueprint("schema", __name__, url_prefix="/api/schema")

# @schema_bp.route("/tables", methods=["GET"])
# @require_auth
# def get_tables():
#     conn = None
#     try:
#         conn = get_connection()
#         if not conn:
#             return jsonify({"error": "PostgreSQL connection failed"}), 500
#         result = {"tables": [], "views": [], "matviews": []}
#         with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
#             # -- Tables --
#             cur.execute("""
#                 SELECT table_name
#                 FROM information_schema.tables
#                 WHERE table_schema='public' AND table_type='BASE TABLE'
#                 ORDER BY table_name;
#             """)
#             tables = [row["table_name"] for row in cur.fetchall()]
#             for table in tables:
#                 cur.execute("""
#                     SELECT column_name, data_type, is_nullable, column_default
#                     FROM information_schema.columns
#                     WHERE table_schema='public' AND table_name=%s
#                     ORDER BY ordinal_position;
#                 """, (table,))
#                 columns = [dict(row) for row in cur.fetchall()]
#                 cur.execute("""
#                     SELECT
#                         kcu.column_name,
#                         tc.constraint_type
#                     FROM information_schema.table_constraints tc
#                     JOIN information_schema.key_column_usage kcu
#                       ON tc.constraint_name = kcu.constraint_name
#                      AND tc.table_schema = kcu.table_schema
#                      AND tc.table_name = kcu.table_name
#                     WHERE tc.table_schema='public' AND tc.table_name=%s;
#                 """, (table,))
#                 constraints = [dict(row) for row in cur.fetchall()]
#                 result["tables"].append({
#                     "table_name": table,
#                     "columns": columns,
#                     "constraints": constraints
#                 })
#             # -- Views --
#             cur.execute("""
#                 SELECT table_name, view_definition
#                 FROM information_schema.views
#                 WHERE table_schema='public'
#                 ORDER BY table_name;
#             """)
#             views = []
#             for row in cur.fetchall():
#                 views.append({
#                     "view_name": row["table_name"],
#                     "definition": row["view_definition"]
#                 })
#             result["views"] = views
#             # -- Materialized Views --
#             cur.execute("""
#                 SELECT matviewname AS matview_name, definition
#                 FROM pg_catalog.pg_matviews
#                 WHERE schemaname='public'
#                 ORDER BY matviewname;
#             """)
#             matviews = []
#             for row in cur.fetchall():
#                 matviews.append({
#                     "matview_name": row["matview_name"],
#                     "definition": row["definition"]
#                 })
#             result["matviews"] = matviews
#         return jsonify(result)
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500
#     finally:
#         if conn:
#             try:
#                 conn.close()
#             except:
#                 pass
