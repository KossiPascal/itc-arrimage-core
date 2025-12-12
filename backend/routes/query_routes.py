# backend/query_routes.py
from flask import Blueprint, request, jsonify
from utils.auth import require_auth
from utils.db import get_connection
import psycopg2
import psycopg2.extras
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID

query_bp = Blueprint("query", __name__, url_prefix="/api/query")

# --------------------------
# Ensure Table Exists
# --------------------------
def ensure_table_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS saved_queries (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name TEXT NOT NULL,
                sql TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()

# Sérialisation universelle
def jsonify_value(val):
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, UUID):
        return str(val)
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="ignore")
    return val

# --------------------------
# List Queries
# --------------------------
@query_bp.route("/", methods=["GET"])
@require_auth
def get_queries():
    """Récupère toutes les requêtes sauvegardées"""
    conn = get_connection()
    ensure_table_exists(conn)

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT id, name, sql, created_at, updated_at 
                FROM saved_queries 
                ORDER BY updated_at DESC;
            """)
            data = [dict(row) for row in cur.fetchall()]
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# Create / Save Query
@query_bp.route("/", methods=["POST"])
@require_auth
def save_query():
    """Sauvegarde une nouvelle requête SQL utilisateur."""
    payload = request.get_json() or {}
    name = payload.get("name")
    sql_text = payload.get("sql")

    if not name or not sql_text:
        return jsonify({"error": "Name and query are required"}), 400

    conn = get_connection()
    # --- Auto-create table if not exists ---
    ensure_table_exists(conn)
    
    try:
        with conn.cursor() as cur:
            # --- Insert new query ---
            cur.execute(
                "INSERT INTO saved_queries (name, sql) VALUES (%s, %s) RETURNING id;",
                (name, sql_text)
            )
            saved_id = cur.fetchone()[0]
            conn.commit()

        return jsonify({ "id": saved_id, "message": "Query saved successfully" })
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()


# Get One Query
@query_bp.route("/<int:query_id>", methods=["GET"])
@require_auth
def get_query(query_id):
    conn = get_connection()
    ensure_table_exists(conn)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, sql FROM saved_queries WHERE id = %s;", (query_id,))
            row = cur.fetchone()

        if not row:
            return jsonify({"error": "Not found"}), 404

        return jsonify({"id": row[0], "name": row[1], "sql": row[2]}), 200
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# Update One Query
@query_bp.route("/<int:query_id>", methods=["PUT"])
@require_auth
def update_query(query_id):
    """Met à jour une requête existante"""
    payload = request.get_json() or {}
    query_name = payload.get("name")
    query_sql = payload.get("sql")

    if not query_name or not query_sql:
        return jsonify({"error": "Name and query are required"}), 400

    conn = get_connection()
    ensure_table_exists(conn)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE saved_queries 
                SET name=%s, sql=%s, updated_at=NOW() 
                WHERE id=%s RETURNING id;""",
                (query_name, query_sql, query_id)
            )
            if cur.rowcount == 0:
                return jsonify({"error": "Query not found"}), 404
            conn.commit()
        return jsonify({"id": query_id, "message": "Query updated successfully"}), 200
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()


@query_bp.route("/<int:query_id>", methods=["DELETE"])
@require_auth
def delete_query(query_id):
    """Supprime une requête sauvegardée"""
    conn = get_connection()
    ensure_table_exists(conn)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM saved_queries WHERE id=%s;", (query_id,))
            if cur.rowcount == 0:
                return jsonify({"error": "Query not found"}), 404
            conn.commit()
        return jsonify({"message": "Query deleted successfully"})
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()
