# backend/sql_routes.py
import re
import json
import time
import logging
import psycopg2
import psycopg2.extras

from uuid import UUID
from decimal import Decimal
from datetime import datetime, date
from flask import Blueprint, request, jsonify, current_app
from utils.auth import require_auth
from utils.models import User
from utils.db import get_connection

logger = logging.getLogger("sql_routes")

# ---------------- CONFIG ----------------
MAX_ALLOWED_ROWS = 50000      # sécurité haute
STATEMENT_TIMEOUT_MS = 15_000  # 15s
DEFAULT_NON_ADMIN_MAX_ROWS = 1000  # si non-admin et pas de max_rows fourni

# Commandes à bloquer globalement (tu peux assouplir pour superadmin)
BLOCKED_SQL = [
    "DROP", "TRUNCATE", "ALTER", "GRANT", "REVOKE", "CREATE ROLE",
    "CREATE DATABASE", "COPY", "DO", "EXEC", "FUNCTION", "PROCEDURE",
    "CREATE TABLE", "VACUUM", "ANALYZE", "REFRESH MATERIALIZED VIEW"
]

# Tables totalement exclues pour tout le monde sauf superadmin
EXCLUDES_TABLE = ["users", "refresh_tokens", "saved_queries"]

# Regex pour détecter référence à tables protégées (schema.table, "quoted", simple)
# On match : optional schema + dot + table, or quoted table with optional schema
EXCLUDES_PATTERN = re.compile(
    r'(?:\b\w+\.)?(?:"(?:' + r'|"|'.join(re.escape(t) for t in EXCLUDES_TABLE) + r')"|(?:' + r'|'.join(re.escape(t) for t in EXCLUDES_TABLE) + r'))\b',
    re.IGNORECASE
)

# Mots-clés dangereux à rechercher au début de la requête
DANGEROUS_KEYWORDS = [
    "DROP", "TRUNCATE", "ALTER", "GRANT", "REVOKE", "CREATE",
    "VACUUM", "ANALYZE", "REFRESH"
]

# Multi-statement detection (improved: semicolons outside quotes)
MULTI_STATEMENT_SEMICOLON_RE = re.compile(r";\s*(SELECT|INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE)", re.IGNORECASE)  # re.compile(r";")

# Helper patterns
_LEADING_WORD_RE = re.compile(r"^\s*(?P<first>\w+)", re.IGNORECASE)

# # Mots-clés SQL à surveiller (toutes les opérations)
EXCLUDES_PATTERN_DANGEROUS_KEYWORDS = [
    "from", "join", "update", "insert", "delete", "drop", "truncate", "refresh",
    "alter", "into", "table", "grant", "revoke", "create", "vacuum", "analyse"
]

KEYWORDS_REGEX = re.compile(
    r"\b(" + "|".join(re.escape(t) for t in EXCLUDES_PATTERN_DANGEROUS_KEYWORDS) + r")\b",
    re.IGNORECASE
)


# ------------------ SERIALIZATION ------------------
def jsonify_value(val):
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, Decimal):
        # keep precision? cast to float (acceptable for most indicators)
        return float(val)
    if isinstance(val, UUID):
        return str(val)
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="ignore")
    if isinstance(val, (list, tuple)):
        return [jsonify_value(v) for v in val]
    return val

# ------------------ SQL CLEANING & ANALYSIS ------------------
def remove_sql_comments(sql: str) -> str:
    """
    Remove -- comments and /* */ comments.
    Not perfect for nested, but covers most practical attacks.
    """
    # Remove block comments first
    sql_no_block = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    # Remove line comments
    sql_no_lines = re.sub(r"--.*?$", " ", sql_no_block, flags=re.M)
    return sql_no_lines

def normalize_sql(sql: str) -> str:
    """Lowercase + collapse whitespace for easier lexical checks (but keep original case for execution)."""
    cleaned = remove_sql_comments(sql)
    return " ".join(cleaned.strip().split())

def get_first_keyword(sql: str) -> str | None:
    """Return first token/word of the SQL (SELECT, INSERT, etc.)"""
    m = _LEADING_WORD_RE.search(remove_sql_comments(sql))
    return m.group("first").upper() if m else None

def contains_excluded_table(sql: str) -> bool:
    """
    Detect if SQL touches an excluded table.
    Uses a robust regex that matches schema.table, quoted, and simple occurrences.
    """
    if not sql:
        return False
    
    sql = " ".join(sql.lower().split())  # supprime les doubles espaces, \n, \t
    tokens = sql.split()
    for i, token in enumerate(tokens):
        if KEYWORDS_REGEX.match(token):
            if i + 1 < len(tokens):
                next_token = tokens[i + 1].replace('"', '').replace("'", "")
                if EXCLUDES_PATTERN.search(next_token):
                    return True
        if EXCLUDES_PATTERN.search(sql):
            return True

    cleaned = remove_sql_comments(sql)
    return bool(EXCLUDES_PATTERN.search(cleaned))

def contains_blocked_keyword(sql: str) -> str | None:
    """Return the blocked keyword found (exact) or None"""
    upper = normalize_sql(sql).upper()
    for kw in DANGEROUS_KEYWORDS:
        # word boundary
        if re.search(r"\b" + re.escape(kw) + r"\b", upper):
            return kw
    return None

def has_multiple_statements(sql: str) -> bool:
    """
    Heuristic: detect semicolons that act as statement separators.
    We reject if there's more than one non-empty statement.
    This avoids naive ';' in strings by a simple heuristic: count semicolons after removing quoted strings.
    """
    # remove single and double quoted strings to avoid semicolons inside them
    tmp = re.sub(r"'.*?'|\".*?\"", " ", sql, flags=re.S)
    semi_count = tmp.count(";")
    return semi_count > 0  # any semicolon treated as multi-statement; conservative


# ------------------ EXECUTION ------------------
def start_execute_sql(conn, sql_text, max_rows=None, explain: bool = False, read_only: bool = False):
    """
    Execute a SQL and return (result_dict, status_code).
    Uses statement_timeout and, for read_only, sets transaction read-only.
    """
    if not conn:
        return ({"error": "PostgreSQL connection failed"}, 500)

    start_ts = time.time()
    cur = None
    try:
        # We will use a server-side cursor for big selects? for simplicity use normal cursor
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Apply statement timeout for this transaction
        try:
            cur.execute(f"SET LOCAL statement_timeout = {int(STATEMENT_TIMEOUT_MS)};")
        except Exception as e:
            logger.warning("Could not set statement_timeout: %s", e)

        # If read_only requested (for non-admins), set transaction readonly
        if read_only:
            try:
                cur.execute("SET LOCAL TRANSACTION READ ONLY;")
            except Exception as e:
                # If DB doesn't permit, log and continue (we'll still avoid writes via checks)
                logger.warning("Could not set transaction READ ONLY: %s", e)

        # EXPLAIN mode
        if explain:
            cur.execute("EXPLAIN ANALYZE " + sql_text)
            rows = [r[0] for r in cur.fetchall()]
            duration = round((time.time() - start_ts) * 1000, 2)
            return ({"explain": rows, "timing_ms": duration}, 200)

        # Execute the actual SQL
        cur.execute(sql_text)

        # If select-like (cursor.description exists)
        columns, data, rowcount = [], [], 0
        if cur.description:
            columns = [desc.name for desc in cur.description]

            # Determine how many rows to fetch safely
            if isinstance(max_rows, int) and max_rows > 0:
                fetch_n = min(max_rows, MAX_ALLOWED_ROWS)
            else:
                # default safety limits: non-admin callers should pass max_rows; caller can set DEFAULT_NON_ADMIN_MAX_ROWS
                fetch_n = DEFAULT_NON_ADMIN_MAX_ROWS

            rows = cur.fetchmany(fetch_n)
            data = [{col: jsonify_value(row[col]) for col in columns} for row in rows]
            rowcount = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else len(data)

        else:
            # DML: commit the transaction effect
            conn.commit()
            rowcount = cur.rowcount
            data = []

        duration = round((time.time() - start_ts) * 1000, 2)
        result = {
            "columns": columns,
            "rows": data,
            "rowcount": rowcount,
            "timing_ms": duration,
            "message": "Query executed successfully",
            # intentionally DON'T echo back full sql in production logs / responses; include on debug only
        }
        return (result, 200)

    except psycopg2.errors.QueryCanceled as e:
        # statement timeout
        try:
            conn.rollback()
        except Exception:
            pass
        return ({"error": "Query timeout", "details": str(e), "timeout_ms": STATEMENT_TIMEOUT_MS}, 408)

    except psycopg2.Error as e:
        try:
            conn.rollback()
        except Exception:
            pass
        # Provide sanitized error to user
        pg_err = getattr(e, "pgerror", None) or str(e)
        return ({"error": "Database error", "details": pg_err}, 400)

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("Unexpected error executing SQL")
        return ({"error": "Internal server error", "details": str(e)}, 500)

    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        # close connection (get_connection might produce a pooled or raw conn; adapt if pooling)
        try:
            conn.close()
        except Exception:
            pass



# ------------------ ROUTE ------------------
run_sql_bp = Blueprint("sql", __name__, url_prefix="/api/sql")

@run_sql_bp.route("/execute", methods=["POST"])
@require_auth
def execute_sql():
    """
    POST payload: { "sql": "...", "user_id": 1, "max_rows": 1000, "explain": false }
    """
    payload = request.get_json() or {}
    sql_text = payload.get("sql")
    user_id = payload.get("user_id")
    max_rows = payload.get("max_rows", None)
    explain = bool(payload.get("explain", False))

    # basic validation
    if "sql" not in payload:
        return jsonify({"error": "Use field 'sql'"}), 400
    if not sql_text or not isinstance(sql_text, str):
        return jsonify({"error": "A valid SQL string is required"}), 400
    if not user_id:
        return jsonify({"error": "Missing user_id (authorization)"}), 400

    # fetch user
    user = User.query.filter_by(id=user_id).first()
    if not user:
        return jsonify({"error": "User not found / unauthorized"}), 401

    is_admin = user.role in ("admin", "superadmin")
    is_superadmin = user.role == "superadmin"

    # Normalize and inspect SQL
    normalized = normalize_sql(sql_text)
    first_kw = get_first_keyword(sql_text) or ""

    # Block multi-statements for non-admin; even admins can be restricted if you prefer
    if has_multiple_statements(sql_text) and not is_admin:
        return jsonify({"error": "Multiple statements are not allowed"}), 403

    # Block dangerous keywords presence for non-superadmin
    if not is_superadmin:
        blocked_kw = contains_blocked_keyword(sql_text)
        if blocked_kw:
            return jsonify({"error": f"Command '{blocked_kw}' is not allowed for your role"}), 403

    # If non-admin: only allow SELECT or EXPLAIN (explain handled separately)
    if not is_superadmin:
        first = first_kw.strip().upper()
        if first not in ("SELECT", "WITH", "EXPLAIN"):
            return jsonify({"error": "Only SELECT/EXPLAIN queries are allowed for your role"}), 403

        # Disallow any reference to excluded tables
        if contains_excluded_table(sql_text):
            return jsonify({"error": "Operation on a protected table is not allowed"}), 403

    # Validate max_rows param
    if isinstance(max_rows, str) and max_rows.isdigit():
        max_rows = int(max_rows)
    elif isinstance(max_rows, int) and max_rows > 0:
        max_rows = int(max_rows)
    else:
        max_rows = None

    # safety upper bound
    if max_rows is not None and max_rows > MAX_ALLOWED_ROWS:
        return jsonify({"error": f"max_rows too large (>{MAX_ALLOWED_ROWS})", "hint": "Use pagination"}), 400

    # Connect to database
    conn = None
    try:
        conn = get_connection()
    except Exception as e:
        logger.exception("DB connection failed")
        return jsonify({"error": "DB connection error", "details": str(e)}), 500

    # For non-admins, enforce read_only and a safe fetch limit
    read_only = not is_admin

    # If non-admin and no max_rows provided, we will use safe default
    if read_only and (max_rows is None):
        max_rows = DEFAULT_NON_ADMIN_MAX_ROWS

    # Execute and return result
    result, status = start_execute_sql(conn, sql_text, max_rows=max_rows, explain=explain, read_only=read_only)

    # Audit logging (do NOT log full SQL in prod or strip secrets)
    try:
        logger.info("SQL_EXEC user_id=%s role=%s first_kw=%s status=%s rowcount=%s time_ms=%s",
                    user_id, user.role, first_kw, status, result.get("rowcount"), result.get("timing_ms"))
    except Exception:
        pass

    return jsonify(result), status
