# auth.py
from __future__ import annotations
import os
import secrets
import time
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict

import jwt
from flask import request, jsonify, g

from utils.config import config
from utils.models import User, RefreshToken, db
from utils.logger import get_logger

from functools import wraps

from utils.hasher_uitls import hash_token

logger = get_logger(__name__, level="INFO")

# --------------------
# Configuration defaults (use env/config)
# --------------------
JWT_SECRET = getattr(config, "JWT_SECRET", os.getenv("JWT_SECRET", "change_me"))
JWT_ALGORITHM = getattr(config, "JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRES_MINUTES = int(getattr(config, "ACCESS_TOKEN_EXPIRES_MINUTES", 15))
REFRESH_TOKEN_EXPIRES_DAYS = int(getattr(config, "REFRESH_TOKEN_EXPIRES_DAYS", 30))

# Rate limit for refresh endpoint (simple in-memory)
_REFRESH_RATE_LIMIT_MAX = int(getattr(config, "REFRESH_RATE_LIMIT_MAX", 10))
_REFRESH_RATE_LIMIT_WINDOW_SECONDS = int(getattr(config, "REFRESH_RATE_LIMIT_WINDOW_SECONDS", 60))
_rate_limit_store: Dict[str, Tuple[int, int]] = {}  # client_id -> (count, first_ts)



# --------------------
# Helpers
# --------------------

def println(msg:str):
    return logger.debug(f"\n\n{msg}\n\n")

def _now_ts() -> int:
    return int(datetime.utcnow().timestamp())


def _generate_raw_refresh_token(nbytes: int = 64) -> str:
    """Generate a secure URL-safe refresh token (raw)."""
    return secrets.token_urlsafe(nbytes)

def create_access_token(payload: dict, expires_minutes: Optional[int] = None) -> Tuple[str, int]:
    """Return (jwt_string, expires_at_epoch_seconds)."""
    expires_minutes = expires_minutes or ACCESS_TOKEN_EXPIRES_MINUTES
    now = datetime.utcnow()
    exp = now + timedelta(minutes=expires_minutes)
    p = payload.copy()
    p.update({"iat": int(now.timestamp()), "exp": int(exp.timestamp())})
    token = jwt.encode(p, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token, int(exp.timestamp())

def create_refresh_token_and_hashed(expires_days: Optional[int] = None) -> Tuple[str, str, datetime]:
    expires_days = expires_days or REFRESH_TOKEN_EXPIRES_DAYS
    raw = _generate_raw_refresh_token()
    hashed = hash_token(raw)
    expires_at = datetime.utcnow() + timedelta(days=expires_days)
    return raw, hashed, expires_at

def check_rate_limit(client_id: str) -> bool:
    """Simple sliding window per-client rate limit (in-memory)."""
    now = int(time.time())
    entry = _rate_limit_store.get(client_id)
    if not entry:
        _rate_limit_store[client_id] = (1, now)
        return True
    count, first_ts = entry
    if now - first_ts > _REFRESH_RATE_LIMIT_WINDOW_SECONDS:
        _rate_limit_store[client_id] = (1, now)
        return True
    if count >= _REFRESH_RATE_LIMIT_MAX:
        return False
    _rate_limit_store[client_id] = (count + 1, first_ts)
    return True

# Utilities: DB helpers (SQLAlchemy)
def db_save_refresh_token(username: str, hashed_token: str, expires_at: datetime) -> RefreshToken:
    rt = RefreshToken(username=username, token=hashed_token, issued_at=datetime.utcnow(), expires_at=expires_at, revoked=False)
    db.session.add(rt)
    db.session.commit()
    return rt

def db_get_refresh_token_by_hashed(hashed_token: str) -> Optional[RefreshToken]:
    return RefreshToken.query.filter_by(token=hashed_token).first()

def db_revoke_refresh_token(rt_obj: RefreshToken) -> None:
    rt_obj.revoked = True
    db.session.add(rt_obj)
    db.session.commit()

# Decorator to protect routes (uses JWT access token)
def require_auth(f=None, *, roles: Optional[list[str]] = None):
    """ Usage: @require_auth -> any logged-in user | @require_auth(roles=["admin"]) -> only roles allowed"""
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            # 1. Read Authorization header
            auth_header = request.headers.get("Authorization", "")
            if not auth_header.startswith("Bearer "):
                return jsonify({"error": "Missing Authorization header"}), 401

            token = auth_header.split(" ", 1)[1]

             # 2. Decode JWT
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            except jwt.ExpiredSignatureError:
                return jsonify({"error": "Access token expired"}), 401
            except jwt.InvalidTokenError:
                return jsonify({"error": "Invalid access token"}), 401

            username = payload.get("username")
            if not username:
                return jsonify({"error": "Invalid token payload"}), 401

            # 3. Validate user exists in PostgreSQL
            user: Optional[User] = User.query.filter_by(username=username).first()
            if not user:
                return jsonify({"error": "User not found"}), 401

            # 4. Validate role if required
            if roles and user.role not in roles:
                return jsonify({"error": "Forbidden"}), 403

            # 5. Attach user context to g
            g.current_user = user.to_payload_dict()

            return func(*args, **kwargs)
        return wrapped
    return decorator if f is None else decorator(f)

