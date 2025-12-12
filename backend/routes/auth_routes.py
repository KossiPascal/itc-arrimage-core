# auth.py
from datetime import datetime
from typing import Optional
from flask import Blueprint, request, jsonify, g, current_app
from utils.config import config
from utils.models import User, RefreshToken
from utils.logger import get_logger
from utils.auth import (
    create_access_token,
    create_refresh_token_and_hashed,
    db_save_refresh_token,
    check_rate_limit,
    db_get_refresh_token_by_hashed,
    db_revoke_refresh_token,
    require_auth,
)

from utils.hasher_uitls import verify_password, hash_token

logger = get_logger(__name__, level="INFO")


# Blueprint
auth_bp = Blueprint("auth", __name__, url_prefix="/api/auth")

# --------------------
# Routes
# --------------------

@auth_bp.post("/login")
def login():
    """
    Authenticate user and return access + refresh tokens.
    Body JSON: { "username": "...", "password": "..." }
    """
    try:
        data = request.get_json(silent=True) or {}
        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return jsonify({"error": "username and password required"}), 400

        user: Optional[User] = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({"error": "invalid credentials"}), 401

        # verify_password implementation: expect method on User or util - try both
        verify_ok = False
        try:
            verify_ok = verify_password(password, user.password)
        except Exception:
            # Fallback: if User has verify_password method
            if hasattr(user, "verify_password"):
                verify_ok = user.verify_password(password)
            else:
                verify_ok = False

        if not verify_ok:
            return jsonify({"error": "invalid credentials"}), 401

        # Create access token
        payload = user.to_payload_dict()
        access_token, access_exp = create_access_token(payload)

        # Create refresh token (raw + hashed) and persist hashed
        raw_refresh, hashed_refresh, expires_at = create_refresh_token_and_hashed()
        db_save_refresh_token(user.username, hashed_refresh, expires_at)

        response = {
            "access_token": access_token,
            "access_token_expires_at": access_exp,
            "refresh_token": raw_refresh,  # frontend can store it or use cookie
            "refresh_token_expires_at": int(expires_at.timestamp()),
            "user": user.to_dict_safe()
        }

        # Optionally set HttpOnly cookie (recommended)
        set_cookie = bool(request.args.get("cookie", False)) or current_app.config.get("AUTH_SET_COOKIE", False)
        if set_cookie:
            secure_flag = bool(getattr(config, "USE_SSL", False))
            current_response = jsonify(response)
            current_response.set_cookie(
                "refresh_token",
                raw_refresh,
                httponly=True,
                secure=secure_flag,
                samesite="Lax",
                expires=expires_at
            )
            return current_response, 200

        return jsonify(response), 200

    except Exception as e:
        logger.exception("Login error")
        return jsonify({"error": "Login failed", "details": str(e)}), 500


@auth_bp.post("/refresh")
# @require_auth
def refresh():
    """
    Rotate refresh token and return new access + refresh token.
    Accepts:
      - JSON body { "refresh_token": "..." }
      - Or cookie "refresh_token" (HttpOnly)
    """
    client_id = request.remote_addr or "unknown"
    if not check_rate_limit(client_id):
        return jsonify({"error": "Too many attempts"}), 429

    data = request.get_json(silent=True) or {}
    incoming = data.get("refresh_token") or request.cookies.get("refresh_token")

    if not incoming:
        return jsonify({"error": "Missing refresh_token"}), 400

    try:
        hashed_incoming = hash_token(incoming)
        rt = db_get_refresh_token_by_hashed(hashed_incoming)

        # backward compatibility: maybe raw tokens stored in DB (less secure)
        if not rt:
            rt = RefreshToken.query.filter_by(token=incoming).first()

        if not rt:
            return jsonify({"error": "Invalid refresh token"}), 401

        if rt.revoked:
            logger.warning("Attempt to use revoked refresh token for user=%s", rt.username)
            return jsonify({"error": "Refresh token revoked"}), 401

        if rt.expires_at and rt.expires_at < datetime.utcnow():
            return jsonify({"error": "Refresh token expired"}), 401

        # load user
        user: Optional[User] = User.query.filter_by(username=rt.username).first()
        if not user:
            return jsonify({"error": "User not found"}), 401

        # revoke current token (single-use)
        try:
            db_revoke_refresh_token(rt)
        except Exception:
            logger.exception("Failed to revoke refresh token")
            # continue — do not leak internal error, but fail
            return jsonify({"error": "Internal error"}), 500

        # issue new refresh token and persist hashed
        raw_new, hashed_new, expires_at = create_refresh_token_and_hashed()
        try:
            db_save_refresh_token(user.username, hashed_new, expires_at)
        except Exception:
            logger.exception("Failed to persist new refresh token")
            return jsonify({"error": "Internal error"}), 500

        # create new access token
        payload = user.to_payload_dict()
        access_token, access_exp = create_access_token(payload)

        response = {
            "access_token": access_token,
            "access_token_expires_at": access_exp,
            "refresh_token": raw_new,
            "refresh_token_expires_at": int(expires_at.timestamp()),
        }

        # Optionally set cookie
        set_cookie = bool(request.args.get("cookie", False)) or current_app.config.get("AUTH_SET_COOKIE", False)
        if set_cookie:
            secure_flag = bool(getattr(config, "USE_SSL", False))
            resp = jsonify(response)
            resp.set_cookie(
                "refresh_token",
                raw_new,
                httponly=True,
                secure=secure_flag,
                samesite="Lax",
                expires=expires_at
            )
            return resp, 200

        return jsonify(response), 200

    except Exception as e:
        logger.exception("Unhandled error during refresh")
        return jsonify({"error": "Internal error", "details": str(e)}), 500


@auth_bp.post("/logout")
@require_auth
def logout():
    """Invalidate a given refresh token (body) for current user."""
    try:
        data = request.get_json(silent=True) or {}
        incoming = data.get("refresh_token")
        if not incoming:
            return jsonify({"error": "missing refresh_token"}), 400

        hashed = hash_token(incoming)
        rt = db_get_refresh_token_by_hashed(hashed)
        if not rt:
            # maybe raw stored
            rt = RefreshToken.query.filter_by(token=incoming).first()
            if not rt:
                return jsonify({"error": "refresh token not found"}), 404

        if rt.username != g.current_user["username"]:
            return jsonify({"error": "forbidden"}), 403

        db_revoke_refresh_token(rt)
        return jsonify({"message": "logged out successfully"}), 200

    except Exception as e:
        logger.exception("Logout error")
        return jsonify({"error": "Logout failed", "details": str(e)}), 500


@auth_bp.get("/me")
@require_auth
def me():
    """Return current user info (from require_auth -> g.current_user)."""
    try:
        current = g.get("current_user")
        if not current:
            return jsonify({"error": "Unauthorized"}), 401
        return jsonify({"user": current}), 200
    except Exception as e:
        logger.exception("Failed to return current user")
        return jsonify({"error": "Failed", "details": str(e)}), 500
    


    