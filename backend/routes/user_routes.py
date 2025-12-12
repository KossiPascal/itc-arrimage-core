# backend/query_routes.py
from flask import Blueprint, request, jsonify, g
from utils.auth import require_auth
from utils.hasher_uitls import verify_password, hash_password
from utils.db import get_connection
from utils.models import db, User

from utils.logger import get_logger
logger = get_logger(__name__)

user_bp = Blueprint("user", __name__, url_prefix="/api/user")

ADMIN_ROLES = ("admin","superadmin")

# GET ALL USERS
@user_bp.get("/all")
@require_auth
def get_users():
    """Return all users (admin only)."""
    current = g.get("current_user")

    if not current:
        return jsonify({"error": "User not found"}), 404

    if current["role"] not in ADMIN_ROLES:
        return jsonify({"error": "Only admin can view users"}), 403

    if current["role"] == "superadmin":
        users = User.query.all()
    else:
        users = User.query.filter(User.role != "superadmin").all()
        
    result = [u.to_dict_safe() for u in users]
    return jsonify(result), 200


# REGISTER USER (ADMIN ONLY)
@user_bp.post("/register")
@require_auth
def register():
    """Create a new user (admin only)."""
    current = g.get("current_user")

    if not current:
        return jsonify({"error": "User not found"}), 404

    if current["role"] not in ADMIN_ROLES:
        return jsonify({"error": "Only admin can create new users"}), 403

    data = request.get_json(silent=True) or {}
    
    fullname = data.get("fullname")
    username = data.get("username")
    password = data.get("password")
    role = data.get("role", "user")

    if not username or not password:
        return jsonify({"error": "username and password are required"}), 400

    if len(password) > 72:
        return jsonify({"error": "Password too long (max 72 chars)"}), 400

    if User.query.filter_by(username=username).first():
        return jsonify({"error": "Username already exists"}), 409

    try:
        psw_hash = hash_password(password)

        user = User(
            username=username,
            fullname=fullname,
            password=psw_hash,
            role=role
        )
        db.session.add(user)
        db.session.commit()

        return jsonify({"message": "User created successfully", "username": username}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


# UPDATE USER (ADMIN ONLY)
@user_bp.put("/update/<int:user_id>")
@require_auth
def update_user(user_id):
    """Update a user's role or profile (admin only)."""
    current = g.get("current_user")
    target = User.query.get_or_404(user_id)

    if not target or not current:
        return jsonify({"error": "User not found"}), 404

    # PROTECTION : On ne touche jamais au superAdmin sauf si superAdmin
    if target["role"] == "superadmin" and current["role"] != "superadmin":
        return jsonify({"error": "Forbidden"}), 403

    if current["role"] not in ADMIN_ROLES:
        return jsonify({"error": "Only admin can update users"}), 403

    data = request.get_json(silent=True) or {}

    # # Admin → peut modifier uniquement le password
    # if current["role"] == "admin" and current.id != target.id:
    #     allowed = {"password"}   # admin NE PEUT PAS modifier role / username
    #     if not set(data).issubset(allowed):
    #         return jsonify({"error": "Admin can only change password"}), 403

    if "role" in data:
        target["role"] = data["role"]

    if "fullname" in data:
        target.fullname = data["fullname"]

    if "password" in data:
        new_password = data["password"]
        pass_len = len(new_password)
        if pass_len > 0:
            if pass_len > 72:
                return jsonify({"error": "New password too long (max 72 chars)"}), 400
            target.password = hash_password(new_password)

    try:
        db.session.commit()
        return jsonify(target.to_dict_safe()), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


# UPDATE PASSWORD
@user_bp.put("/update-password/<int:user_id>")
@require_auth
def update_password(user_id):
    """
    Secure password update.
    Only the user themselves or an admin can update the password.
    """
    conn = None
    try:
        # Current logged user injected by require_auth
        current = g.get("current_user")
        target = User.query.get_or_404(user_id)

        if not target or not current:
            return jsonify({"error": "User not found"}), 404
        
        data = request.get_json(silent=True) or {}
        old_password = data.get("old_password")
        new_password = data.get("new_password")

        if not old_password or not new_password:
            return jsonify({"error": "Both old_password and new_password are required"}), 400

        if len(new_password) > 72:
            return jsonify({"error": "New password too long (max 72 chars)"}), 400

        # Only admin OR the user itself
        if (current["role"] not in ADMIN_ROLES) and (current.id != target.id):
            return jsonify({"error": "Unauthorized: You cannot modify another user's password."}), 403

        # Verify old password
        if not verify_password(old_password, target.password):
            return jsonify({"error": "Old password is incorrect"}), 401

        # Hash new password
        target.password = hash_password(new_password)

        db.session.commit()

        return jsonify({
            "message": "Password updated successfully",
            "user": current
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

    finally:
        if conn:
            conn.close()


# ADMIN UPDATE PASSWORD
@user_bp.put("/admin-update-password/<int:user_id>")
@require_auth
def admin_update_password(user_id):
    """
    Secure password update.
    Only the user themselves or an admin can update the password.
    """
    conn = None
    try:
        data = request.get_json(silent=True) or {}

        new_password = data.get("new_password")

        if not new_password:
            return jsonify({"error": "new_password is required"}), 400

        if len(new_password) > 72:
            return jsonify({"error": "New password too long (max 72 chars)"}), 400

        # Current logged user injected by require_auth
        user = g.get("current_user")

        # Only admin OR the user itself
        if user["role"]  not in ADMIN_ROLES:
            return jsonify({"error": "Unauthorized: You cannot modify another user's password."}), 403

        # DB Connection (raw SQL)
        conn = get_connection()
        cur = conn.cursor()

        # Retrieve target user
        cur.execute("SELECT id, username, password, role FROM users WHERE id=%s", (user_id,))
        user = cur.fetchone()

        if not user:
            return jsonify({"error": "User not found"}), 404

        # Hash new password
        new_hash = hash_password(new_password)

        cur.execute("""
            UPDATE users 
            SET password = %s
            WHERE id = %s
            RETURNING id, username, role;
        """, (new_hash, user_id))

        updated_user = cur.fetchone()
        conn.commit()

        return jsonify({
            "message": "Password updated successfully",
            "user": updated_user
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

    finally:
        if conn:
            conn.close()


# DELETE USER (ADMIN ONLY)
@user_bp.delete("/delete/<int:user_id>")
@require_auth
def delete_user(user_id):
    """Delete a user (admin only)."""
    user = g.get("current_user")
    if user["role"] not in ADMIN_ROLES:
        return jsonify({"error": "Only admin can delete users"}), 403

    user = User.query.get_or_404(user_id)

    try:
        db.session.delete(user)
        db.session.commit()
        return jsonify({"message": "User deleted"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500
