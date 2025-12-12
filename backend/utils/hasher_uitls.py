
import hashlib
from passlib.hash import sha256_crypt


def hash_token(token: str) -> str:
    """Deterministic SHA256 hex digest for storing refresh tokens."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()

def hash_password(password: str) -> str:
    if not password:
        raise ValueError("Password cannot be empty")
    return sha256_crypt.hash(password)

def verify_password(password: str, password_hash: str) -> bool:
    if not password or not password_hash:
        return False
    return sha256_crypt.verify(password, password_hash)
