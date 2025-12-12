# models.py
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from utils.config import config
from utils.hasher_uitls import hash_password
from utils.logger import get_logger

logger = get_logger(__name__)
db = SQLAlchemy()


class User(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    fullname = db.Column(db.String(255), nullable=True)
    username = db.Column(db.String(150), unique=True, nullable=False, index=True)
    password = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(50), default="user", nullable=False)  # simple RBAC
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    refresh_tokens = db.relationship(
        "RefreshToken",
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="dynamic"
    )

    def __init__(self, username, fullname, password, role="user"):
        self.username = username
        self.fullname = fullname
        self.password = password
        self.role = role

    # --------------- FIX: INSTANCE METHOD ---------------
    def to_dict_safe(self):
        return {
            "id": self.id,
            "username": self.username,
            "fullname": self.fullname,
            "role": self.role,
            "isAdmin": self.role in ("admin","superadmin"),
            "isSuperAdmin": self.role == "superadmin",
            "created_at": self.created_at.isoformat() if self.created_at else None
            # "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
    
    def to_payload_dict(self):
        return {
            "id": self.id,
            "fullname": self.fullname,
            "username": self.username, 
            "role": self.role
        }

    @classmethod
    def create_default_admin(cls):
        """
        Crée l'admin par défaut si inexistant.
        Retourne l'instance existante ou nouvellement créée.
        Protège contre les erreurs de concurrence et IntegrityError.
        """
        DFA = config.DEFAULT_ADMIN
        username = DFA.get("username")
        password = DFA.get("password")
        fullname = DFA.get("fullname")
        role = DFA.get("role", "superadmin")

        if not username or not password:
            logger.error("DEFAULT_ADMIN configuration incomplete.")
            raise ValueError("DEFAULT_ADMIN must have at least 'username' and 'password'.")

        # Vérifie si l'admin existe déjà
        existing = cls.query.filter_by(username=username).first()
        if existing:
            logger.info("Default admin already exists.")
            return existing

        # Prépare l'admin
        psw_hash = hash_password(password)
        admin = cls(
            username=username,
            fullname=fullname,
            password=psw_hash,
            role=role
        )
        db.session.add(admin)

        try:
            db.session.commit()
            logger.info("Default admin created successfully.")
            return admin
        except IntegrityError:
            db.session.rollback()
            # Vérifie à nouveau si l'admin existe (race condition)
            existing = cls.query.filter_by(username=username).first()
            if existing:
                logger.warning("Default admin already exists (handled race condition).")
                return existing
            else:
                logger.error("Failed to create default admin due to IntegrityError.")
                raise


class RefreshToken(db.Model):
    __tablename__ = "refresh_tokens"

    id = db.Column(db.Integer, primary_key=True)
    token = db.Column(db.String(255), unique=True, nullable=False)  # store hashed token (sha256 hex)
    issued_at = db.Column(db.DateTime, default=datetime.utcnow)
    expires_at = db.Column(db.DateTime, nullable=False)
    revoked = db.Column(db.Boolean, default=False, nullable=False)

    username = db.Column(db.String(150), db.ForeignKey("users.username"), nullable=False)
    user = db.relationship("User", back_populates="refresh_tokens")

    def __init__(self, username, token, issued_at, expires_at, revoked):
        self.username = username
        self.token = token
        self.issued_at = issued_at
        self.expires_at = expires_at
        self.revoked = revoked
