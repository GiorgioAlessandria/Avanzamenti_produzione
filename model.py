# models.py
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
import uuid
import json

db = SQLAlchemy()

# --- Tabelle di associazione ---

user_roles = db.Table(
    "user_roles",
    db.Column("user_id", db.Integer, db.ForeignKey(
        "users.id", ondelete="CASCADE"), primary_key=True),
    db.Column("role_id", db.Integer, db.ForeignKey(
        "roles.id", ondelete="CASCADE"), primary_key=True),
)

roles_permission = db.Table(
    "roles_permission",
    db.Column("role_id", db.Integer, db.ForeignKey(
        "roles.id", ondelete="CASCADE"), primary_key=True),
    db.Column("permission_id", db.Integer, db.ForeignKey(
        "permissions.id", ondelete="CASCADE"), primary_key=True),
)

user_reparti = db.Table(
    "user_reparti",
    db.Column("user_id", db.Integer, db.ForeignKey(
        "users.id", ondelete="CASCADE"), primary_key=True),
    db.Column("reparto_id", db.Integer, db.ForeignKey(
        "reparti.id", ondelete="CASCADE"), primary_key=True),
)

# --- RBAC ---


class Role(db.Model):
    __tablename__ = "roles"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, unique=True, nullable=False)
    description = db.Column(db.String)

    permissions = db.relationship(
        "Permission",
        secondary=roles_permission,
        backref=db.backref("roles", lazy="dynamic"),
        lazy="dinamic",
    )

    def add_permission(self, permission: "Permission"):
        if not self.permissions.filter_by(id=permission.id).first():
            self.permissions.append(permission)

    def remove_permission(self, permission: "Permission"):
        if self.permissions.filter_by(id=permission.id).first():
            self.permissions.remove(permission)

    def has_permission(self, code: str) -> bool:
        return self.permissions.filter_by(code=code).first() is not None

    def __repr__(self):
        return f"<Role {self.name}>"


class Permission(db.Model):
    __tablename__ = "permissions"

    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String, unique=True, nullable=False)
    description = db.Column(db.String)

    def __repr__(self):
        return f"<Permission {self.code}>"


class Reparto(db.Model):
    __tablename__ = "reparti"

    id = db.Column(db.Integer, primary_key=True)
    codice = db.Column(db.String, unique=True,
                       nullable=False)  # es. "MONTAGGIO"
    descrizione = db.Column(db.String)

    users = db.relationship(
        "User",
        secondary=user_reparti,
        back_populates="reparti",
    )

    def __repr__(self):
        return f"<Reparto {self.codice}>"


class User(UserMixin, db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    public_id = db.Column(
        db.String,
        unique=True,
        nullable=False,
        default=lambda: str(uuid.uuid4()),
    )
    username = db.Column(db.String, unique=True, nullable=False)
    active = db.Column(db.Boolean, nullable=False, default=True)

    # preference TEXT nel DB: lo uso come JSON
    preference = db.Column(db.Text)

    roles = db.relationship(
        "Role",
        secondary=user_roles,
        backref=db.backref("users", lazy="dynamic"),
        lazy="joined",
    )

    reparti = db.relationship(
        "Reparto",
        secondary=user_reparti,
        back_populates="users",
        lazy="joined",
    )

    # --- helper per preferences (JSON) ---

    @property
    def preferences(self) -> dict:
        if not self.preference:
            return {}
        try:
            return json.loads(self.preference)
        except json.JSONDecodeError:
            return {}

    @preferences.setter
    def preferences(self, value: dict):
        self.preference = json.dumps(value or {})

    def get_pref(self, key, default=None):
        return self.preferences.get(key, default)

    def set_pref(self, key, value):
        prefs = self.preferences
        prefs[key] = value
        self.preferences = prefs

    # --- helper RBAC ---

    def has_role(self, role_name: str) -> bool:
        return any(r.name == role_name for r in self.roles)

    def has_permission(self, code: str) -> bool:
        return any(r.has_permission(code) for r in self.roles)

    def allowed_reparti_ids(self) -> list[int]:
        return [r.id for r in self.reparti]

    def allowed_reparti_codici(self) -> list[str]:
        return [r.codice for r in self.reparti]

    def __repr__(self):
        return f"<User {self.username}>"
