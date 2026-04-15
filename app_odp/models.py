from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo
from flask_login import UserMixin
import uuid
import json
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_
from sqlalchemy.orm import foreign
import hashlib
from werkzeug.security import generate_password_hash, check_password_hash
import re

db = SQLAlchemy()

# --- Tabelle di associazione ---
roles_famiglia = db.Table(
    "roles_famiglia",
    db.Column(
        "roles_id",
        db.Integer,
        db.ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "famiglia_id",
        db.Integer,
        db.ForeignKey("famiglia.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)

roles_ineritance = db.Table(
    "roles_ineritance",
    db.Column("role_id", db.Integer, db.ForeignKey("roles.id"), primary_key=True),
    db.Column("included_role", db.Integer, db.ForeignKey("roles.id"), primary_key=True),
)

roles_lavorazioni = db.Table(
    "roles_lavorazioni",
    db.Column(
        "roles_id",
        db.Integer,
        db.ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "lavorazioni_id",
        db.Integer,
        db.ForeignKey("lavorazioni.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)

roles_macrofamiglia = db.Table(
    "roles_macrofamiglia",
    db.Column(
        "roles_id",
        db.Integer,
        db.ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "macrofamiglia_id",
        db.Integer,
        db.ForeignKey("macrofamiglia.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)

roles_magazzini = db.Table(
    "roles_magazzini",
    db.Column(
        "roles_id",
        db.Integer,
        db.ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "magazzini_id",
        db.Integer,
        db.ForeignKey("magazzini.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)

roles_permission = db.Table(
    "roles_permission",
    db.Column(
        "role_id",
        db.Integer,
        db.ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "permission_id",
        db.Integer,
        db.ForeignKey("permissions.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)

roles_reparti = db.Table(
    "roles_reparti",
    db.Column(
        "roles_id",
        db.Integer,
        db.ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "reparto_id",
        db.Integer,
        db.ForeignKey("reparti.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)

roles_risorse = db.Table(
    "roles_risorse",
    db.Column(
        "roles_id",
        db.Integer,
        db.ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "risorse_id",
        db.Integer,
        db.ForeignKey("risorse.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)

user_roles = db.Table(
    "user_roles",
    db.Column(
        "user_id",
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "role_id",
        db.Integer,
        db.ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)

users_lavorazioni = db.Table(
    "users_lavorazioni",
    db.Column(
        "user_id",
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "lavorazioni_id",
        db.Integer,
        db.ForeignKey("lavorazioni.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)

users_risorse = db.Table(
    "users_risorse",
    db.Column(
        "user_id",
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "risorse_id",
        db.Integer,
        db.ForeignKey("risorse.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


roles_manageable_roles = db.Table(
    "roles_manageable_roles",
    db.Column(
        "manager_role_id",
        db.Integer,
        db.ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "managed_role_id",
        db.Integer,
        db.ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)

# --- Modelli ---


class Causaliattivita(db.Model):
    __tablename__ = "causaliattivita"

    id = db.Column(db.Integer, primary_key=True)
    DesCausaleAttivita = db.Column(db.Text, nullable=False)
    TipoCausale = db.Column(db.Text, nullable=False)
    CausaleAttivita = db.Column(db.Text, nullable=False)
    CodCategoriaAttivita = db.Column(db.Text, nullable=False)

    def __repr__(self):
        return f"<Causaliattivita {self.__dict__}>"


class Famiglia(db.Model):
    __tablename__ = "famiglia"

    id = db.Column(db.Integer, primary_key=True)
    Codice = db.Column(db.Text, nullable=False)
    Descrizione = db.Column(db.Text, nullable=False)

    def __repr__(self):
        return f"<Famiglia {self.__dict__}>"


class GiacenzaMateriale(db.Model):
    __tablename__ = "giacenza_materiale"

    CodArt = db.Column(db.Text)
    CodMag = db.Column(db.Text)
    Giacenza = db.Column(db.Text, nullable=False)
    RifLottoAlfa = db.Column(db.Text, nullable=False)
    DesArt = db.Column(db.Text, nullable=False)

    __table_args__ = (db.PrimaryKeyConstraint("CodArt", "CodMag"),)

    def __repr__(self):
        return f"<GiacenzaMateriale {self.__dict__}>"


class InputOdpLog(db.Model):
    __bind_key__ = "log"
    __tablename__ = "input_odp_log"

    log_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    logged_at = db.Column(
        db.Text,
        nullable=False,
        default=lambda: datetime.now(ZoneInfo("Europe/Rome")).isoformat(
            timespec="seconds"
        ),
    )

    OperationGroupId = db.Column(db.Text, nullable=False, index=True)

    # chiavi ordine
    IdDocumento = db.Column(db.Text, nullable=False, index=True)
    IdRiga = db.Column(db.Text, nullable=False, index=True)
    RifRegistraz = db.Column(db.Text)

    # snapshot ERP/input
    CodArt = db.Column(db.Text)
    DesArt = db.Column(db.Text)
    Quantita = db.Column(db.Text)
    NumFase = db.Column(db.Text)
    CodLavorazione = db.Column(db.Text)
    CodRisorsaProd = db.Column(db.Text)
    DataInizioSched = db.Column(db.Text)
    DataFineSched = db.Column(db.Text)
    GestioneLotto = db.Column(db.Text)
    GestioneMatricola = db.Column(db.Text)
    DistintaMateriale = db.Column(db.Text)
    CodMatricola = db.Column(db.Text)
    StatoRiga = db.Column(db.Text)
    CodFamiglia = db.Column(db.Text)
    CodMacrofamiglia = db.Column(db.Text)
    CodMagPrincipale = db.Column(db.Text)
    CodReparto = db.Column(db.Text)
    TempoPrevistoLavoraz = db.Column(db.Text)
    CodClassifTecnica = db.Column(db.Text)
    CodTipoDoc = db.Column(db.Text)

    # stato ordine/runtime al momento della chiusura
    FaseAttiva = db.Column(db.Text)
    QtyDaLavorare = db.Column(db.Text)
    RisorsaAttiva = db.Column(db.Text)
    LavorazioneAttiva = db.Column(db.Text)
    AttrezzaggioAttivo = db.Column(db.Text)
    RifOrdinePrinc = db.Column(db.Text)
    Note = db.Column(db.Text)

    # dati specifici della consuntivazione
    FaseConsuntivata = db.Column(db.Text)
    QuantitaConforme = db.Column(db.Text)
    QuantitaNonConforme = db.Column(db.Text)
    TempoFunzionamentoFinale = db.Column(db.Text)
    TempoNonFunzionamentoMinuti = db.Column(db.Text)
    TempoNonFunzionamentoSecondi = db.Column(db.Text)
    ChiusuraParziale = db.Column(db.Text)
    NoteChiusura = db.Column(db.Text)

    # delta pre/post operazione
    StatoOrdinePre = db.Column(db.Text)
    StatoOrdinePost = db.Column(db.Text)
    QtyDaLavorarePre = db.Column(db.Text)
    QtyDaLavorarePost = db.Column(db.Text)

    ClosedBy = db.Column(db.Text)
    ClosedAt = db.Column(db.Text, nullable=False, index=True)
    VarianteArt = db.Column(db.Text)

    __table_args__ = (db.Index("ix_input_odp_log_doc_riga", "IdDocumento", "IdRiga"),)

    def __repr__(self):
        return f"<InputOdpLog {self.log_id} {self.IdDocumento}/{self.IdRiga}>"


class OdpRuntimeLog(db.Model):
    __bind_key__ = "log"
    __tablename__ = "odp_runtime_log"

    log_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    logged_at = db.Column(
        db.Text,
        nullable=False,
        default=lambda: datetime.now(ZoneInfo("Europe/Rome")).isoformat(
            timespec="seconds"
        ),
    )

    OperationGroupId = db.Column(db.Text, index=True)
    EventSequence = db.Column(db.Integer)

    Topic = db.Column(db.Text, index=True)  # ex ChangeEvent.topic
    Scope = db.Column(db.Text, index=True)  # ex ChangeEvent.scope
    CodArt = db.Column(db.Text, index=True)
    CodReparto = db.Column(db.Text, index=True)
    PayloadJson = db.Column(db.Text)  # extra leggibile/debug

    IdDocumento = db.Column(db.Text, nullable=False, index=True)
    IdRiga = db.Column(db.Text, nullable=False, index=True)
    RifRegistraz = db.Column(db.Text)

    Azione = db.Column(db.Text, nullable=False, index=True)
    Motivo = db.Column(db.Text)
    UtenteOperazione = db.Column(db.Text)
    EventAt = db.Column(db.Text, nullable=False, index=True)

    StatoOdpPre = db.Column(db.Text)
    StatoOdpPost = db.Column(db.Text)

    StatoOrdinePre = db.Column(db.Text)
    StatoOrdinePost = db.Column(db.Text)

    FasePre = db.Column(db.Text)
    FasePost = db.Column(db.Text)

    DataInCaricoPre = db.Column(db.Text)
    DataInCaricoPost = db.Column(db.Text)

    DataUltimaAttivazionePre = db.Column(db.Text)
    DataUltimaAttivazionePost = db.Column(db.Text)

    TempoFunzionamentoPre = db.Column(db.Text)
    TempoFunzionamentoPost = db.Column(db.Text)

    ElapsedSeconds = db.Column(db.Text)
    TempoNonFunzionamentoMinuti = db.Column(db.Text)
    TempoNonFunzionamentoSecondi = db.Column(db.Text)

    QtyDaLavorarePre = db.Column(db.Text)
    QtyDaLavorarePost = db.Column(db.Text)

    QuantitaConforme = db.Column(db.Text)
    QuantitaNonConforme = db.Column(db.Text)

    Causale = db.Column(db.Text)
    Note = db.Column(db.Text)
    RifOrdinePrinc = db.Column(db.Text)
    VarianteArt = db.Column(db.Text)
    NumProgrRiga = db.Column(db.Text)

    __table_args__ = (
        db.Index("ix_odp_runtime_log_doc_riga", "IdDocumento", "IdRiga"),
        db.Index("ix_odp_runtime_log_topic", "Topic"),
        db.Index("ix_odp_runtime_log_scope", "Scope"),
    )


class Lavorazioni(db.Model):
    __tablename__ = "lavorazioni"

    id = db.Column(db.Integer, primary_key=True)
    Codice = db.Column(db.Text, nullable=False)
    Descrizione = db.Column(db.Text, nullable=False)

    def __repr__(self):
        return f"<Lavorazioni {self.__dict__}>"


class Macrofamiglia(db.Model):
    __tablename__ = "macrofamiglia"

    id = db.Column(db.Integer, primary_key=True)
    Codice = db.Column(db.Text, nullable=False)
    Descrizione = db.Column(db.Text, nullable=False)

    def __repr__(self):
        return f"<Macrofamiglia {self.__dict__}>"


class Magazzini(db.Model):
    __tablename__ = "magazzini"

    id = db.Column(db.Integer, primary_key=True)
    Codice = db.Column(db.Text, nullable=False)
    Descrizione = db.Column(db.Text, nullable=False)

    def __repr__(self):
        return f"<Magazzini {self.__dict__}>"


class Permissions(db.Model):
    __tablename__ = "permissions"

    id = db.Column(db.Integer, primary_key=True)
    Codice = db.Column(db.Text, nullable=False)
    Descrizione = db.Column(db.Text)

    def __repr__(self):
        return f"<Permissions {self.__dict__}>"


class Reparti(db.Model):
    __tablename__ = "reparti"

    id = db.Column(db.Integer, primary_key=True)
    Codice = db.Column(db.Text, nullable=False)
    Descrizione = db.Column(db.Text)

    def __repr__(self):
        return f"<Reparti {self.__dict__}>"


class Risorse(db.Model):
    __tablename__ = "risorse"

    id = db.Column(db.Integer, primary_key=True)
    Codice = db.Column(db.Text, nullable=False)
    Descrizione = db.Column(db.Text, nullable=False)

    def __repr__(self):
        return f"<Risorse {self.__dict__}>"


class Roles(db.Model):
    __tablename__ = "roles"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text, nullable=False)
    description = db.Column(db.Text)
    permissions = db.relationship(
        "Permissions",
        secondary=roles_permission,
        backref=db.backref("roles", lazy="dynamic"),
        lazy="dynamic",
    )
    reparti = db.relationship(
        "Reparti",
        secondary=roles_reparti,
        lazy="selectin",
    )
    risorse = db.relationship(
        "Risorse",
        secondary=roles_risorse,
        lazy="selectin",
    )
    lavorazioni = db.relationship(
        "Lavorazioni",
        secondary=roles_lavorazioni,
        lazy="selectin",
    )
    magazzini = db.relationship(
        "Magazzini",
        secondary=roles_magazzini,
        lazy="selectin",
    )
    famiglia = db.relationship(
        "Famiglia",
        secondary=roles_famiglia,
        lazy="selectin",
    )
    macrofamiglia = db.relationship(
        "Macrofamiglia",
        secondary=roles_macrofamiglia,
        lazy="selectin",
    )
    included_roles = db.relationship(
        "Roles",
        secondary=roles_ineritance,
        primaryjoin=(id == roles_ineritance.c.role_id),
        secondaryjoin=(id == roles_ineritance.c.included_role),
        lazy="selectin",
        backref=db.backref("included_by", lazy="selectin"),
    )
    manageable_roles = db.relationship(
        "Roles",
        secondary=roles_manageable_roles,
        primaryjoin=(id == roles_manageable_roles.c.manager_role_id),
        secondaryjoin=(id == roles_manageable_roles.c.managed_role_id),
        lazy="selectin",
        backref=db.backref("managed_by_roles", lazy="selectin"),
    )

    def iter_manageable_roles(self):
        seen = set()
        stack = list(self.manageable_roles or [])
        while stack:
            role = stack.pop()
            if role is None or role.id in seen:
                continue
            seen.add(role.id)
            yield role
            stack.extend(getattr(role, "manageable_roles", []) or [])

    def iter_self_and_included(self):
        """Ritorna questo ruolo + tutti i ruoli inclusi (ricorsivo), evitando cicli."""
        seen = set()
        stack = [self]
        while stack:
            r = stack.pop()
            if r is None or r.id in seen:
                continue
            seen.add(r.id)
            yield r
            stack.extend(getattr(r, "included_roles", []) or [])

    @property
    def effective_reparti(self):
        out = {}
        for r in self.iter_self_and_included():
            for rep in r.reparti or []:
                out[rep.id] = rep
        return list(out.values())

    @property
    def effective_risorse(self):
        out = {}
        for r in self.iter_self_and_included():
            for x in r.risorse or []:
                out[x.id] = x
        return list(out.values())

    @property
    def effective_lavorazioni(self):
        out = {}
        for r in self.iter_self_and_included():
            for x in r.lavorazioni or []:
                out[x.id] = x
        return list(out.values())

    @property
    def effective_magazzini(self):
        out = {}
        for r in self.iter_self_and_included():
            for x in r.magazzini or []:
                out[x.id] = x
        return list(out.values())

    @property
    def effective_famiglia(self):
        out = {}
        for r in self.iter_self_and_included():
            for x in r.famiglia or []:
                out[x.id] = x
        return list(out.values())

    @property
    def effective_macrofamiglia(self):
        out = {}
        for r in self.iter_self_and_included():
            for x in r.macrofamiglia or []:
                out[x.id] = x
        return list(out.values())

    def add_permission(self, permission: "Permissions"):
        if not self.permissions.filter_by(id=permission.id).first():
            self.permissions.append(permission)

    def remove_permission(self, permission: "Permissions"):
        if self.permissions.filter_by(id=permission.id).first():
            self.permissions.remove(permission)

    def __repr__(self):
        return f"<Roles {self.__dict__}>"


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

    login_code_lookup = db.Column(db.String(64), unique=True, index=True)
    login_code_hash = db.Column(db.String(255))

    active = db.Column(
        db.Boolean, nullable=False, default=True, server_default=db.text("1")
    )
    preference = db.Column(db.Text)
    genere = db.Column(db.Text)
    RepartoPrinc = db.Column(db.Text)

    roles = db.relationship(
        "Roles",
        secondary=user_roles,
        backref=db.backref("users", lazy="dynamic"),
        lazy="joined",
    )
    lavorazioni = db.relationship(
        "Lavorazioni",
        secondary=users_lavorazioni,
        lazy="joined",
    )
    risorse = db.relationship(
        "Risorse",
        secondary=users_risorse,
        lazy="joined",
    )

    @staticmethod
    def validate_login_code(raw_code: str) -> str:
        code = User._normalize_login_code(raw_code)

        if not code:
            raise ValueError("Il login_code è obbligatorio.")

        if len(code) < 12:
            raise ValueError("Il login_code deve contenere almeno 12 caratteri.")

        if not re.fullmatch(r"[A-Z0-9]+", code):
            raise ValueError(
                "Il login_code deve essere alfanumerico e contenere solo lettere e numeri."
            )

        return code

    @staticmethod
    def _normalize_login_code(raw_code: str) -> str:
        return (raw_code or "").strip().upper()

    def set_login_code(self, raw_code: str) -> None:
        code = self._normalize_login_code(raw_code)
        if not code:
            raise ValueError("Il codice accesso non può essere vuoto.")

        self.login_code_lookup = hashlib.sha256(code.encode("utf-8")).hexdigest()
        self.login_code_hash = generate_password_hash(code)

    def check_login_code(self, raw_code: str) -> bool:
        code = self._normalize_login_code(raw_code)
        if not code or not self.login_code_hash:
            return False
        return check_password_hash(self.login_code_hash, code)

    @property
    def is_active(self):
        return bool(self.active)

    @property
    def is_active(self):
        return bool(self.active)

    # --- helper per preferences (JSON) ---
    @property
    def manageable_roles(self):
        out = {}
        for role in self._iter_roles(include_inherited=True):
            for managed in getattr(role, "iter_manageable_roles", lambda: [])():
                out[managed.id] = managed
        return list(out.values())

    @property
    def manageable_role_ids(self) -> set[int]:
        return {r.id for r in self.manageable_roles}

    def has_management_scope(self) -> bool:
        return bool(self.manageable_role_ids)

    def can_manage_role(self, role) -> bool:
        role_id = role.id if hasattr(role, "id") else int(role)
        return role_id in self.manageable_role_ids

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

    # --- helper policy ---

    def has_role(self, role_name: str) -> bool:
        return any(r.name == role_name for r in self.roles)

    def _iter_roles(self, include_inherited: bool = True):
        """Itera i ruoli dell'utente; opzionalmente include ruoli ereditati (included_roles)."""
        seen = set()
        stack = list(self.roles or [])
        while stack:
            role = stack.pop()
            if role is None or role.id in seen:
                continue
            seen.add(role.id)
            yield role
            if include_inherited:
                stack.extend(getattr(role, "included_roles", []) or [])

    def has_permission(self, permission) -> bool:
        if permission is None:
            return False
        if isinstance(permission, Permissions):
            perm_id = permission.id
            perm_code = permission.Codice
        else:
            perm_code = str(permission).strip()
            try:
                perm_id = int(perm_code)
            except (TypeError, ValueError):
                perm_id = None

        for role in self._iter_roles(include_inherited=True):
            q = role.permissions  # lazy="dynamic" => query
            if perm_id is not None and q.filter_by(id=perm_id).first():
                return True
            if perm_code and q.filter_by(Codice=perm_code).first():
                return True

        return False

    def __repr__(self):
        return f"<Users {self.__dict__}>"


class GiacenzaLotti(db.Model):
    __tablename__ = "giacenza_lotti"

    CodArt = db.Column(db.Text, primary_key=True)
    RifLottoAlfa = db.Column(db.Text, primary_key=True)
    CodMag = db.Column(db.Text, primary_key=True)
    Giacenza = db.Column(db.Text, nullable=False)


class TipologieStato(db.Model):
    __tablename__ = "tipologie_stato"

    id = db.Column(db.Integer, primary_key=True)
    tipo = db.Column(db.Integer)

    def __repr__(self):
        return f"<TipologieStato {self.__dict__}>"


class InputOdp(db.Model):
    __tablename__ = "input_odp"

    IdDocumento = db.Column(db.Text)
    IdRiga = db.Column(db.Text)
    RifRegistraz = db.Column(db.Text)
    CodArt = db.Column(db.Text)
    DesArt = db.Column(db.Text)
    Quantita = db.Column(db.Text)
    NumFase = db.Column(db.Text)
    CodLavorazione = db.Column(db.Text)
    CodRisorsaProd = db.Column(db.Text)
    DataInizioSched = db.Column(db.Text)
    DataFineSched = db.Column(db.Text)
    GestioneLotto = db.Column(db.Text)
    GestioneMatricola = db.Column(db.Text)
    DistintaMateriale = db.Column(db.Text)
    CodMatricola = db.Column(db.Text)
    StatoRiga = db.Column(db.Text)
    CodFamiglia = db.Column(db.Text)
    CodMacrofamiglia = db.Column(db.Text)
    CodMagPrincipale = db.Column(db.Text)
    CodReparto = db.Column(db.Text)
    TempoPrevistoLavoraz = db.Column(db.Text)
    IndiceModifica = db.Column(db.Text)
    StatoOrdineErp = db.Column("StatoOrdine", db.Text)
    CodClassifTecnica = db.Column(db.Text)
    CodTipoDoc = db.Column(db.Text)
    TempoAttrezzaggio = db.Column(db.Text)
    VarianteArt = db.Column(db.Text)
    NumProgrRiga = db.Column(db.Text)

    __table_args__ = (db.PrimaryKeyConstraint("IdDocumento", "IdRiga"),)

    runtime_row = db.relationship(
        "InputOdpRuntime",
        uselist=False,
        lazy="selectin",
        cascade="all, delete-orphan",
        single_parent=True,
        primaryjoin=lambda: and_(
            InputOdp.IdDocumento == foreign(InputOdpRuntime.IdDocumento),
            InputOdp.IdRiga == foreign(InputOdpRuntime.IdRiga),
        ),
    )

    @staticmethod
    def _text(value) -> str:
        return str(value or "").strip()

    def _ensure_runtime_row(self) -> InputOdpRuntime:
        row = self.runtime_row
        if row is None:
            row = InputOdpRuntime(
                IdDocumento=self.IdDocumento,
                IdRiga=self.IdRiga,
                RifRegistraz=self.RifRegistraz,
            )
            self.runtime_row = row
            db.session.add(row)
        return row

    @staticmethod
    def _active_value_from_phase_list(raw_value, fase_attiva) -> str:
        raw = InputOdp._text(raw_value)
        if not raw:
            return ""

        try:
            parsed = json.loads(raw)
        except Exception:
            return raw

        if not isinstance(parsed, list):
            return InputOdp._text(parsed)

        try:
            idx = int(float(InputOdp._text(fase_attiva))) - 1
        except (TypeError, ValueError):
            idx = 0

        if 0 <= idx < len(parsed):
            return InputOdp._text(parsed[idx])

        return InputOdp._text(parsed[0]) if parsed else ""

    @property
    def StatoOrdine(self) -> str:
        stato_runtime = self._text(getattr(self.runtime_row, "Stato_odp", ""))
        if stato_runtime:
            return stato_runtime

        stato_erp = self._text(self.StatoOrdineErp)
        if stato_erp:
            return stato_erp

        return "Pianificata"

    @StatoOrdine.setter
    def StatoOrdine(self, value):
        row = self._ensure_runtime_row()
        row.Stato_odp = self._text(value)

    @property
    def FaseAttiva(self) -> str:
        fase_runtime = self._text(getattr(self.runtime_row, "FaseAttiva", ""))
        if fase_runtime:
            return fase_runtime
        return "1"

    @FaseAttiva.setter
    def FaseAttiva(self, value):
        value = self._text(value)
        stato = self._ensure_runtime_row()
        stato.FaseAttiva = value

    @property
    def Note(self) -> str:
        return self._text(getattr(self.runtime_row, "Note", ""))

    @Note.setter
    def Note(self, value):
        rt = self._ensure_runtime_row()
        rt.Note = self._text(value)

    @property
    def QtyDaLavorare(self) -> str:
        qty_runtime = self._text(getattr(self.runtime_row, "QtyDaLavorare", ""))
        if qty_runtime:
            return qty_runtime
        return self._text(self.Quantita)

    @QtyDaLavorare.setter
    def QtyDaLavorare(self, value):
        rt = self._ensure_runtime_row()
        rt.QtyDaLavorare = self._text(value)

    @property
    def RisorsaAttiva(self) -> str:
        return self._text(getattr(self.runtime_row, "RisorsaAttiva", ""))

    @RisorsaAttiva.setter
    def RisorsaAttiva(self, value):
        rt = self._ensure_runtime_row()
        rt.RisorsaAttiva = self._text(value)

    @property
    def AttrezzaggioAttivo(self) -> str:
        valore_runtime = self._text(getattr(self.runtime_row, "AttrezzaggioAttivo", ""))
        if valore_runtime:
            return valore_runtime

        return self._active_value_from_phase_list(
            self.TempoAttrezzaggio,
            self.FaseAttiva,
        )

    @AttrezzaggioAttivo.setter
    def AttrezzaggioAttivo(self, value):
        rt = self._ensure_runtime_row()
        rt.AttrezzaggioAttivo = self._text(value)

    def __repr__(self):
        return f"<{self.__dict__}>"

    @property
    def LavorazioneAttiva(self) -> str:
        return self._text(getattr(self.runtime_row, "LavorazioneAttiva", ""))

    @LavorazioneAttiva.setter
    def LavorazioneAttiva(self, value):
        rt = self._ensure_runtime_row()
        rt.LavorazioneAttiva = self._text(value)

    @property
    def RifOrdinePrinc(self) -> str:
        return self._text(getattr(self.runtime_row, "RifOrdinePrinc", ""))

    @RifOrdinePrinc.setter
    def RifOrdinePrinc(self, value):
        rt = self._ensure_runtime_row()
        rt.RifOrdinePrinc = self._text(value)


class LottiUsatiLog(db.Model):
    __bind_key__ = "log"
    __tablename__ = "lotti_usati_log"

    log_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    logged_at = db.Column(
        db.Text,
        nullable=False,
        default=lambda: datetime.now(ZoneInfo("Europe/Rome")).isoformat(
            timespec="seconds"
        ),
    )

    OperationGroupId = db.Column(db.Text, index=True)

    IdDocumento = db.Column(db.Text, nullable=False, index=True)
    IdRiga = db.Column(db.Text, nullable=False, index=True)
    RifRegistraz = db.Column(db.Text)

    CodArt = db.Column(db.Text, nullable=False, index=True)
    RifLottoAlfa = db.Column(db.Text, nullable=False, index=True)
    Quantita = db.Column(db.Text, nullable=False)
    Esito = db.Column(db.Text)

    ClosedBy = db.Column(db.Text)
    ClosedAt = db.Column(db.Text)
    Fase = db.Column(db.Text)

    __table_args__ = (
        db.Index("ix_lotti_usati_log_doc_riga", "IdDocumento", "IdRiga"),
        db.Index(
            "ix_lotti_usati_log_doc_riga_codart",
            "IdDocumento",
            "IdRiga",
            "CodArt",
        ),
    )

    def __repr__(self):
        return (
            f"<LottiUsatiLog {self.log_id} "
            f"{self.IdDocumento}/{self.IdRiga} {self.CodArt} {self.RifLottoAlfa}>"
        )


class ErpOutbox(db.Model):
    __bind_key__ = "log"
    __tablename__ = "erp_outbox"

    outbox_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    created_at = db.Column(
        db.Text,
        nullable=False,
        default=lambda: datetime.now(ZoneInfo("Europe/Rome")).isoformat(
            timespec="seconds"
        ),
    )

    kind = db.Column(db.Text, nullable=False)  # "consuntivo_fase"
    status = db.Column(db.Text, nullable=False, default="pending")

    IdDocumento = db.Column(db.Text, nullable=False)
    IdRiga = db.Column(db.Text, nullable=False)
    RifRegistraz = db.Column(db.Text)
    CodArt = db.Column(db.Text)
    Fase = db.Column(db.Text, nullable=False)
    payload_json = db.Column(db.Text, nullable=False)
    attempts = db.Column(db.Integer, nullable=False, default=0)
    last_error = db.Column(db.Text)
    exported_at = db.Column(db.Text)


class LottiGeneratiLog(db.Model):
    __bind_key__ = "log"
    __tablename__ = "lotti_generati_log"

    log_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    logged_at = db.Column(
        db.Text,
        nullable=False,
        default=lambda: datetime.now(ZoneInfo("Europe/Rome")).isoformat(
            timespec="seconds"
        ),
    )

    OperationGroupId = db.Column(db.Text, index=True)

    IdDocumento = db.Column(db.Text, nullable=False, index=True)
    IdRiga = db.Column(db.Text, nullable=False, index=True)
    RifRegistraz = db.Column(db.Text)

    CodArt = db.Column(db.Text, nullable=False, index=True)
    RifLottoAlfa = db.Column(db.Text, nullable=False, index=True)
    Quantita = db.Column(db.Text, nullable=False)
    Fase = db.Column(db.Text)

    ClosedBy = db.Column(db.Text)
    ClosedAt = db.Column(db.Text)

    __table_args__ = (
        db.Index("ix_lotti_generati_log_doc_riga", "IdDocumento", "IdRiga"),
    )

    def __repr__(self):
        return (
            f"<LottiGeneratiLog {self.log_id} "
            f"{self.IdDocumento}/{self.IdRiga} {self.CodArt} {self.RifLottoAlfa}>"
        )


class InputOdpRuntime(db.Model):
    __tablename__ = "input_odp_runtime"
    IdDocumento = db.Column(db.Text, primary_key=True)
    IdRiga = db.Column(db.Text, primary_key=True)
    RifRegistraz = db.Column(db.Text)
    Stato_odp = db.Column(db.Text)
    Data_in_carico = db.Column(db.Text)
    Tempo_funzionamento = db.Column(db.Text)
    Utente_operazione = db.Column(db.Text)
    FaseAttiva = db.Column(db.Text)
    data_ultima_attivazione = db.Column(db.Text)
    Note = db.Column(db.Text)
    QtyDaLavorare = db.Column(db.Text)
    RisorsaAttiva = db.Column(db.Text)
    LavorazioneAttiva = db.Column(db.Text)
    RifOrdinePrinc = db.Column(db.Text)
    AttrezzaggioAttivo = db.Column(db.Text)
    VarianteArt = db.Column(db.Text)

    __table_args__ = (
        db.ForeignKeyConstraint(
            ["IdDocumento", "IdRiga"],
            ["input_odp.IdDocumento", "input_odp.IdRiga"],
            ondelete="CASCADE",
        ),
    )

    def __repr__(self):
        return f"<InputOdpRuntime {self.__dict__}>"


class AcqArticoli(db.Model):
    __bind_key__ = "acq"
    __tablename__ = "acq_articoli"

    CodArt = db.Column(db.Text, primary_key=True)
    DesArt = db.Column(db.Text)
    LottoRiordino = db.Column(db.Float)
    PuntoRiordino = db.Column(db.Float)
    PianTempoApprovFisso = db.Column(db.Integer)
    DataPrevistaApprovvigionamento = db.Column(db.Text)
    synced_at = db.Column(db.Text)


class AcqGiacenze(db.Model):
    __bind_key__ = "acq"
    __tablename__ = "acq_giacenze"

    CodArt = db.Column(db.Text, primary_key=True)
    CodMag = db.Column(db.Text, primary_key=True)
    Giacenza = db.Column(db.Float)
    synced_at = db.Column(db.Text)


class AcqFabbisognoOdp(db.Model):
    __bind_key__ = "acq"
    __tablename__ = "acq_fabbisogno_odp"

    IdDocumento = db.Column(db.Text, primary_key=True)
    IdRiga = db.Column(db.Text, primary_key=True)
    NumFase = db.Column(db.Text, primary_key=True)
    CodArt = db.Column(db.Text, primary_key=True)
    VarianteArt = db.Column(db.Text, primary_key=True)
    QuantitaNecessaria = db.Column(db.Float)
    synced_at = db.Column(db.Text)


class AcqRiepilogoMateriali(db.Model):
    __bind_key__ = "acq"
    __tablename__ = "acq_riepilogo_materiali"

    IdDocumento = db.Column(db.Text, primary_key=True)
    IdRiga = db.Column(db.Text, primary_key=True)
    NumFase = db.Column(db.Text, primary_key=True)
    CodArt = db.Column(db.Text, primary_key=True)
    VarianteArt = db.Column(db.Text, primary_key=True)
    QuantitaNecessaria = db.Column(db.Float)
    GiacenzaTotale = db.Column(db.Float)
    LottoRiordino = db.Column(db.Float)
    PuntoRiordino = db.Column(db.Float)
    PianTempoApprovFisso = db.Column(db.Integer)
    DataPrevistaApprovvigionamento = db.Column(db.Text)
    synced_at = db.Column(db.Text)
