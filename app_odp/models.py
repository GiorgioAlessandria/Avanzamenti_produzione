from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo
from flask_login import UserMixin
import uuid
import json
from flask_sqlalchemy import SQLAlchemy

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


class ChangeEvent(db.Model):
    __tablename__ = "change_event"

    id = db.Column(db.Integer, primary_key=True)
    topic = db.Column(db.Text, nullable=False)
    scope = db.Column(db.Text)
    payload_json = db.Column(db.Text)
    created_at = db.Column(
        db.Text, nullable=False, default=lambda: datetime.now(ZoneInfo("Europe/Rome"))
    )

    def __repr__(self):
        return f"<ChangeEvent {self.__dict__}>"


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
    StatoOrdine = db.Column(db.Text)
    CodClassifTecnica = db.Column(db.Text)
    CodTipoDoc = db.Column(db.Text)
    FaseAttiva = db.Column(db.Text)
    Note = db.Column(db.Text)

    __table_args__ = (db.PrimaryKeyConstraint("IdDocumento", "IdRiga"),)

    def __repr__(self):
        return f"<{self.__dict__}>"


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
    Giacenza = db.Column(db.Text, nullable=False)
    CodMag = db.Column(db.Text, nullable=False)


class StatoOdp(db.Model):
    __tablename__ = "odp_in_carico"

    IdDocumento = db.Column(db.Text, primary_key=True)
    IdRiga = db.Column(db.Text, primary_key=True)
    RifRegistraz = db.Column(db.Text, index=True, nullable=False)

    Stato_odp = db.Column(db.Text)
    Data_in_carico = db.Column(db.Text)
    Tempo_funzionamento = db.Column(db.Text)
    Utente_operazione = db.Column(db.Text)
    Fase = db.Column(db.Text)
    data_ultima_attivazione = db.Column(db.Text)


class TipologieStato(db.Model):
    __tablename__ = "tipologie_stato"

    id = db.Column(db.Integer, primary_key=True)
    tipo = db.Column(db.Integer)

    def __repr__(self):
        return f"<TipologieStato {self.__dict__}>"


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

    # chiavi originali
    IdDocumento = db.Column(db.Text, nullable=False)
    IdRiga = db.Column(db.Text, nullable=False)

    # snapshot campi InputOdp
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
    StatoOrdine = db.Column(db.Text)
    CodClassifTecnica = db.Column(db.Text)
    CodTipoDoc = db.Column(db.Text)
    FaseAttiva = db.Column(db.Text)
    Note = db.Column(db.Text)

    # dati chiusura (processo normale)
    QuantitaConforme = db.Column(db.Text)
    QuantitaNonConforme = db.Column(db.Text)
    NoteChiusura = db.Column(db.Text)
    ClosedBy = db.Column(db.Text)
    ClosedAt = db.Column(db.Text)


class StatoOdpLog(db.Model):
    __bind_key__ = "log"
    __tablename__ = "odp_in_carico_log"

    log_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    logged_at = db.Column(
        db.Text,
        nullable=False,
        default=lambda: datetime.now(ZoneInfo("Europe/Rome")).isoformat(
            timespec="seconds"
        ),
    )

    IdDocumento = db.Column(db.Text, nullable=False)
    IdRiga = db.Column(db.Text, nullable=False)
    RifRegistraz = db.Column(db.Text)

    Stato_odp = db.Column(db.Text)
    Data_in_carico = db.Column(db.Text)
    Tempo_funzionamento = db.Column(db.Text)
    Utente_operazione = db.Column(db.Text)
    Fase = db.Column(db.Text)
    data_ultima_attivazione = db.Column(db.Text)

    ClosedBy = db.Column(db.Text)
    ClosedAt = db.Column(db.Text)


class ChangeEventLog(db.Model):
    __bind_key__ = "log"
    __tablename__ = "change_event_log"

    log_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    logged_at = db.Column(
        db.Text,
        nullable=False,
        default=lambda: datetime.now(ZoneInfo("Europe/Rome")).isoformat(
            timespec="seconds"
        ),
    )

    # riferimento evento originale
    src_id = db.Column(db.Integer)
    topic = db.Column(db.Text, nullable=False)
    scope = db.Column(db.Text)
    payload_json = db.Column(db.Text)
    created_at = db.Column(db.Text)

    # chiavi ordine “estratte” per query più facili
    IdDocumento = db.Column(db.Text)
    IdRiga = db.Column(db.Text)


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

    # Riferimento ordine
    IdDocumento = db.Column(db.Text, nullable=False)
    IdRiga = db.Column(db.Text, nullable=False)
    RifRegistraz = db.Column(db.Text)

    # Dati lotto
    CodArt = db.Column(db.Text, nullable=False)  # codice componente
    RifLottoAlfa = db.Column(db.Text, nullable=False)  # numero lotto
    Quantita = db.Column(db.Text, nullable=False)  # quantità utilizzata
    Esito = db.Column(db.Text)  # "ok" o "ko"

    ClosedBy = db.Column(db.Text)
    ClosedAt = db.Column(db.Text)
