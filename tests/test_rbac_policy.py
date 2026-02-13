from __future__ import annotations

import os
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Integer, String, Table, create_engine, insert, select
from sqlalchemy.orm import declarative_base, sessionmaker

# =========================
# Modelli minimi + tabelle ponte (solo ciò che serve alla policy)
# =========================
Base = declarative_base()
metadata = Base.metadata

# Tabelle ponte (nomi/colonne coerenti con RBAC.db)
user_roles = Table(
    "user_roles",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("role_id", Integer, primary_key=True),
)

roles_ineritance = Table(
    "roles_ineritance",
    metadata,
    Column("role_id", Integer, primary_key=True),
    Column("included_role", Integer, primary_key=True),
)

roles_permission = Table(
    "roles_permission",
    metadata,
    Column("role_id", Integer, primary_key=True),
    Column("permission_id", Integer, primary_key=True),
)

roles_reparti = Table(
    "roles_reparti",
    metadata,
    Column("roles_id", Integer, primary_key=True),
    Column("reparto_id", Integer, primary_key=True),
)


# Tabelle "entità" minime
class Permissions(Base):
    __tablename__ = "permissions"
    id = Column(Integer, primary_key=True)
    code = Column(String, nullable=False)
    description = Column(String)


class Reparti(Base):
    __tablename__ = "reparti"
    id = Column(Integer, primary_key=True)
    codice = Column(String, nullable=False)
    descrizione = Column(String)


class InputOdp(Base):
    __tablename__ = "input_odp"
    IdDocumento = Column(String, primary_key=True)
    IdRiga = Column(String, primary_key=True)
    CodReparto = Column(String)


# Per creare un utente/ruolo di test "senza reparti"
roles_tbl = Table(
    "roles",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("description", String),
)

users_tbl = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("public_id", String, nullable=False),
    Column("username", String, nullable=False),
    Column("active", Integer, nullable=False),
    Column("preference", String),
    Column("genere", String),
    Column("RepartoPrinc", String),
)


# =========================
# Helpers
# =========================
def _find_rbac_db() -> Path:
    env = os.environ.get("RBAC_DB_PATH")
    candidates = [
        Path(env) if env else None,
        Path("app_odp/instance/RBAC.db"),
        Path("app/instance/RBAC.db"),
        Path("instance/RBAC.db"),
        Path("RBAC.db"),
    ]
    for p in candidates:
        if p and p.is_file():
            return p

    # Fallback: cerca nel repo
    for p in Path(".").rglob("RBAC.db"):
        return p

    raise FileNotFoundError(
        "RBAC.db non trovato. Imposta RBAC_DB_PATH oppure metti RBAC.db in una delle path standard."
    )


def _ensure_permission(session, code: str, description: str = "test") -> Permissions:
    perm = session.execute(
        select(Permissions).where(Permissions.code == code)
    ).scalar_one_or_none()
    if perm:
        return perm
    perm = Permissions(code=code, description=description)
    session.add(perm)
    session.commit()
    return perm


def _insert_test_odp_rows(session):
    # Righe "pulite" con CodReparto = '10'/'20'/... per testare davvero il filtro in_(reparti)
    rows = [
        InputOdp(IdDocumento="UT1", IdRiga="1", CodReparto="10"),
        InputOdp(IdDocumento="UT2", IdRiga="1", CodReparto="20"),
        InputOdp(IdDocumento="UT3", IdRiga="1", CodReparto="30"),
        InputOdp(IdDocumento="UT4", IdRiga="1", CodReparto="40"),
    ]
    session.add_all(rows)
    session.commit()
    return {"UT1", "UT2", "UT3", "UT4"}


# =========================
# Fixtures
# =========================
@pytest.fixture()
def db_session(tmp_path):
    src = _find_rbac_db()
    dst = tmp_path / "RBAC_test.db"
    shutil.copy(src, dst)

    engine = create_engine(
        f"sqlite:///{dst}", connect_args={"check_same_thread": False}
    )
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


@pytest.fixture()
def policy_mod(monkeypatch, db_session):
    """
    Importa la tua policy e la "lega" a:
    - una sessione SQLAlchemy di test
    - modelli minimi coerenti con lo schema del DB
    """
    import app_odp.RBAC.policy as policy  # <-- se il path nel tuo progetto è diverso, cambia qui

    fake_db = SimpleNamespace(session=db_session)

    monkeypatch.setattr(policy, "db", fake_db, raising=False)
    monkeypatch.setattr(policy, "InputOdp", InputOdp, raising=False)
    monkeypatch.setattr(policy, "Permissions", Permissions, raising=False)
    monkeypatch.setattr(policy, "Reparti", Reparti, raising=False)

    monkeypatch.setattr(policy, "user_roles", user_roles, raising=False)
    monkeypatch.setattr(policy, "roles_ineritance", roles_ineritance, raising=False)
    monkeypatch.setattr(policy, "roles_permission", roles_permission, raising=False)
    monkeypatch.setattr(policy, "roles_reparti", roles_reparti, raising=False)

    return policy


# =========================
# Test: role tree / permessi / reparti
# =========================
def test_role_ids_recursive_tree_user_1(policy_mod):
    # user_id=1 nel DB: role_id=2 (responsabile_produzione) e eredita una catena di ruoli
    p = policy_mod.RbacPolicy(SimpleNamespace(id=1))
    assert p.role_ids == {2, 3, 4, 5, 6, 7, 8, 9, 10, 11}


def test_role_ids_admin_user_11(policy_mod):
    # user_id=11: role_id=1 (admin) -> include 2 -> include ...
    p = policy_mod.RbacPolicy(SimpleNamespace(id=11))
    assert p.role_ids == {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}


def test_can_permission_inherited(policy_mod):
    # user_id=5: role_id=6 (responsabile_officina) include role 11 (operatore_officina) che ha 'home'
    p = policy_mod.RbacPolicy(SimpleNamespace(id=5))
    assert p.can("home") is True
    assert p.can("controllo_qualita") is False


def test_allowed_reparti_codes_inherited(policy_mod):
    # user_id=1 eredita ruoli operatore_assemblaggio/officina/carpenteria -> reparti 10,20,30
    p = policy_mod.RbacPolicy(SimpleNamespace(id=1))
    assert p.allowed_reparti_codes == {"10", "20", "30"}


def test_role_tree_is_cycle_safe_union(policy_mod, db_session):
    # Creo un ciclo 6 -> 11 (già esiste) e aggiungo 11 -> 6.
    # Con UNION, la CTE non dovrebbe esplodere.
    db_session.execute(insert(roles_ineritance).values(role_id=11, included_role=6))
    db_session.commit()

    p = policy_mod.RbacPolicy(SimpleNamespace(id=5))  # user 5 ha role 6
    assert p.role_ids == {6, 11}


# =========================
# Test: filtro ODP
# =========================
def test_filter_input_odp_read_all_bypasses_filter(policy_mod, db_session):
    # Aggiungo 'odp.read_all' SOLO nel DB di test e lo assegno al ruolo 1 (admin)
    perm = _ensure_permission(db_session, "odp.read_all", "read all odp (test)")
    db_session.execute(
        insert(roles_permission).values(role_id=1, permission_id=perm.id)
    )
    db_session.commit()

    _insert_test_odp_rows(db_session)

    q = db_session.query(InputOdp).filter(
        InputOdp.IdDocumento.in_(["UT1", "UT2", "UT3", "UT4"])
    )

    admin = policy_mod.RbacPolicy(SimpleNamespace(id=11))  # user 11 include role 1
    got = {r.IdDocumento for r in admin.filter_input_odp(q).all()}
    assert got == {"UT1", "UT2", "UT3", "UT4"}


def test_filter_input_odp_filters_by_reparti(policy_mod, db_session):
    _insert_test_odp_rows(db_session)

    q = db_session.query(InputOdp).filter(
        InputOdp.IdDocumento.in_(["UT1", "UT2", "UT3", "UT4"])
    )

    # user 2: role 7 -> reparto 10
    u2 = policy_mod.RbacPolicy(SimpleNamespace(id=2))
    got_u2 = {r.IdDocumento for r in u2.filter_input_odp(q).all()}
    assert got_u2 == {"UT1"}

    # user 5: role 6+11 -> reparto 20
    u5 = policy_mod.RbacPolicy(SimpleNamespace(id=5))
    got_u5 = {r.IdDocumento for r in u5.filter_input_odp(q).all()}
    assert got_u5 == {"UT2"}

    # user 1: reparti 10,20,30
    u1 = policy_mod.RbacPolicy(SimpleNamespace(id=1))
    got_u1 = {r.IdDocumento for r in u1.filter_input_odp(q).all()}
    assert got_u1 == {"UT1", "UT2", "UT3"}


def test_filter_input_odp_no_reparti_returns_zero_rows(policy_mod, db_session):
    # Creo un ruolo e utente di test senza reparti e senza permessi speciali
    db_session.execute(
        insert(roles_tbl).values(id=99, name="utest_role", description="utest")
    )
    db_session.execute(
        insert(users_tbl).values(
            id=99, public_id="utest_user", username="UTest User", active=1
        )
    )
    db_session.execute(insert(user_roles).values(user_id=99, role_id=99))
    db_session.commit()

    db_session.add(InputOdp(IdDocumento="UT_NO", IdRiga="1", CodReparto="10"))
    db_session.commit()

    q = db_session.query(InputOdp).filter(InputOdp.IdDocumento == "UT_NO")

    u99 = policy_mod.RbacPolicy(SimpleNamespace(id=99))
    assert u99.allowed_reparti_codes == set()
    assert u99.can("odp.read_all") is False
    assert u99.filter_input_odp(q).count() == 0


@pytest.mark.xfail(
    reason=(
        'Nel DB reale CodReparto sembra salvato come stringa JSON-like (es. [["10"]]). '
        "La policy usa in_({'10'}) e quindi non matcha. Normalizza CodReparto oppure cambia filtro."
    )
)
def test_filter_input_odp_on_real_rows_should_not_be_empty(policy_mod, db_session):
    # Smoke test sul DB reale: per un utente con reparti assegnati dovrebbe vedere qualcosa.
    u2 = policy_mod.RbacPolicy(SimpleNamespace(id=2))
    q = db_session.query(InputOdp)
    assert u2.filter_input_odp(q).count() > 0


def test_filter_input_odp_matches_json_codreparto(policy_mod, db_session):
    # CodReparto come nel DB reale
    db_session.add(
        InputOdp(IdDocumento="UTJ", IdRiga="1", CodReparto='[["10"], ["10"]]')
    )
    db_session.commit()

    q = db_session.query(InputOdp).filter(InputOdp.IdDocumento == "UTJ")

    u2 = policy_mod.RbacPolicy(SimpleNamespace(id=2))  # user 2 -> reparto 10
    got = {r.IdDocumento for r in u2.filter_input_odp(q).all()}
    assert got == {"UTJ"}
