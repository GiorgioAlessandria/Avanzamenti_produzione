import importlib
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Integer, String, Table, create_engine, insert
from sqlalchemy.orm import declarative_base, sessionmaker

POLICY_MODULE_PATH = "app_odp.policy.policy"
DECORATOR_MODULE_PATH = "app_odp.policy.decorator"


@pytest.fixture()
def mod():
    return importlib.import_module(POLICY_MODULE_PATH)


@pytest.fixture()
def deco_mod():
    return importlib.import_module(DECORATOR_MODULE_PATH)


Base = declarative_base()
metadata = Base.metadata

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

roles_risorse = Table(
    "roles_risorse",
    metadata,
    Column("roles_id", Integer, primary_key=True),
    Column("risorse_id", Integer, primary_key=True),
)

roles_lavorazioni = Table(
    "roles_lavorazioni",
    metadata,
    Column("roles_id", Integer, primary_key=True),
    Column("lavorazioni_id", Integer, primary_key=True),
)

roles_famiglia = Table(
    "roles_famiglia",
    metadata,
    Column("roles_id", Integer, primary_key=True),
    Column("famiglia_id", Integer, primary_key=True),
)

roles_macrofamiglia = Table(
    "roles_macrofamiglia",
    metadata,
    Column("roles_id", Integer, primary_key=True),
    Column("macrofamiglia_id", Integer, primary_key=True),
)

roles_magazzini = Table(
    "roles_magazzini",
    metadata,
    Column("roles_id", Integer, primary_key=True),
    Column("magazzini_id", Integer, primary_key=True),
)


class Permissions(Base):
    __tablename__ = "permissions"

    id = Column(Integer, primary_key=True)
    Codice = Column(String, nullable=False)


class Reparti(Base):
    __tablename__ = "reparti"

    id = Column(Integer, primary_key=True)
    Codice = Column(String, nullable=False)
    Descrizione = Column(String)


class Risorse(Base):
    __tablename__ = "risorse"

    id = Column(Integer, primary_key=True)
    Codice = Column(String, nullable=False)


class Lavorazioni(Base):
    __tablename__ = "lavorazioni"

    id = Column(Integer, primary_key=True)
    Codice = Column(String, nullable=False)


class Famiglia(Base):
    __tablename__ = "famiglia"

    id = Column(Integer, primary_key=True)
    Codice = Column(String, nullable=False)


class Macrofamiglia(Base):
    __tablename__ = "macrofamiglia"

    id = Column(Integer, primary_key=True)
    Codice = Column(String, nullable=False)


class Magazzini(Base):
    __tablename__ = "magazzini"

    id = Column(Integer, primary_key=True)
    Codice = Column(String, nullable=False)


class InputOdp(Base):
    __tablename__ = "input_odp"

    IdDocumento = Column(String, primary_key=True)
    IdRiga = Column(String, primary_key=True)
    CodReparto = Column(String)
    CodFamiglia = Column(String)
    CodMacrofamiglia = Column(String)
    CodMagPrincipale = Column(String)


class InputOdpRuntime(Base):
    __tablename__ = "input_odp_runtime"

    IdDocumento = Column(String, primary_key=True)
    IdRiga = Column(String, primary_key=True)
    RisorsaAttiva = Column(String)
    LavorazioneAttiva = Column(String)


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


@pytest.fixture()
def policy_env(monkeypatch, mod, db_session):
    fake_db = SimpleNamespace(session=db_session)

    monkeypatch.setattr(mod, "db", fake_db, raising=False)
    monkeypatch.setattr(mod, "Permissions", Permissions, raising=False)
    monkeypatch.setattr(mod, "Reparti", Reparti, raising=False)
    monkeypatch.setattr(mod, "Risorse", Risorse, raising=False)
    monkeypatch.setattr(mod, "Lavorazioni", Lavorazioni, raising=False)
    monkeypatch.setattr(mod, "Famiglia", Famiglia, raising=False)
    monkeypatch.setattr(mod, "Macrofamiglia", Macrofamiglia, raising=False)
    monkeypatch.setattr(mod, "Magazzini", Magazzini, raising=False)
    monkeypatch.setattr(mod, "InputOdp", InputOdp, raising=False)
    monkeypatch.setattr(mod, "InputOdpRuntime", InputOdpRuntime, raising=False)
    monkeypatch.setattr(mod, "user_roles", user_roles, raising=False)
    monkeypatch.setattr(mod, "roles_ineritance", roles_ineritance, raising=False)
    monkeypatch.setattr(mod, "roles_permission", roles_permission, raising=False)
    monkeypatch.setattr(mod, "roles_reparti", roles_reparti, raising=False)
    monkeypatch.setattr(mod, "roles_risorse", roles_risorse, raising=False)
    monkeypatch.setattr(mod, "roles_lavorazioni", roles_lavorazioni, raising=False)
    monkeypatch.setattr(mod, "roles_famiglia", roles_famiglia, raising=False)
    monkeypatch.setattr(mod, "roles_macrofamiglia", roles_macrofamiglia, raising=False)
    monkeypatch.setattr(mod, "roles_magazzini", roles_magazzini, raising=False)

    return db_session


class _ObjWithCode:
    def __init__(self, code):
        self.Codice = code


class Abort403(Exception):
    pass


def _policy_for(mod, user_id: int):
    return mod.RbacPolicy(SimpleNamespace(id=user_id))


def _seed_entities(session):
    session.add_all(
        [
            Permissions(id=1, Codice="odp.read_all"),
            Permissions(id=2, Codice="home"),
            Reparti(id=1, Codice="10", Descrizione="Assemblaggio"),
            Reparti(id=2, Codice="20", Descrizione="Officina"),
            Risorse(id=1, Codice="RIS1"),
            Risorse(id=2, Codice="RIS2"),
            Lavorazioni(id=1, Codice="LAV1"),
            Lavorazioni(id=2, Codice="LAV2"),
            Famiglia(id=1, Codice="F1"),
            Famiglia(id=2, Codice="F2"),
            Macrofamiglia(id=1, Codice="MF1"),
            Magazzini(id=1, Codice="MAG1"),
        ]
    )
    session.commit()


def _seed_odp_rows(session):
    session.add_all(
        [
            InputOdp(
                IdDocumento="A",
                IdRiga="1",
                CodReparto="10",
                CodFamiglia="F1",
                CodMacrofamiglia="MF1",
                CodMagPrincipale="MAG1",
            ),
            InputOdp(
                IdDocumento="B",
                IdRiga="1",
                CodReparto='[["10"]]',
                CodFamiglia="F1",
                CodMacrofamiglia="MF1",
                CodMagPrincipale="MAG1",
            ),
            InputOdp(
                IdDocumento="C",
                IdRiga="1",
                CodReparto="20",
                CodFamiglia="F2",
                CodMacrofamiglia="MF1",
                CodMagPrincipale="MAG1",
            ),
        ]
    )
    session.add_all(
        [
            InputOdpRuntime(
                IdDocumento="A",
                IdRiga="1",
                RisorsaAttiva="RIS1",
                LavorazioneAttiva="LAV1",
            ),
            InputOdpRuntime(
                IdDocumento="B",
                IdRiga="1",
                RisorsaAttiva='[["RIS1"]]',
                LavorazioneAttiva='[["LAV1"]]',
            ),
            InputOdpRuntime(
                IdDocumento="C",
                IdRiga="1",
                RisorsaAttiva="RIS2",
                LavorazioneAttiva="LAV2",
            ),
        ]
    )
    session.commit()


def test_codes_handles_scalars_dicts_and_objects(mod):
    assert mod._codes(["10", 20]) == {"10", "20"}
    assert mod._codes({"10": "x", "20": "y"}) == {"10", "20"}
    assert mod._codes([_ObjWithCode("R1"), _ObjWithCode("R2")]) == {"R1", "R2"}
    assert mod._codes([]) == set()


def test_role_ids_recursive_union_is_cycle_safe(mod, policy_env):
    session = policy_env
    session.execute(insert(user_roles), [{"user_id": 1, "role_id": 100}])
    session.execute(
        insert(roles_ineritance),
        [
            {"role_id": 100, "included_role": 200},
            {"role_id": 200, "included_role": 300},
            {"role_id": 300, "included_role": 100},
        ],
    )
    session.commit()

    policy = _policy_for(mod, 1)

    assert policy.role_ids == {100, 200, 300}


def test_can_reads_permissions_from_inherited_roles(mod, policy_env):
    session = policy_env
    _seed_entities(session)
    session.execute(insert(user_roles), [{"user_id": 5, "role_id": 10}])
    session.execute(insert(roles_ineritance), [{"role_id": 10, "included_role": 20}])
    session.execute(insert(roles_permission), [{"role_id": 20, "permission_id": 2}])
    session.commit()

    policy = _policy_for(mod, 5)

    assert policy.can("home") is True
    assert policy.can("odp.read_all") is False


def test_allowed_reparti_and_menu_are_distinct_and_sorted(mod, policy_env):
    session = policy_env
    _seed_entities(session)

    session.execute(
        insert(user_roles),
        [
            {"user_id": 1, "role_id": 10},
            {"user_id": 1, "role_id": 11},
        ],
    )

    session.execute(
        insert(roles_reparti),
        [
            {"roles_id": 10, "reparto_id": 2},
            {"roles_id": 11, "reparto_id": 1},
            {"roles_id": 10, "reparto_id": 1},
        ],
    )
    session.commit()

    policy = mod.RbacPolicy(SimpleNamespace(id=1))

    assert policy.allowed_reparti == {"10", "20"}
    assert policy.allowed_reparti_menu == [
        ("10", "Assemblaggio"),
        ("20", "Officina"),
    ]
    assert set(policy.allowed_reparti_descr) == {"Assemblaggio", "Officina"}


def test_filter_input_odp_bypasses_filters_when_user_has_read_all(mod, policy_env):
    session = policy_env
    _seed_entities(session)
    _seed_odp_rows(session)
    session.execute(insert(user_roles), [{"user_id": 11, "role_id": 1}])
    session.execute(insert(roles_permission), [{"role_id": 1, "permission_id": 1}])
    session.commit()

    q = session.query(InputOdp).order_by(InputOdp.IdDocumento)
    got = [row.IdDocumento for row in _policy_for(mod, 11).filter_input_odp(q).all()]

    assert got == ["A", "B", "C"]


def test_filter_input_odp_filters_by_direct_reparto_code(mod, policy_env):
    session = policy_env
    _seed_entities(session)
    _seed_odp_rows(session)
    session.execute(insert(user_roles), [{"user_id": 2, "role_id": 7}])
    session.execute(insert(roles_reparti), [{"roles_id": 7, "reparto_id": 1}])
    session.commit()

    q = session.query(InputOdp).order_by(InputOdp.IdDocumento)
    got = [row.IdDocumento for row in _policy_for(mod, 2).filter_input_odp(q).all()]

    assert got == ["A", "B"]


def test_filter_input_odp_matches_nested_json_reparto_values(mod, policy_env):
    session = policy_env
    _seed_entities(session)
    _seed_odp_rows(session)
    session.execute(insert(user_roles), [{"user_id": 2, "role_id": 7}])
    session.execute(insert(roles_reparti), [{"roles_id": 7, "reparto_id": 1}])
    session.commit()

    q = session.query(InputOdp).filter(InputOdp.IdDocumento == "B")
    got = [row.IdDocumento for row in _policy_for(mod, 2).filter_input_odp(q).all()]

    assert got == ["B"]


def test_filter_input_odp_applies_and_logic_across_dimensions(mod, policy_env):
    session = policy_env
    _seed_entities(session)
    _seed_odp_rows(session)
    session.execute(insert(user_roles), [{"user_id": 3, "role_id": 30}])
    session.execute(insert(roles_reparti), [{"roles_id": 30, "reparto_id": 1}])
    session.execute(insert(roles_risorse), [{"roles_id": 30, "risorse_id": 1}])
    session.execute(insert(roles_lavorazioni), [{"roles_id": 30, "lavorazioni_id": 1}])
    session.execute(insert(roles_famiglia), [{"roles_id": 30, "famiglia_id": 1}])
    session.execute(
        insert(roles_macrofamiglia), [{"roles_id": 30, "macrofamiglia_id": 1}]
    )
    session.execute(insert(roles_magazzini), [{"roles_id": 30, "magazzini_id": 1}])
    session.commit()

    q = session.query(InputOdp).order_by(InputOdp.IdDocumento)
    got = [row.IdDocumento for row in _policy_for(mod, 3).filter_input_odp(q).all()]

    assert got == ["A", "B"]


def test_filter_input_odp_for_reparto_applies_general_filter_then_requested_tab(
    mod, policy_env
):
    session = policy_env
    _seed_entities(session)
    _seed_odp_rows(session)
    session.execute(insert(user_roles), [{"user_id": 4, "role_id": 40}])
    session.execute(
        insert(roles_reparti),
        [
            {"roles_id": 40, "reparto_id": 1},
            {"roles_id": 40, "reparto_id": 2},
        ],
    )
    session.commit()

    q = session.query(InputOdp).order_by(InputOdp.IdDocumento)
    got = [
        row.IdDocumento
        for row in _policy_for(mod, 4).filter_input_odp_for_reparto(q, "20").all()
    ]

    assert got == ["C"]


def test_filter_input_odp_uses_runtime_json_for_risorsa_and_lavorazione(
    mod, policy_env
):
    session = policy_env
    _seed_entities(session)
    _seed_odp_rows(session)
    session.execute(insert(user_roles), [{"user_id": 6, "role_id": 60}])
    session.execute(insert(roles_risorse), [{"roles_id": 60, "risorse_id": 1}])
    session.execute(insert(roles_lavorazioni), [{"roles_id": 60, "lavorazioni_id": 1}])
    session.commit()

    q = session.query(InputOdp).order_by(InputOdp.IdDocumento)
    got = [row.IdDocumento for row in _policy_for(mod, 6).filter_input_odp(q).all()]

    assert got == ["A", "B"]


def test_filter_input_odp_returns_unfiltered_query_when_no_scope_is_available(
    mod, policy_env
):
    session = policy_env
    _seed_entities(session)
    _seed_odp_rows(session)
    session.execute(insert(user_roles), [{"user_id": 99, "role_id": 999}])
    session.commit()

    q = session.query(InputOdp).order_by(InputOdp.IdDocumento)
    got = [row.IdDocumento for row in _policy_for(mod, 99).filter_input_odp(q).all()]

    assert got == ["A", "B", "C"]


def test_require_perm_allows_execution_and_preserves_wrapped_metadata(
    deco_mod, monkeypatch
):
    class AllowPolicy:
        def __init__(self, user):
            self.user = user

        def can(self, code):
            return True

    monkeypatch.setattr(deco_mod, "RbacPolicy", AllowPolicy)
    monkeypatch.setattr(deco_mod, "current_user", SimpleNamespace(id=123))
    monkeypatch.setattr(
        deco_mod, "abort", lambda code: (_ for _ in ()).throw(Abort403(code))
    )

    @deco_mod.require_perm("home")
    def target(a, b):
        return a + b

    assert target(2, 3) == 5
    assert target.__name__ == "target"


def test_require_perm_aborts_with_403_when_permission_is_missing(deco_mod, monkeypatch):
    class DenyPolicy:
        def __init__(self, user):
            self.user = user

        def can(self, code):
            return False

    def fake_abort(code):
        raise Abort403(code)

    monkeypatch.setattr(deco_mod, "RbacPolicy", DenyPolicy)
    monkeypatch.setattr(deco_mod, "current_user", SimpleNamespace(id=321))
    monkeypatch.setattr(deco_mod, "abort", fake_abort)

    @deco_mod.require_perm("home")
    def target():
        return "never"

    with pytest.raises(Abort403) as exc:
        target()

    assert exc.value.args == (403,)


def test_role_ids_is_empty_when_user_has_no_roles(mod, policy_env):
    from types import SimpleNamespace

    # Il fixture policy_env mantiene attivo il contesto Flask/SQLAlchemy
    _ = policy_env

    policy = mod.RbacPolicy(SimpleNamespace(id=9999))
    assert policy.role_ids == set()


def test_can_returns_false_for_unknown_permission(mod, policy_env):
    from types import SimpleNamespace

    _ = policy_env

    policy = mod.RbacPolicy(SimpleNamespace(id=9999))
    assert policy.can("permesso.che.non.esiste") is False


def test_match_returns_false_expression_when_allowed_is_empty(mod):
    from sqlalchemy import false

    expr = mod._match(mod.InputOdp.CodReparto, set())

    assert expr.compare(false())


def test_filter_input_odp_keeps_row_when_direct_reparto_matches_and_runtime_is_missing(
    mod,
    policy_env,
):
    from sqlalchemy import insert, select
    from types import SimpleNamespace

    session = policy_env
    _seed_entities(session)

    session.execute(insert(mod.user_roles), [{"user_id": 1, "role_id": 10}])
    session.execute(insert(mod.roles_reparti), [{"roles_id": 10, "reparto_id": 1}])

    # Usa solo colonne realmente necessarie al filtro della policy
    session.add(
        mod.InputOdp(
            IdDocumento="900",
            IdRiga="1",
            CodReparto="10",
            CodFamiglia="FAM-X",
            CodMacrofamiglia="MAC-X",
            CodMagPrincipale="MAG-X",
        )
    )
    session.commit()

    policy = mod.RbacPolicy(SimpleNamespace(id=1))
    rows = policy.filter_input_odp(session.query(mod.InputOdp)).all()

    assert len(rows) == 1
    assert rows[0].IdDocumento == "900"
    assert rows[0].IdRiga == "1"


def test_filter_input_odp_for_reparto_returns_empty_when_requested_reparto_is_not_allowed(
    mod,
    policy_env,
):
    from sqlalchemy import insert
    from types import SimpleNamespace

    session = policy_env
    _seed_entities(session)

    session.execute(insert(mod.user_roles), [{"user_id": 1, "role_id": 10}])
    session.execute(insert(mod.roles_reparti), [{"roles_id": 10, "reparto_id": 1}])

    session.add(
        mod.InputOdp(
            IdDocumento="901",
            IdRiga="1",
            CodReparto="10",
            CodFamiglia="FAM-X",
            CodMacrofamiglia="MAC-X",
            CodMagPrincipale="MAG-X",
        )
    )
    session.commit()

    policy = mod.RbacPolicy(SimpleNamespace(id=1))
    rows = policy.filter_input_odp_for_reparto(
        session.query(mod.InputOdp),
        "20",  # reparto richiesto non autorizzato
    ).all()

    assert rows == []


def test_allowed_sets_for_other_dimensions_are_distinct(mod, policy_env):
    from sqlalchemy import insert
    from types import SimpleNamespace

    session = policy_env
    _seed_entities(session)

    session.execute(
        insert(mod.user_roles),
        [
            {"user_id": 1, "role_id": 10},
            {"user_id": 1, "role_id": 11},
        ],
    )

    session.execute(
        insert(mod.roles_risorse),
        [
            {"roles_id": 10, "risorse_id": 1},
            {"roles_id": 11, "risorse_id": 1},
            {"roles_id": 10, "risorse_id": 2},
        ],
    )
    session.execute(
        insert(mod.roles_lavorazioni),
        [
            {"roles_id": 10, "lavorazioni_id": 1},
            {"roles_id": 11, "lavorazioni_id": 1},
            {"roles_id": 10, "lavorazioni_id": 2},
        ],
    )
    session.execute(
        insert(mod.roles_famiglia),
        [
            {"roles_id": 10, "famiglia_id": 1},
            {"roles_id": 11, "famiglia_id": 1},
            {"roles_id": 10, "famiglia_id": 2},
        ],
    )

    # Per macrofamiglia/magazzini usa solo gli id realmente presenti nel seed,
    # così il test resta coerente con i dati disponibili.
    existing_macrofamiglia_ids = [
        x.id
        for x in session.query(mod.Macrofamiglia).order_by(mod.Macrofamiglia.id).all()
    ]
    existing_magazzini_ids = [
        x.id for x in session.query(mod.Magazzini).order_by(mod.Magazzini.id).all()
    ]

    if existing_macrofamiglia_ids:
        payload = [{"roles_id": 10, "macrofamiglia_id": existing_macrofamiglia_ids[0]}]
        if len(existing_macrofamiglia_ids) > 1:
            payload += [
                {"roles_id": 11, "macrofamiglia_id": existing_macrofamiglia_ids[0]},
                {"roles_id": 10, "macrofamiglia_id": existing_macrofamiglia_ids[1]},
            ]
        session.execute(insert(mod.roles_macrofamiglia), payload)

    if existing_magazzini_ids:
        payload = [{"roles_id": 10, "magazzini_id": existing_magazzini_ids[0]}]
        if len(existing_magazzini_ids) > 1:
            payload += [
                {"roles_id": 11, "magazzini_id": existing_magazzini_ids[0]},
                {"roles_id": 10, "magazzini_id": existing_magazzini_ids[1]},
            ]
        session.execute(insert(mod.roles_magazzini), payload)

    session.commit()

    policy = mod.RbacPolicy(SimpleNamespace(id=1))

    expected_risorse = {
        str(x.Codice)
        for x in session.query(mod.Risorse).filter(mod.Risorse.id.in_([1, 2])).all()
    }
    expected_lavorazioni = {
        str(x.Codice)
        for x in session.query(mod.Lavorazioni)
        .filter(mod.Lavorazioni.id.in_([1, 2]))
        .all()
    }
    expected_famiglia = {
        str(x.Codice)
        for x in session.query(mod.Famiglia).filter(mod.Famiglia.id.in_([1, 2])).all()
    }

    expected_macrofamiglia = {
        str(x.Codice)
        for x in session.query(mod.Macrofamiglia)
        .filter(mod.Macrofamiglia.id.in_(existing_macrofamiglia_ids[:2]))
        .all()
    }

    expected_magazzini = {
        str(x.Codice)
        for x in session.query(mod.Magazzini)
        .filter(mod.Magazzini.id.in_(existing_magazzini_ids[:2]))
        .all()
    }

    assert mod._codes(policy.allowed_risorse) == expected_risorse
    assert mod._codes(policy.allowed_lavorazioni) == expected_lavorazioni
    assert mod._codes(policy.allowed_famiglia) == expected_famiglia
    assert mod._codes(policy.allowed_macrofamiglia) == expected_macrofamiglia
    assert mod._codes(policy.allowed_magazzini) == expected_magazzini


def test_require_perm_passes_args_kwargs_return_value_and_current_user(monkeypatch):
    import importlib
    from types import SimpleNamespace

    decorator_mod = importlib.import_module("app_odp.policy.decorator")

    observed = {}
    fake_user = SimpleNamespace(id=123, username="tester")

    class FakePolicy:
        def __init__(self, user):
            observed["user"] = user

        def can(self, code):
            observed["code"] = code
            return True

    monkeypatch.setattr(decorator_mod, "RbacPolicy", FakePolicy)
    monkeypatch.setattr(decorator_mod, "current_user", fake_user)

    @decorator_mod.require_perm("odp.extra")
    def endpoint(a, b=0, *, c=0):
        return a + b + c

    assert endpoint(2, 3, c=4) == 9
    assert observed == {"user": fake_user, "code": "odp.extra"}
