import importlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from flask import Blueprint

MODULE_PATH = "app_odp.app"


@pytest.fixture()
def mod():
    return importlib.import_module(MODULE_PATH)


def test_load_config_reads_toml(tmp_path, mod):
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        """
[Percorsi]
percorso_db = "db.sqlite"
percorso_db_log = "log.sqlite"
percorso_file_output = "OUT"

[parametri_etichette]
dimensioni = [50, 80]
dpi = 300
font_path = "font.ttf"
""".strip(),
        encoding="utf-8",
    )

    result = mod.load_config(cfg)

    assert result["Percorsi"]["percorso_db"] == "db.sqlite"
    assert result["Percorsi"]["percorso_db_log"] == "log.sqlite"
    assert result["Percorsi"]["percorso_file_output"] == "OUT"
    assert result["parametri_etichette"]["dimensioni"] == [50, 80]
    assert result["parametri_etichette"]["dpi"] == 300
    assert result["parametri_etichette"]["font_path"] == "font.ttf"


def test_apply_sqlite_pragmas_registers_connect_listener(monkeypatch, mod):
    captured = {}

    def fake_listens_for(engine, event_name):
        captured["engine"] = engine
        captured["event_name"] = event_name

        def decorator(fn):
            captured["callback"] = fn
            return fn

        return decorator

    monkeypatch.setattr(mod.event, "listens_for", fake_listens_for)

    fake_engine = object()
    mod._apply_sqlite_pragmas(fake_engine)

    assert captured["engine"] is fake_engine
    assert captured["event_name"] == "connect"

    executed = []

    class FakeCursor:
        def execute(self, sql):
            executed.append(sql)

        def close(self):
            executed.append("CLOSE")

    class FakeDbapiConnection:
        def cursor(self):
            return FakeCursor()

    captured["callback"](FakeDbapiConnection(), object())

    assert executed == [
        "PRAGMA busy_timeout=5000;",
        "PRAGMA journal_mode=WAL;",
        "CLOSE",
    ]


def test_setup_request_logging_logs_request_and_response(monkeypatch, caplog, mod):
    from flask import Flask

    app = Flask(__name__)

    class FakeUuid:
        hex = "abcdef1234567890"

    monkeypatch.setattr(mod, "uuid4", lambda: FakeUuid())
    mod.setup_request_logging(app)

    @app.route("/ping")
    def ping():
        return "ok"

    with caplog.at_level("INFO"):
        resp = app.test_client().get(
            "/ping?x=1",
            headers={
                "Referer": "http://localhost/ref",
                "X-Forwarded-For": "10.0.0.1",
                "User-Agent": "pytest-agent",
            },
        )

    assert resp.status_code == 200
    joined = "\n".join(record.getMessage() for record in caplog.records)
    assert "[abcdef12] GET /ping?x=1" in joined
    assert "ref=http://localhost/ref" in joined
    assert "ip=10.0.0.1" in joined
    assert "ua=pytest-agent" in joined
    assert "[abcdef12] -> 200 text/html" in joined


def test_create_app_configures_app_and_registers_components(monkeypatch, mod):
    calls = {
        "register_filters": 0,
        "apply_pragmas": [],
        "create_all": [],
        "db_init_app": [],
    }

    monkeypatch.setattr(
        mod,
        "configurazione",
        {
            "Percorsi": {
                "percorso_db": "data/main.sqlite",
                "percorso_db_log": "data/log.sqlite",
                "percorso_file_output": "exports",
            },
            "parametri_etichette": {
                "dimensioni": [50, 80],
                "dpi": 300,
                "font_path": "C:/fonts/test.ttf",
            },
        },
    )

    monkeypatch.setattr(
        mod,
        "register_filters",
        lambda app: calls.__setitem__(
            "register_filters", calls["register_filters"] + 1
        ),
    )
    monkeypatch.setattr(
        mod, "_apply_sqlite_pragmas", lambda eng: calls["apply_pragmas"].append(eng)
    )

    class FakeQuery:
        def __init__(self):
            self.requested_ids = []

        def get(self, user_id):
            self.requested_ids.append(user_id)
            return {"user_id": user_id}

    fake_query = FakeQuery()
    monkeypatch.setattr(mod, "User", SimpleNamespace(query=fake_query))

    class FakeDb:
        def __init__(self):
            self.engines = {"default": "ENG1", "log": "ENG2"}

        def init_app(self, app):
            calls["db_init_app"].append(app)

        def create_all(self, bind_key=None):
            calls["create_all"].append(bind_key)

    monkeypatch.setattr(mod, "db", FakeDb())

    class FakeLoginManager:
        last_instance = None

        def __init__(self):
            FakeLoginManager.last_instance = self
            self.login_view = None
            self.app = None
            self.user_loader_fn = None

        def init_app(self, app):
            self.app = app

        def user_loader(self, fn):
            self.user_loader_fn = fn
            return fn

    monkeypatch.setattr(mod, "LoginManager", FakeLoginManager)

    monkeypatch.setattr(mod, "auth_bp", Blueprint("auth", __name__))
    monkeypatch.setattr(mod, "main_bp", Blueprint("main", __name__))

    app = mod.create_app()

    assert app.debug is True
    assert app.config["SECRET_KEY"] == "Berserk"
    assert app.config["SQLALCHEMY_DATABASE_URI"] == "sqlite:///data/main.sqlite"
    assert app.config["SQLALCHEMY_BINDS"] == {"log": "sqlite:///data/log.sqlite"}
    assert app.config["ERP_EXPORT_DIR"] == "exports"
    assert app.config["DIMENSIONI"] == [50, 80]
    assert app.config["DPI"] == 300
    assert app.config["FONT_PATH"] == "C:/fonts/test.ttf"
    assert calls["register_filters"] == 1
    assert calls["apply_pragmas"] == ["ENG1", "ENG2"]
    assert calls["create_all"] == ["log"]
    assert calls["db_init_app"] == [app]
    assert "auth" in app.blueprints
    assert "main" in app.blueprints

    login_manager = FakeLoginManager.last_instance
    assert login_manager is not None
    assert login_manager.app is app
    assert login_manager.login_view == "auth.login"
    assert login_manager.user_loader_fn("7") == {"user_id": 7}
    assert fake_query.requested_ids == [7]


def test_create_app_inject_policy_returns_policy_for_authenticated_user(
    monkeypatch, mod
):
    monkeypatch.setattr(
        mod,
        "configurazione",
        {
            "Percorsi": {
                "percorso_db": "db.sqlite",
                "percorso_db_log": "log.sqlite",
                "percorso_file_output": "out",
            },
            "parametri_etichette": {
                "dimensioni": [10, 20],
                "dpi": 203,
                "font_path": "font.ttf",
            },
        },
    )

    monkeypatch.setattr(mod, "register_filters", lambda app: None)
    monkeypatch.setattr(mod, "_apply_sqlite_pragmas", lambda eng: None)
    monkeypatch.setattr(
        mod, "User", SimpleNamespace(query=SimpleNamespace(get=lambda user_id: None))
    )

    class FakeDb:
        engines = {}

        def init_app(self, app):
            pass

        def create_all(self, bind_key=None):
            pass

    monkeypatch.setattr(mod, "db", FakeDb())

    class FakeLoginManager:
        def init_app(self, app):
            pass

        def user_loader(self, fn):
            return fn

    monkeypatch.setattr(mod, "LoginManager", lambda: FakeLoginManager())
    monkeypatch.setattr(mod, "auth_bp", Blueprint("auth_policy_yes", __name__))
    monkeypatch.setattr(mod, "main_bp", Blueprint("main_policy_yes", __name__))

    fake_user = SimpleNamespace(id=42, is_authenticated=True)
    monkeypatch.setattr(mod, "current_user", fake_user)
    monkeypatch.setattr(mod, "RbacPolicy", lambda user: {"wrapped_user_id": user.id})

    app = mod.create_app()
    injectors = app.template_context_processors[None]
    inject_policy = next(fn for fn in injectors if fn.__name__ == "inject_policy")

    with app.app_context():
        result = inject_policy()

    assert result == {"policy": {"wrapped_user_id": 42}}


def test_create_app_inject_policy_returns_none_for_anonymous_user(monkeypatch, mod):
    monkeypatch.setattr(
        mod,
        "configurazione",
        {
            "Percorsi": {
                "percorso_db": "db.sqlite",
                "percorso_db_log": "log.sqlite",
                "percorso_file_output": "out",
            },
            "parametri_etichette": {
                "dimensioni": [10, 20],
                "dpi": 203,
                "font_path": "font.ttf",
            },
        },
    )

    monkeypatch.setattr(mod, "register_filters", lambda app: None)
    monkeypatch.setattr(mod, "_apply_sqlite_pragmas", lambda eng: None)
    monkeypatch.setattr(
        mod, "User", SimpleNamespace(query=SimpleNamespace(get=lambda user_id: None))
    )

    class FakeDb:
        engines = {}

        def init_app(self, app):
            pass

        def create_all(self, bind_key=None):
            pass

    monkeypatch.setattr(mod, "db", FakeDb())

    class FakeLoginManager:
        def init_app(self, app):
            pass

        def user_loader(self, fn):
            return fn

    monkeypatch.setattr(mod, "LoginManager", lambda: FakeLoginManager())
    monkeypatch.setattr(mod, "auth_bp", Blueprint("auth_policy_no", __name__))
    monkeypatch.setattr(mod, "main_bp", Blueprint("main_policy_no", __name__))

    monkeypatch.setattr(mod, "current_user", SimpleNamespace(is_authenticated=False))
    monkeypatch.setattr(mod, "RbacPolicy", lambda user: {"wrapped_user_id": user.id})

    app = mod.create_app()
    injectors = app.template_context_processors[None]
    inject_policy = next(fn for fn in injectors if fn.__name__ == "inject_policy")

    with app.app_context():
        result = inject_policy()

    assert result == {"policy": None}


import importlib
from types import SimpleNamespace

import pytest
from flask import Blueprint, Flask


MODULE_PATH = "app_odp.app"


@pytest.fixture()
def mod():
    return importlib.import_module(MODULE_PATH)


# ---------------------------------------------------------------------------
# Test aggiuntivi per app_odp.app
# ---------------------------------------------------------------------------


def test_apply_sqlite_pragmas_closes_cursor_even_if_execute_fails(monkeypatch, mod):
    captured = {}

    def fake_listens_for(engine, event_name):
        captured["engine"] = engine
        captured["event_name"] = event_name

        def decorator(fn):
            captured["callback"] = fn
            return fn

        return decorator

    monkeypatch.setattr(mod.event, "listens_for", fake_listens_for)

    fake_engine = object()
    mod._apply_sqlite_pragmas(fake_engine)

    closed = {"value": False}

    class FakeCursor:
        def execute(self, sql):
            raise RuntimeError("dbapi error")

        def close(self):
            closed["value"] = True

    class FakeDbapiConnection:
        def cursor(self):
            return FakeCursor()

    with pytest.raises(RuntimeError, match="dbapi error"):
        captured["callback"](FakeDbapiConnection(), object())

    assert captured["engine"] is fake_engine
    assert captured["event_name"] == "connect"
    assert closed["value"] is True


def test_setup_request_logging_uses_remote_addr_truncates_ua_and_sets_short_rid(
    monkeypatch,
    caplog,
    mod,
):
    app = Flask(__name__)

    class FakeUuid:
        hex = "12345678abcdef009999"

    monkeypatch.setattr(mod, "uuid4", lambda: FakeUuid())
    mod.setup_request_logging(app)

    @app.route("/ping")
    def ping():
        return "ok"

    long_ua = "A" * 200

    with caplog.at_level("INFO"):
        resp = app.test_client().get(
            "/ping",
            headers={
                "Referer": "http://localhost/source",
                "User-Agent": long_ua,
            },
            environ_overrides={"REMOTE_ADDR": "192.168.10.25"},
        )

    assert resp.status_code == 200

    messages = [record.getMessage() for record in caplog.records]
    joined = "\n".join(messages)

    assert "[12345678] GET /ping? endpoint=ping" in joined
    assert "ref=http://localhost/source" in joined
    assert "ip=192.168.10.25" in joined
    assert f"ua={'A' * 120}" in joined
    assert long_ua not in joined
    assert "[12345678] -> 200 text/html" in joined


def test_create_app_user_loader_raises_value_error_for_non_numeric_id(monkeypatch, mod):
    monkeypatch.setattr(
        mod,
        "configurazione",
        {
            "Percorsi": {
                "percorso_db": "db.sqlite",
                "percorso_db_log": "log.sqlite",
                "percorso_file_output": "out",
            },
            "parametri_etichette": {
                "dimensioni": [10, 20],
                "dpi": 203,
                "font_path": "font.ttf",
            },
        },
    )

    monkeypatch.setattr(mod, "register_filters", lambda app: None)
    monkeypatch.setattr(mod, "_apply_sqlite_pragmas", lambda eng: None)

    class FakeQuery:
        def get(self, user_id):
            return {"user_id": user_id}

    monkeypatch.setattr(mod, "User", SimpleNamespace(query=FakeQuery()))

    class FakeDb:
        engines = {}

        def init_app(self, app):
            pass

        def create_all(self, bind_key=None):
            pass

    monkeypatch.setattr(mod, "db", FakeDb())

    class FakeLoginManager:
        last_instance = None

        def __init__(self):
            FakeLoginManager.last_instance = self
            self.user_loader_fn = None
            self.login_view = None

        def init_app(self, app):
            pass

        def user_loader(self, fn):
            self.user_loader_fn = fn
            return fn

    monkeypatch.setattr(mod, "LoginManager", FakeLoginManager)
    monkeypatch.setattr(mod, "auth_bp", Blueprint("auth_loader_invalid", __name__))
    monkeypatch.setattr(mod, "main_bp", Blueprint("main_loader_invalid", __name__))

    mod.create_app()

    loader = FakeLoginManager.last_instance.user_loader_fn
    with pytest.raises(ValueError):
        loader("abc")


def test_create_app_inject_policy_receives_exact_current_user_instance(
    monkeypatch, mod
):
    monkeypatch.setattr(
        mod,
        "configurazione",
        {
            "Percorsi": {
                "percorso_db": "db.sqlite",
                "percorso_db_log": "log.sqlite",
                "percorso_file_output": "out",
            },
            "parametri_etichette": {
                "dimensioni": [10, 20],
                "dpi": 203,
                "font_path": "font.ttf",
            },
        },
    )

    monkeypatch.setattr(mod, "register_filters", lambda app: None)
    monkeypatch.setattr(mod, "_apply_sqlite_pragmas", lambda eng: None)
    monkeypatch.setattr(
        mod, "User", SimpleNamespace(query=SimpleNamespace(get=lambda user_id: None))
    )

    class FakeDb:
        engines = {}

        def init_app(self, app):
            pass

        def create_all(self, bind_key=None):
            pass

    monkeypatch.setattr(mod, "db", FakeDb())

    class FakeLoginManager:
        def init_app(self, app):
            pass

        def user_loader(self, fn):
            return fn

    monkeypatch.setattr(mod, "LoginManager", lambda: FakeLoginManager())
    monkeypatch.setattr(mod, "auth_bp", Blueprint("auth_exact_user", __name__))
    monkeypatch.setattr(mod, "main_bp", Blueprint("main_exact_user", __name__))

    fake_user = SimpleNamespace(id=77, is_authenticated=True)
    received = {"user": None}

    def fake_policy(user):
        received["user"] = user
        return {"ok": True}

    monkeypatch.setattr(mod, "current_user", fake_user)
    monkeypatch.setattr(mod, "RbacPolicy", fake_policy)

    app = mod.create_app()
    injectors = app.template_context_processors[None]
    inject_policy = next(fn for fn in injectors if fn.__name__ == "inject_policy")

    with app.app_context():
        result = inject_policy()

    assert result == {"policy": {"ok": True}}
    assert received["user"] is fake_user
