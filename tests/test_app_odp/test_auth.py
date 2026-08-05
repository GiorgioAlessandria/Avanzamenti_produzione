import hashlib
import importlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from flask import Blueprint, Flask
from flask_login import LoginManager

MODULE_PATH = "app_odp.auth"


@pytest.fixture()
def mod():
    return importlib.import_module(MODULE_PATH)


def _lookup(code):
    return hashlib.sha256(code.strip().upper().encode("utf-8")).hexdigest()


class FakeUserRow:
    def __init__(
        self,
        user_id,
        username="utente",
        active=True,
        login_code="ABC123",
        accepts_code=True,
        permissions=(),
    ):
        self.id = user_id
        self.username = username
        self.active = active
        self.login_code = login_code.strip().upper()
        self.login_code_lookup = _lookup(self.login_code)
        self.accepts_code = accepts_code
        self.permissions = set(permissions)

    def check_login_code(self, value):
        return self.accepts_code and value == self.login_code


class FakeQuery:
    def __init__(self, rows=None):
        self._rows = list(rows or [])

    def filter_by(self, **kwargs):
        return FakeQuery(
            row
            for row in self._rows
            if all(getattr(row, key, None) == value for key, value in kwargs.items())
        )

    def first(self):
        return self._rows[0] if self._rows else None


@pytest.fixture(autouse=True)
def fake_policy(mod, monkeypatch):
    class FakePolicy:
        def __init__(self, user):
            self.user = user

        def can(self, permission):
            return permission in getattr(self.user, "permissions", set())

    monkeypatch.setattr(mod, "RbacPolicy", FakePolicy)


@pytest.fixture()
def install_fake_user_model(mod, monkeypatch):
    class FakeUserModel:
        query = FakeQuery([])

    def _install(rows):
        FakeUserModel.query = FakeQuery(rows)
        monkeypatch.setattr(mod, "User", FakeUserModel)
        return FakeUserModel

    return _install


@pytest.fixture()
def render_spy(mod, monkeypatch):
    calls = []

    def fake_render(template_name, **context):
        calls.append((template_name, context))
        return f"template={template_name};error={context.get('error', '')}"

    monkeypatch.setattr(mod, "render_template", fake_render)
    return calls


def _register_main_routes(app):
    main_bp = Blueprint("main", __name__)

    @main_bp.route("/", endpoint="home")
    def home():
        return "home"

    @main_bp.route("/acquisti", endpoint="home_acquisti")
    def home_acquisti():
        return "home_acquisti"

    @main_bp.route("/vendite", endpoint="vendite_page")
    def vendite_page():
        return "vendite"

    @main_bp.route("/carichi-scarichi", endpoint="logistica_page")
    def logistica_page():
        return "logistica"

    app.register_blueprint(main_bp)


@pytest.fixture()
def app(mod):
    app = Flask(__name__)
    app.config.update(TESTING=True, SECRET_KEY="test-secret", LOGIN_DISABLED=True)

    login_manager = LoginManager(app)

    @login_manager.user_loader
    def _load_user(_user_id):
        return None

    _register_main_routes(app)
    app.register_blueprint(mod.auth_bp)
    return app


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def app_with_templates(mod):
    project_root = Path(__file__).resolve().parents[2]
    template_dir = project_root / "app_odp" / "templates"

    app = Flask(__name__, template_folder=str(template_dir))
    app.config.update(TESTING=True, SECRET_KEY="test-secret", LOGIN_DISABLED=True)

    login_manager = LoginManager(app)

    @login_manager.user_loader
    def _load_user(_user_id):
        return None

    _register_main_routes(app)
    app.register_blueprint(mod.auth_bp)
    return app


@pytest.fixture()
def client_with_templates(app_with_templates):
    return app_with_templates.test_client()


@pytest.fixture()
def app_auth_required(mod):
    app = Flask(__name__)
    app.config.update(TESTING=True, SECRET_KEY="test-secret", LOGIN_DISABLED=False)

    login_manager = LoginManager(app)
    login_manager.login_view = "auth.login"

    @login_manager.user_loader
    def _load_user(_user_id):
        return None

    _register_main_routes(app)
    app.register_blueprint(mod.auth_bp)
    return app


@pytest.fixture()
def client_auth_required(app_auth_required):
    return app_auth_required.test_client()


def test_login_get_renders_login_template(client, render_spy):
    response = client.get("/login")

    assert response.status_code == 200
    assert response.get_data(as_text=True) == "template=login.j2;error="
    assert render_spy == [("login.j2", {})]


def test_login_get_real_template_uses_login_code_input(client_with_templates):
    response = client_with_templates.get("/login")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'id="login_code"' in html
    assert 'name="login_code"' in html
    assert 'type="password"' in html
    assert 'id="username"' not in html


def test_login_post_without_code_returns_required_error(client, render_spy):
    response = client.post("/login", data={})

    assert response.status_code == 400
    assert render_spy[-1] == (
        "login.j2",
        {"error": "Inserisci il codice di accesso."},
    )


def test_login_post_unknown_code_returns_invalid_error(
    client, install_fake_user_model, render_spy, mod, monkeypatch
):
    install_fake_user_model([])
    login_calls = []
    monkeypatch.setattr(mod, "login_user", lambda user: login_calls.append(user))

    response = client.post("/login", data={"login_code": "ABC123"})

    assert response.status_code == 401
    assert login_calls == []
    assert render_spy[-1] == (
        "login.j2",
        {"error": "Codice di accesso non valido."},
    )


def test_login_post_rejects_user_when_hash_check_fails(
    client, install_fake_user_model, render_spy, mod, monkeypatch
):
    user = FakeUserRow(1, accepts_code=False)
    install_fake_user_model([user])
    login_calls = []
    monkeypatch.setattr(mod, "login_user", lambda current_user: login_calls.append(current_user))

    response = client.post("/login", data={"login_code": "ABC123"})

    assert response.status_code == 401
    assert login_calls == []
    assert render_spy[-1] == (
        "login.j2",
        {"error": "Codice di accesso non valido."},
    )


def test_login_post_acquisti_and_produzione_redirects_to_acquisti_with_session(
    client, install_fake_user_model, mod, monkeypatch
):
    user = FakeUserRow(9, permissions={"home_acquisti", "home"})
    install_fake_user_model([user])
    login_calls = []
    session_calls = []
    monkeypatch.setattr(mod, "login_user", lambda current_user: login_calls.append(current_user))
    monkeypatch.setattr(
        mod,
        "create_operator_session",
        lambda current_user: session_calls.append(current_user) or "tok-9",
    )

    response = client.post("/login", data={"login_code": " abc123 "})

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/acquisti?tab_session=tok-9")
    assert login_calls == [user]
    assert session_calls == [user]


def test_login_post_acquisti_only_redirects_without_operator_session(
    client, install_fake_user_model, mod, monkeypatch
):
    user = FakeUserRow(2, permissions={"home_acquisti"})
    install_fake_user_model([user])
    login_calls = []
    monkeypatch.setattr(mod, "login_user", lambda current_user: login_calls.append(current_user))
    monkeypatch.setattr(
        mod,
        "create_operator_session",
        lambda current_user: pytest.fail("sessione operatore non attesa"),
    )

    response = client.post("/login", data={"login_code": "ABC123"})

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/acquisti")
    assert login_calls == [user]


def test_login_post_produzione_only_redirects_to_home_with_session(
    client, install_fake_user_model, mod, monkeypatch
):
    user = FakeUserRow(3, permissions={"home"})
    install_fake_user_model([user])
    login_calls = []
    session_calls = []
    monkeypatch.setattr(mod, "login_user", lambda current_user: login_calls.append(current_user))
    monkeypatch.setattr(
        mod,
        "create_operator_session",
        lambda current_user: session_calls.append(current_user) or "tok-3",
    )

    response = client.post("/login", data={"login_code": "ABC123"})

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/?tab_session=tok-3")
    assert login_calls == []
    assert session_calls == [user]


def test_login_post_vendite_only_redirects_without_operator_session(
    client, install_fake_user_model, mod, monkeypatch
):
    user = FakeUserRow(5, permissions={"vendite"})
    install_fake_user_model([user])
    login_calls = []
    monkeypatch.setattr(mod, "login_user", lambda current_user: login_calls.append(current_user))
    monkeypatch.setattr(
        mod,
        "create_operator_session",
        lambda current_user: pytest.fail("sessione operatore non attesa"),
    )

    response = client.post("/login", data={"login_code": "ABC123"})

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/vendite")
    assert login_calls == [user]


def test_login_post_carica_uses_logistica_as_main_page(
    client, install_fake_user_model, mod, monkeypatch
):
    user = FakeUserRow(30, permissions={"carica", "home", "home_acquisti"})
    install_fake_user_model([user])
    login_calls = []
    session_calls = []
    monkeypatch.setattr(mod, "login_user", lambda current_user: login_calls.append(current_user))
    monkeypatch.setattr(
        mod,
        "create_operator_session",
        lambda current_user: session_calls.append(current_user) or "tok-30",
    )

    response = client.post("/login", data={"login_code": "ABC123"})

    assert response.status_code == 302
    assert response.headers["Location"].endswith(
        "/carichi-scarichi?tab_session=tok-30"
    )
    assert login_calls == []
    assert session_calls == [user]


def test_login_post_ricezione_only_redirects_to_logistica(
    client, install_fake_user_model, mod, monkeypatch
):
    user = FakeUserRow(31, permissions={"ricezione"})
    install_fake_user_model([user])
    login_calls = []
    session_calls = []
    monkeypatch.setattr(mod, "login_user", lambda current_user: login_calls.append(current_user))
    monkeypatch.setattr(
        mod,
        "create_operator_session",
        lambda current_user: session_calls.append(current_user) or "tok-31",
    )

    response = client.post("/login", data={"login_code": "ABC123"})

    assert response.status_code == 302
    assert response.headers["Location"].endswith(
        "/carichi-scarichi?tab_session=tok-31"
    )
    assert login_calls == []
    assert session_calls == [user]


def test_login_post_user_without_permissions_returns_403(
    client, install_fake_user_model, render_spy, mod, monkeypatch
):
    user = FakeUserRow(4)
    install_fake_user_model([user])
    monkeypatch.setattr(mod, "login_user", lambda current_user: pytest.fail("login non atteso"))
    monkeypatch.setattr(
        mod,
        "create_operator_session",
        lambda current_user: pytest.fail("sessione operatore non attesa"),
    )

    response = client.post("/login", data={"login_code": "ABC123"})

    assert response.status_code == 403
    assert render_spy[-1] == (
        "login.j2",
        {"error": "Utente senza permessi di accesso."},
    )


def test_login_get_authenticated_user_redirects(client, mod, monkeypatch):
    user = SimpleNamespace(is_authenticated=True, permissions={"home_acquisti"})
    monkeypatch.setattr(mod, "current_user", user)

    response = client.get("/login")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/acquisti")


def test_logout_closes_both_sessions_and_redirects_to_login(
    client, mod, monkeypatch
):
    calls = []
    row = SimpleNamespace(user_id=77)
    monkeypatch.setattr(mod, "resolve_operator_session", lambda: row)
    monkeypatch.setattr(
        mod,
        "revoke_operator_sessions_for_user",
        lambda user_id: calls.append(("revoke", user_id)),
    )
    monkeypatch.setattr(mod, "logout_user", lambda: calls.append("logout"))

    response = client.get("/logout")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/login")
    assert calls == [("revoke", 77), "logout"]


def test_logout_is_available_without_an_active_flask_session(client_auth_required):
    response = client_auth_required.get("/logout")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/login")
