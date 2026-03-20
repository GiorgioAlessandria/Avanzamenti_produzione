import importlib
import sys
from types import SimpleNamespace

import pytest
from flask import Blueprint, Flask
from flask_login import LoginManager
from pathlib import Path

MODULE_PATH = "app_odp.auth"

# Indice test
# 1. test_load_user_returns_user_from_query_get:
#    verifica che load_user converta l'id in int e richiami User.query.get.
# 2. test_load_user_raises_value_error_for_non_numeric_id:
#    verifica l'errore con user_id non numerico.
# 3. test_load_user_returns_none_when_numeric_id_not_found:
#    verifica il caso di id numerico valido ma assente in query.
# 4. test_user_in_returns_only_active_non_admin_users_sorted:
#    verifica filtro active=True, esclusione admin case-insensitive e ordinamento.
# 5. test_user_in_returns_user_objects_not_strings:
#    verifica che user_in restituisca oggetti utente e non username stringa.
# 6. test_login_get_renders_login_template_with_active_users:
#    verifica il rendering GET con la lista utenti attivi non-admin.
# 7. test_login_get_real_template_contains_username_select_contract:
#    verifica il contratto minimo del template reale login.j2.
# 8. test_login_post_without_username_returns_selection_error:
#    verifica l'errore quando username manca o è vuoto.
# 9. test_login_post_invalid_user_returns_invalid_user_error:
#    verifica il ramo con utente inesistente/non valido.
# 10. test_login_post_whitespace_username_returns_invalid_user_error:
#     verifica che username composto solo da spazi non entri nel ramo "Seleziona un utente".
# 11. test_login_post_inactive_user_returns_invalid_user_error:
#     verifica che un utente inattivo non possa autenticarsi.
# 12. test_login_post_valid_user_logs_in_and_redirects_home:
#     verifica login_user e redirect verso main.home.
# 13. test_logout_calls_logout_user_and_redirects_to_login:
#     verifica logout_user e redirect verso auth.login.
# 14. test_logout_requires_login_with_real_client:
#     verifica che /logout da anonimo sia protetto da login_required.


@pytest.fixture()
def mod(monkeypatch):
    """
    Importa il modulo sotto test predisponendo una dipendenza fittizia per `icecream`.
    """
    monkeypatch.setitem(
        sys.modules,
        "icecream",
        SimpleNamespace(ic=lambda *args, **kwargs: None),
    )
    sys.modules.pop(MODULE_PATH, None)
    return importlib.import_module(MODULE_PATH)


class FakeUserRow:
    def __init__(self, user_id, username, active=True):
        self.id = user_id
        self.username = username
        self.active = active


class FakeQuery:
    def __init__(self, rows=None):
        self._rows = list(rows or [])

    def get(self, value):
        for row in self._rows:
            if getattr(row, "id", None) == value:
                return row
        return None

    def filter_by(self, **kwargs):
        def _match(row, key, expected):
            current = getattr(row, key, None)
            return current == expected or str(current) == str(expected)

        filtered = [
            row
            for row in self._rows
            if all(_match(row, key, value) for key, value in kwargs.items())
        ]
        return FakeQuery(filtered)

    def order_by(self, _expr):
        return FakeQuery(sorted(self._rows, key=lambda row: str(row.username).lower()))

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


@pytest.fixture()
def install_fake_user_model(mod, monkeypatch):
    class FakeUserModel:
        username = "username"
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
        users = context.get("users", [])
        error = context.get("error", "")
        return f"template={template_name};users={len(users)};error={error}"

    monkeypatch.setattr(mod, "render_template", fake_render)
    return calls


@pytest.fixture()
def app(mod):
    app = Flask(__name__)
    app.config.update(TESTING=True, SECRET_KEY="test-secret", LOGIN_DISABLED=True)

    LoginManager(app)

    main_bp = Blueprint("main", __name__)

    @main_bp.route("/", endpoint="home")
    def home():
        return "home"

    app.register_blueprint(main_bp)
    app.register_blueprint(mod.auth_bp)
    return app


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def app_with_templates(mod):
    project_root = Path(__file__).resolve().parents[2]
    template_dir = project_root / "app_odp" / "templates"

    app = Flask(
        __name__,
        template_folder=str(template_dir),
    )
    app.config.update(TESTING=True, SECRET_KEY="test-secret", LOGIN_DISABLED=True)

    login_manager = LoginManager(app)

    @login_manager.user_loader
    def _load_user(_user_id):
        return None

    main_bp = Blueprint("main", __name__)

    @main_bp.route("/", endpoint="home")
    def home():
        return "home"

    app.register_blueprint(main_bp)
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

    main_bp = Blueprint("main", __name__)

    @main_bp.route("/", endpoint="home")
    def home():
        return "home"

    app.register_blueprint(main_bp)
    app.register_blueprint(mod.auth_bp)
    return app


@pytest.fixture()
def client_auth_required(app_auth_required):
    return app_auth_required.test_client()


def test_load_user_returns_user_from_query_get(mod, install_fake_user_model):
    user = FakeUserRow(7, "giorgio", active=True)
    install_fake_user_model([user])

    got = mod.load_user("7")

    assert got is user


def test_load_user_raises_value_error_for_non_numeric_id(mod, install_fake_user_model):
    install_fake_user_model([])

    with pytest.raises(ValueError):
        mod.load_user("abc")


def test_load_user_returns_none_when_numeric_id_not_found(mod, install_fake_user_model):
    install_fake_user_model([FakeUserRow(1, "alpha", active=True)])

    got = mod.load_user("999")

    assert got is None


def test_user_in_returns_only_active_non_admin_users_sorted(
    mod, install_fake_user_model
):
    rows = [
        FakeUserRow(3, "bravo", active=True),
        FakeUserRow(1, "AdMiN", active=True),
        FakeUserRow(4, "charlie", active=False),
        FakeUserRow(2, "alpha", active=True),
    ]
    install_fake_user_model(rows)

    got = mod.user_in()

    assert [user.username for user in got] == ["alpha", "bravo"]


def test_user_in_returns_user_objects_not_strings(mod, install_fake_user_model):
    install_fake_user_model([FakeUserRow(2, "alpha", active=True)])

    got = mod.user_in()

    assert len(got) == 1
    assert isinstance(got[0], FakeUserRow)
    assert not isinstance(got[0], str)


def test_login_get_renders_login_template_with_active_users(
    client, mod, install_fake_user_model, render_spy
):
    rows = [
        FakeUserRow(10, "zeta", active=True),
        FakeUserRow(20, "admin", active=True),
        FakeUserRow(30, "beta", active=True),
        FakeUserRow(40, "ghost", active=False),
    ]
    install_fake_user_model(rows)

    response = client.get("/login")

    assert response.status_code == 200
    assert response.get_data(as_text=True).startswith("template=login.j2")
    template_name, context = render_spy[-1]
    assert template_name == "login.j2"
    assert [user.username for user in context["users"]] == ["beta", "zeta"]
    assert "error" not in context


def test_login_get_real_template_contains_username_select_contract(
    client_with_templates,
    mod,
    install_fake_user_model,
):
    rows = [
        FakeUserRow(10, "zeta", active=True),
        FakeUserRow(20, "admin", active=True),
        FakeUserRow(30, "beta", active=True),
    ]
    install_fake_user_model(rows)

    response = client_with_templates.get("/login")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert '<select id="username" name="username"' in html
    assert "-- Scegli un utente --" in html
    assert "beta" in html
    assert "zeta" in html
    assert "admin" not in html


def test_login_post_without_username_returns_selection_error(
    client, mod, install_fake_user_model, render_spy, monkeypatch
):
    install_fake_user_model([FakeUserRow(1, "utente", active=True)])
    login_calls = []
    monkeypatch.setattr(mod, "login_user", lambda user: login_calls.append(user))

    response = client.post("/login", data={})

    assert response.status_code == 200
    assert login_calls == []
    template_name, context = render_spy[-1]
    assert template_name == "login.j2"
    assert context["error"] == "Seleziona un utente"


def test_login_post_invalid_user_returns_invalid_user_error(
    client, mod, install_fake_user_model, render_spy, monkeypatch
):
    install_fake_user_model([FakeUserRow(1, "utente", active=True)])
    login_calls = []
    monkeypatch.setattr(mod, "login_user", lambda user: login_calls.append(user))

    response = client.post("/login", data={"username": "999"})

    assert response.status_code == 200
    assert login_calls == []
    template_name, context = render_spy[-1]
    assert template_name == "login.j2"
    assert context["error"] == "Utente non valido"


def test_login_post_whitespace_username_returns_invalid_user_error(
    client,
    mod,
    install_fake_user_model,
    render_spy,
    monkeypatch,
):
    install_fake_user_model([FakeUserRow(1, "utente", active=True)])
    login_calls = []
    monkeypatch.setattr(mod, "login_user", lambda user: login_calls.append(user))

    response = client.post("/login", data={"username": "   "})

    assert response.status_code == 200
    assert login_calls == []
    template_name, context = render_spy[-1]
    assert template_name == "login.j2"
    assert context["error"] == "Utente non valido"


def test_login_post_inactive_user_returns_invalid_user_error(
    client, mod, install_fake_user_model, render_spy, monkeypatch
):
    install_fake_user_model([FakeUserRow(5, "utente-inattivo", active=False)])
    login_calls = []
    monkeypatch.setattr(mod, "login_user", lambda user: login_calls.append(user))

    response = client.post("/login", data={"username": "5"})

    assert response.status_code == 200
    assert login_calls == []
    template_name, context = render_spy[-1]
    assert template_name == "login.j2"
    assert context["error"] == "Utente non valido"


def test_login_post_valid_user_logs_in_and_redirects_home(
    client, mod, install_fake_user_model, monkeypatch
):
    user = FakeUserRow(9, "giorgio", active=True)
    install_fake_user_model([user])
    login_calls = []
    monkeypatch.setattr(
        mod,
        "login_user",
        lambda current_user: login_calls.append(current_user),
    )

    response = client.post("/login", data={"username": "9"})

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")
    assert login_calls == [user]


def test_logout_calls_logout_user_and_redirects_to_login(client, mod, monkeypatch):
    calls = []
    monkeypatch.setattr(mod, "logout_user", lambda: calls.append("logout"))

    response = client.get("/logout")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/login")
    assert calls == ["logout"]


def test_logout_requires_login_with_real_client(client_auth_required):
    response = client_auth_required.get("/logout")

    assert response.status_code == 302
    assert "/login" in response.headers["Location"]
    assert "next=" in response.headers["Location"]
