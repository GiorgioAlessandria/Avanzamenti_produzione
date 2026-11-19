from contextlib import closing
import sqlite3
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

from flask import Blueprint, Flask, jsonify
import pytest

from app_odp import production_sandbox
from app_odp.models import BrowserTabSession, ErpOutbox, InputOdpRuntime, Roles, User, db
from app_odp.operator_session import create_operator_session
from app_odp.services.erp_export_service import (
    _build_export_txt_path,
    _publish_suspension_export,
    _queue_phase_export,
)


def test_sandbox_uses_copies_and_blocks_external_actions(tmp_path, monkeypatch):
    live_paths = {None: tmp_path / "live.sqlite", "log": tmp_path / "live-log.sqlite"}
    monkeypatch.setattr(production_sandbox, "gettempdir", lambda: str(tmp_path))

    def create_app(*, sandbox_db_paths=None):
        app = Flask(__name__)
        paths = sandbox_db_paths or live_paths
        app.config.update(
            TESTING=True,
            SECRET_KEY="test",
            SQLALCHEMY_DATABASE_URI=f"sqlite:///{paths[None]}",
            SQLALCHEMY_BINDS={"log": f"sqlite:///{paths['log']}"},
            PRODUCTION_SANDBOX=sandbox_db_paths is not None,
        )
        db.init_app(app)
        main = Blueprint("main", __name__)

        @main.get("/")
        def home():
            return jsonify(stato=InputOdpRuntime.query.filter_by(IdDocumento="42", IdRiga="1").one().Stato_odp)

        @main.post("/api/ordini/chiudi")
        def api_chiudi_ordine():
            InputOdpRuntime.query.filter_by(IdDocumento="42", IdRiga="1").one().Stato_odp = "Chiusa"
            ordine = SimpleNamespace(IdDocumento="42", IdRiga="1", RifRegistraz="42", CodArt="A")
            outbox = _queue_phase_export(ordine, "1", {})
            db.session.commit()
            return jsonify(outbox_id=outbox.outbox_id)

        @main.post("/api/erp/export/avp")
        def forbidden():
            raise AssertionError("L'endpoint di export non deve essere eseguito in sandbox.")

        app.register_blueprint(main)
        return app

    live = create_app()
    with live.test_request_context():
        db.create_all(bind_key=None)
        ErpOutbox.__table__.create(db.engines["log"])
        admin = User(username="admin", roles=[Roles(name="admin")])
        other = User(username="operatore")
        db.session.add_all([admin, other])
        db.session.add(InputOdpRuntime(IdDocumento="42", IdRiga="1", Stato_odp="Attivo"))
        db.session.flush()
        admin_id = admin.id
        live_token = create_operator_session(admin)
        other_token = create_operator_session(other)
    production_sandbox.install_production_sandbox(live, create_app)
    client = live.test_client()
    assert client.post("/admin/sandbox/attiva", data={"tab_session": other_token}).status_code == 403
    start = client.post("/admin/sandbox/attiva", data={"tab_session": live_token})
    assert start.status_code == 303
    url = urlsplit(start.location)
    root = url.path.rstrip("/")
    token = parse_qs(url.query)["tab_session"][0]
    assert token != live_token
    assert client.get(f"{root}/?tab_session={live_token}").status_code == 403
    assert client.get(start.location).json == {"stato": "Attivo"}
    headers = {"X-Tab-Session": token}
    close = client.post(f"{root}/api/ordini/chiudi", headers=headers)
    assert close.json == {"outbox_id": None}
    assert client.get(start.location).json == {"stato": "Chiusa"}
    assert client.post(f"{root}/api/erp/export/avp", headers=headers).status_code == 403
    with live.app_context():
        assert db.session.get(User, admin_id).username == "admin"
        assert InputOdpRuntime.query.filter_by(IdDocumento="42", IdRiga="1").one().Stato_odp == "Attivo"
        assert BrowserTabSession.query.count() == 2
        assert ErpOutbox.query.count() == 0
    sandbox_log = next(tmp_path.glob("avp-production-sandbox-*/*/log.sqlite"))
    with closing(sqlite3.connect(f"{sandbox_log.as_uri()}?mode=ro", uri=True)) as conn:
        assert conn.execute("SELECT count(*) FROM erp_outbox").fetchone() == (0,)
    stopped = client.post(f"{root}/termina", headers=headers, buffered=True)
    assert stopped.status_code == 200
    assert stopped.json == {"ok": True}
    stopped.close()
    assert not sandbox_log.exists()
    assert client.post(f"{root}/api/ordini/chiudi", headers=headers).status_code == 410
    assert client.get("/").json == {"stato": "Attivo"}


def test_sandbox_blocks_file_generation_before_opening_database_or_directory(tmp_path):
    app = Flask(__name__)
    export_dir = tmp_path / "export"
    app.config.update(PRODUCTION_SANDBOX=True, ERP_EXPORT_DIR=str(export_dir))
    with app.app_context():
        with pytest.raises(RuntimeError, match="AVP disattivata"):
            _build_export_txt_path()
        with pytest.raises(RuntimeError, match="AVP disattivato"):
            _publish_suspension_export(1)
    assert not export_dir.exists()
