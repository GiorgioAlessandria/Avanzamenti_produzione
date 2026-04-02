from flask import Flask, request, g
from flask_login import LoginManager
from .filters import register_filters

from app_odp.models import db, User
from app_odp.auth import auth_bp
from app_odp.routes import main_bp
import tomllib
from flask_login import current_user
from app_odp.policy.policy import RbacPolicy
from pathlib import Path
import logging
from uuid import uuid4
from sqlalchemy import event
from sqlalchemy.engine import Engine

try:
    from icecream import ic
finally:
    pass
CONFIG_PATH = Path("app_odp/static/config.toml")


def _apply_sqlite_pragmas(engine: Engine) -> None:
    @event.listens_for(engine, "connect")
    def _set_pragmas(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA busy_timeout=5000;")
            cursor.execute("PRAGMA journal_mode=WAL;")
        finally:
            cursor.close()


def load_config(config: Path) -> dict:
    """
    Caricamento e lettura file configurazioni

    :return: Ritorna un dizionario con le configurazioni
    :rtype: dict[Any, Any]
    """
    with config.open("rb") as f:
        return tomllib.load(f)


configurazione = load_config(CONFIG_PATH)


def setup_request_logging(app):
    # livello log
    app.logger.setLevel(logging.INFO)

    @app.before_request
    def _log_request():
        g.rid = uuid4().hex[:8]  # request id breve
        app.logger.info(
            "[%s] %s %s endpoint=%s blueprint=%s ref=%s ip=%s ua=%s",
            g.rid,
            request.method,
            request.full_path,
            request.endpoint,
            request.blueprint,
            request.headers.get("Referer"),
            request.headers.get("X-Forwarded-For", request.remote_addr),
            (request.headers.get("User-Agent") or "")[:120],
        )

    @app.after_request
    def _log_response(resp):
        app.logger.info("[%s] -> %s %s", g.rid, resp.status_code, resp.mimetype)
        return resp


def create_app():
    app = Flask(
        __name__,
        instance_relative_config=True,
        static_folder="static",
        template_folder="templates",
    )
    # setup_request_logging(app)
    app.debug = True

    # chiave segreta per sessioni e Flask-Login
    app.config["SECRET_KEY"] = "Berserk"

    # DB SQLite dentro instance
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        f"sqlite:///{configurazione['Percorsi']['percorso_db']}"
    )
    app.config["SQLALCHEMY_BINDS"] = {
        "log": f"sqlite:///{configurazione['Percorsi']['percorso_db_log']}"
    }
    app.config["ERP_EXPORT_DIR"] = configurazione["Percorsi"]["percorso_file_output"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["DIMENSIONI"] = configurazione["parametri_etichette"]["dimensioni"]
    app.config["DPI"] = configurazione["parametri_etichette"]["dpi"]
    app.config["FONT_PATH"] = configurazione["parametri_etichette"]["font_path"]
    app.config["LAVORAZIONI_RICHIESTA_DISEGNI"] = configurazione[
        "lavorazioni_richiesta_disegni"
    ]["lavorazioni"]
    # inizializza estensioni
    db.init_app(app)
    register_filters(app)
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = "auth.login"

    @login_manager.user_loader
    def load_user(user_id: str):
        return User.query.get(int(user_id))

    @app.context_processor
    def inject_policy():
        if current_user.is_authenticated:
            return {"policy": RbacPolicy(current_user)}
        return {"policy": None}

    with app.app_context():
        for eng in db.engines.values():
            _apply_sqlite_pragmas(eng)
        db.create_all(bind_key="log")
    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    return app
