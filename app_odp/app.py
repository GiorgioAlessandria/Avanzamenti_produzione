import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from flask import Flask, request, g
from flask_login import LoginManager, current_user
from sqlalchemy import event
from sqlalchemy.engine import Engine

from .filters import register_filters
from app_odp.models import db, User
from app_odp.auth import auth_bp
from app_odp.routes import main_bp
from app_odp.policy.policy import RbacPolicy
import tomllib
from uuid import uuid4


APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent
CONFIG_PATH = APP_DIR / "static" / "config.toml"
LOG_DIR = PROJECT_ROOT / "logs"
INSTANCE_DIR = APP_DIR / "instance"
SECRETS_PATH = INSTANCE_DIR / "secrets.toml"


def _apply_sqlite_pragmas(
        engine: Engine
        ) -> None:
    @event.listens_for(engine, "connect")
    def _set_pragmas(
            dbapi_connection,
            connection_record
            ):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA busy_timeout=5000;")
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA foreign_keys=ON;")
        finally:
            cursor.close()


def load_secrets(
        config: Path
        ) -> dict:
    with config.open("rb") as f:
        return tomllib.load(f)


def setup_file_logging(
        app
        ):
    LOG_DIR.mkdir(parents = True, exist_ok = True)

    handler = RotatingFileHandler(
            LOG_DIR / "flask_app.log",
            maxBytes = 5_000_000,
            backupCount = 5,
            encoding = "utf-8",
            )
    handler.setLevel(logging.INFO)
    handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
            )

    if not any(
            isinstance(h, RotatingFileHandler)
            and getattr(h, "baseFilename", "").endswith("flask_app.log")
            for h in app.logger.handlers
            ):
        app.logger.addHandler(handler)

    app.logger.setLevel(logging.INFO)


def load_config(
        config: Path
        ) -> dict:
    """
    Caricamento e lettura file configurazioni

    :return: Ritorna un dizionario con le configurazioni
    :rtype: dict[Any, Any]
    """
    with config.open("rb") as f:
        return tomllib.load(f)


configurazione = load_config(CONFIG_PATH)


def setup_request_logging(
        app
        ):
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
    def _log_response(
            resp
            ):
        app.logger.info("[%s] -> %s %s", g.rid, resp.status_code, resp.mimetype)
        return resp


def create_app():
    app = Flask(
            __name__,
            instance_relative_config = True,
            static_folder = "static",
            template_folder = "templates",
            )
    # setup_request_logging(app)
    app.debug = False

    if not SECRETS_PATH.exists():
        raise RuntimeError(f"File secrets mancante: {SECRETS_PATH}")

    secrets = load_secrets(SECRETS_PATH)
    secret_key = str(secrets.get("SECRET_KEY", "")).strip()
    if not secret_key:
        raise RuntimeError("SECRET_KEY mancante in secrets.toml")

    app.config["SECRET_KEY"] = secret_key

    # DB SQLite dentro instance
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        f"sqlite:///{configurazione['Percorsi']['percorso_db']}"
    )
    app.config["SQLALCHEMY_BINDS"] = {
        "log": f"sqlite:///{configurazione['Percorsi']['percorso_db_log']}",
        "acq": f"sqlite:///{configurazione['Percorsi']['percorso_db_acq']}",
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
    def load_user(
            user_id: str
            ):
        return User.query.get(int(user_id))


    @app.context_processor
    def inject_policy():
        if current_user.is_authenticated:
            return {"policy": RbacPolicy(current_user)}
        return {"policy": None}


    with app.app_context():
        for eng in db.engines.values():
            _apply_sqlite_pragmas(eng)

        db.create_all()
        db.create_all(bind_key = "log")
        db.create_all(bind_key = "acq")

    setup_file_logging(app)
    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    return app
