from flask import Flask, request, g, url_for
from flask_login import LoginManager
from .filters import register_filters
from app_odp.operator_session import active_user, active_policy, active_token
from app_odp.models import db, Permissions, User
from app_odp import manutenzioni_models, rifiuti_models, tarature_models
from app_odp.auth import auth_bp
from app_odp.routes import main_bp
import tomllib
from flask_login import current_user
from app_odp.policy.policy import RbacPolicy
from pathlib import Path
import logging
from uuid import uuid4
from sqlalchemy import event, inspect
from sqlalchemy.engine import Engine

CONFIG_PATH = Path("app_odp/static/config.toml")


def _apply_sqlite_pragmas(engine: Engine) -> None:
    @event.listens_for(engine, "connect")
    def _set_pragmas(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA busy_timeout=5000;")
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA foreign_keys=ON;")
        finally:
            cursor.close()


def _ensure_manutenzioni_schema() -> None:
    engine = db.engines["manutenzioni"]
    columns = {
        column["name"]
        for column in inspect(engine).get_columns(
            "manutenzioni_ricorrenti"
        )
    }

    if "archiviata" not in columns:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "ALTER TABLE manutenzioni_ricorrenti "
                "ADD COLUMN archiviata BOOLEAN NOT NULL DEFAULT 0"
            )

    straordinarie_columns = {
        column["name"]
        for column in inspect(engine).get_columns(
            "manutenzioni_straordinarie"
        )
    }

    with engine.begin() as connection:
        if "evento_manutenzione_id" not in straordinarie_columns:
            connection.exec_driver_sql(
                "ALTER TABLE manutenzioni_straordinarie "
                "ADD COLUMN evento_manutenzione_id INTEGER "
                "REFERENCES eventi_manutenzione(id) ON DELETE SET NULL"
            )

        connection.exec_driver_sql(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "uq_manutenzioni_straordinarie_evento "
            "ON manutenzioni_straordinarie (evento_manutenzione_id) "
            "WHERE evento_manutenzione_id IS NOT NULL"
        )

    strumenti_columns = {
        column["name"]
        for column in inspect(engine).get_columns("strumenti_misura")
    }
    if "costruttore" not in strumenti_columns:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "ALTER TABLE strumenti_misura ADD COLUMN costruttore TEXT"
            )
    if "solo_verifica_interna" not in strumenti_columns:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "ALTER TABLE strumenti_misura ADD COLUMN "
                "solo_verifica_interna BOOLEAN NOT NULL DEFAULT 0"
            )

    tipologie_columns = {
        column["name"]
        for column in inspect(engine).get_columns("tipologie_strumento")
    }
    if "taratura_esterna_attiva" not in tipologie_columns:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "ALTER TABLE tipologie_strumento ADD COLUMN "
                "taratura_esterna_attiva BOOLEAN NOT NULL DEFAULT 1"
            )


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


def _ensure_builtin_permissions() -> None:
    builtins = {
        "storico_ordini": "Storico ordini",
        "scorte_segnalazione_libera": ("Segnalazione scorte con testo libero"),
        # Rifiuti
        "rifiuti_carica": (
            "Caricamento dei rifiuti nello stock virtuale"
        ),
        "rifiuti_elimina": (
            "Registrazione dello smaltimento dei rifiuti dallo stock virtuale"
        ),
        # Manutenzioni
        "manutenzioni_visualizza": ("Accesso alla gestione delle manutenzioni"),
        "manutenzioni_gestisci_macchinari": (
            "Creazione e modifica dell'anagrafica macchinari"
        ),
        "manutenzioni_visualizza_tutti_reparti": (
            "Visualizzazione dei macchinari di tutti i reparti"
        ),
        "manutenzioni_gestisci_piani": (
            "Creazione e modifica dei piani di manutenzione"
        ),
        "manutenzioni_esegui": ("Registrazione delle manutenzioni eseguite"),
        "manutenzioni_visualizza_registro": (
            "Visualizzazione del registro delle manutenzioni"
        ),
        "manutenzioni_amministrazione": ("Amministrazione completa delle manutenzioni"),
        # Tarature
        "tarature": ("Gestione delle tarature degli strumenti di misura"),
    }
    existing = {
        row.Codice
        for row in Permissions.query.filter(Permissions.Codice.in_(builtins.keys()))
    }
    for code, description in builtins.items():
        if code not in existing:
            db.session.add(Permissions(Codice=code, Descrizione=description))
    db.session.commit()


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
        "log": f"sqlite:///{configurazione['Percorsi']['percorso_db_log']}",
        "acq": f"sqlite:///{configurazione['Percorsi']['percorso_db_acq']}",
        "manutenzioni": (
            f"sqlite:///{configurazione['Percorsi']['percorso_db_manutenzioni']}"
        ),
        "rifiuti": f"sqlite:///{configurazione['Percorsi']['percorso_db_rifiuti']}",
    }
    app.config["ERP_EXPORT_DIR"] = configurazione["Percorsi"]["percorso_file_output"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["DIMENSIONI"] = configurazione["parametri_etichette"]["dimensioni"]
    app.config["DPI"] = configurazione["parametri_etichette"]["dpi"]
    app.config["FONT_PATH"] = configurazione["parametri_etichette"]["font_path"]
    app.config["LAVORAZIONI_RICHIESTA_DISEGNI"] = configurazione[
        "lavorazioni_richiesta_disegni"
    ]["lavorazioni"]
    app.config["MONTAGGIO_PDF_DIR"] = configurazione["Percorsi"][
        "percorso_metodi_montaggio"
    ]
    app.config["COLLAUDO_PDF_DIR"] = configurazione["Percorsi"].get(
        "percorso_metodi_collaudo",
        "",
    )
    app.config["FOTOGRAFIE_MATERIALE"] = configurazione["Percorsi"][
        "percorso_fotografie_materiale"
    ]
    app.config["ETICHETTE_OUTPUT_DIR"] = configurazione["Percorsi"][
        "percorso_etichette_generate"
    ]
    app.config["LABEL_PRINTER_NAME"] = configurazione.get(
        "parametri_etichette", {}
    ).get(
        "nome_stampante",
        "",
    )
    app.config["METODO_UTILIZZO_DIR"] = configurazione["Percorsi"][
        "percorso_metodo_utilizzo"
    ]
    label_params = configurazione.get("parametri_etichette", {})

    app.config["LABEL_PRINT_ROTATION"] = int(label_params.get("stampa_rotazione", 0))
    app.config["LABEL_PRINT_OFFSET_X_MM"] = float(
        label_params.get("stampa_offset_x_mm", 0.0)
    )
    app.config["LABEL_PRINT_OFFSET_Y_MM"] = float(
        label_params.get("stampa_offset_y_mm", 0.0)
    )
    app.config["LABEL_PRINT_SCALE"] = float(label_params.get("stampa_scala", 1.0))
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
        try:
            request_token = (request.args.get("tab_session") or "").strip()
            active_request_token = active_token()
            token = request_token or active_request_token

            user = active_user()

            if getattr(user, "is_authenticated", False):
                policy_obj = active_policy()
            else:
                policy_obj = None

            def operator_url_for(endpoint, **values):
                current_request_token = (request.args.get("tab_session") or "").strip()
                current_active_token = active_token()
                current_token = current_request_token or current_active_token

                if current_token and "tab_session" not in values:
                    values["tab_session"] = current_token

                return url_for(endpoint, **values)

            return {
                "policy": policy_obj,
                "operator_user": user if token else None,
                "operator_policy": policy_obj if token else None,
                "tab_session": token,
                "operator_url_for": operator_url_for,
            }

        except Exception:
            return {
                "policy": None,
                "operator_user": None,
                "operator_policy": None,
                "tab_session": "",
                "operator_url_for": url_for,
            }

    with app.app_context():
        for eng in db.engines.values():
            _apply_sqlite_pragmas(eng)

        db.create_all()
        db.create_all(bind_key="log")
        db.create_all(bind_key="acq")
        db.create_all(bind_key="manutenzioni")
        db.create_all(bind_key="rifiuti")
        _ensure_manutenzioni_schema()
        _ensure_builtin_permissions()

    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)

    return app
