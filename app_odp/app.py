from flask import Flask, redirect, url_for
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_migrate import Migrate
from app_odp.models import db, User
from app_odp.auth import auth_bp
from app_odp.routes import main_bp
import tomllib
from pathlib import Path
try:
    from icecream import ic
finally:
    pass
CONFIG_PATH = Path("app_odp/static/config.toml")


def load_config(config: Path) -> dict:
    """
    Caricamento e lettura file configurazioni

    :return: Ritorna un dizionario con le configurazioni
    :rtype: dict[Any, Any]
    """
    with config.open("rb") as f:
        return tomllib.load(f)


configurazione = load_config(CONFIG_PATH)


def create_app():
    app = Flask(__name__, instance_relative_config=True,
                static_folder='static', template_folder='templates')
    app.debug = True

    # chiave segreta per sessioni e Flask-Login
    app.config["SECRET_KEY"] = "Berserk"

    # DB SQLite dentro instance
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{configurazione['Percorsi']['percorso_db']}"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # inizializza estensioni
    db.init_app(app)
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = "auth.login"

    @login_manager.user_loader
    def load_user(user_id: str):
        return User.query.get(int(user_id))

    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)

    return app
