from flask import Flask, redirect, url_for
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_migrate import Migrate
from models import db, User
from auth import auth_bp
from routes import main_bp
try:
    from icecream import ic
except:
    pass


def create_app():
    app = Flask(__name__, instance_relative_config=True,
                static_folder='static', template_folder='templates')

    # chiave segreta per sessioni e Flask-Login
    app.config["SECRET_KEY"] = "cambia-questa-chiave-super-segreta"

    # DB SQLite dentro instance
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///\\\\Serverspring02\\PythonDB\\Avanzamenti_produzione\\instance\\RBAC.db"
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
