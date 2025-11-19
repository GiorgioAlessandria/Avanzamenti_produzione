from flask import Flask, render_template, redirect, url_for, request, flash
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user

import os


def create_app() -> Flask:
    app = Flask(__name__, template_folder='template')
    app.config['SECRET_KEY'] = os.environ.get(
        'SECRET_KEY', 'dev-secret-change-me')
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///\\\\Serverspring02\\PythonDB\\Avanzamenti_produzione\\instance\\psw_db.db'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)

    with app.app_context():
        db.create_all()
    from flaskr.routes import register_routes
    register_routes(app, db)
    migrate = Migrate(app, db)
    return app

# if __name__ == '__main__':
#     app.run(debug=True)
