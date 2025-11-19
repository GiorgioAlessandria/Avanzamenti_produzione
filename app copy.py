from flask_sqlalchemy import SQLAlchemy
from flask import Flask, request, render_template
from flask_migrate import Migrate
import pandas as pd
try:
    from icecream import ic
except:
    pass
db = SQLAlchemy()


def create_app() -> Flask:
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True,
                template_folder='templates')
    app.config.from_mapping(
        SECRET_KEY='dev',
        SQLALCHEMY_DATABASE_URI="sqlite:///DBPATH.db",
        SQLALCHEMY_TRACK_MODIFICATIONS=False
    )
    db.init_app(app)

    with app.app_context():
        db.create_all()

    from flaskr.routes import register_routes
    ic()
    register_routes(app, db)

    migrate = Migrate(app, db)
    return app
