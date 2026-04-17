from app_odp.app import create_app
from app_odp.models import db


app = create_app()

with app.app_context():
    db.create_all()
    db.create_all(bind_key = "log")
    db.create_all(bind_key = "acq")

print("DB creati")
