# flask --app run.py run
from app_odp.app import create_app

flask_app = create_app()
flask_app.debug = True

if __name__ == "__main__":
    flask_app.run(host="127.0.0.1", debug=True)
