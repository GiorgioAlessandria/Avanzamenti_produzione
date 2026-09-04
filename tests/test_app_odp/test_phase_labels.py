"""Regressione: preferenze personali, sessioni operatore e codici immutati."""
import re
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

from flask import Flask, render_template, url_for
from flask_login import LoginManager
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.datastructures import MultiDict

from app_odp.models import Permissions, Roles, User, db
from app_odp.operator_session import active_policy, active_token, active_user, create_operator_session
from app_odp.phase_labels import get_phase_labels, phase_label, validate_phase_labels
from app_odp.policy.policy import RbacPolicy
from app_odp.routes_modules.preferenze import main_bp


class PhaseLabelsTest(unittest.TestCase):
    def test_format_and_validation(self):
        labels = validate_phase_labels(["01", "2", "3", ""], [" Montaggio ", "Collaudo", "", ""])
        self.assertEqual(labels, {"1": "Montaggio", "2": "Collaudo"})
        for value in (1, "01", "1.0", "Fase 1"):
            self.assertEqual(phase_label(value, labels), "Montaggio")
        self.assertEqual(phase_label("1 + 2", labels), "Montaggio + Collaudo")
        self.assertEqual(phase_label("1,2", labels), "Montaggio,Collaudo")
        self.assertEqual(phase_label(3, labels), "3")
        self.assertEqual(phase_label(None, labels), "")
        self.assertEqual(phase_label("-", labels), "-")
        self.assertEqual(phase_label("Fase finale", labels), "Fase finale")
        self.assertEqual(phase_label(1, {}), "1")
        self.assertEqual(phase_label(2, {}), "2")
        self.assertEqual(phase_label("1 + 2", {}), "1 + 2")
        self.assertEqual(get_phase_labels(SimpleNamespace(is_authenticated=False)), {})
        self.assertEqual(get_phase_labels(SimpleNamespace(is_authenticated=True, preferences=[])), {})
        self.assertEqual(get_phase_labels(SimpleNamespace(is_authenticated=True, preferences={"phase_labels": []})), {})
        for codes, names in [(["1", "01"], ["A", "B"]), (["-1"], ["A"]),
                             (["1.2"], ["A"]), ([""], ["A"]), (["1"], ["x" * 81]),
                             (["1"], ["A\nB"]), (["1"], []), (["1"] * 101, [""] * 101)]:
            with self.assertRaises(ValueError):
                validate_phase_labels(codes, names)

    def test_personal_preferences_and_operator_sessions(self):
        app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[2] / "app_odp/templates"))
        app.config.update(TESTING=True, SECRET_KEY="phase-labels-test",
                          SQLALCHEMY_DATABASE_URI="sqlite://", SQLALCHEMY_TRACK_MODIFICATIONS=False)
        db.init_app(app)
        login = LoginManager(app)
        login.user_loader(lambda user_id: db.session.get(User, int(user_id)))
        app.add_url_rule("/login", "auth.login", lambda: "Login")
        app.register_blueprint(main_bp)

        # Gli altri endpoint non fanno parte di questa app di test; base.j2 rimane reale.
        def template_url(endpoint, **kwargs):
            if endpoint not in app.view_functions:
                return "/test-placeholder"
            if endpoint == "main.preferenze_fasi" and active_token():
                kwargs.setdefault("tab_session", active_token())
            return url_for(endpoint, **kwargs)

        app.jinja_env.globals["url_for"] = template_url

        @app.context_processor
        def template_context():
            return dict(operator_user=active_user() if active_token() else None,
                        operator_policy=active_policy(), policy=active_policy(), tab_session=active_token(),
                        operator_url_for=template_url)

        with app.app_context():
            db.create_all(bind_key=None)
            alice, bob = User(username="Alice"), User(username="Bob")
            role = Roles(name="personalizza_fasi")
            role.permissions.append(Permissions(Codice="nomi_fase"))
            alice.roles.append(role)
            bob.roles.append(role)
            alice.preferences = {"unrelated": "keep", "phase_labels": {"1": "Montaggio"}}
            bob.preferences = {"phase_labels": {"1": "Assemblaggio"}}
            db.session.add_all([alice, bob])
            db.session.commit()
            alice_id, bob_id = alice.id, bob.id
            with app.test_request_context():
                bob_token = create_operator_session(bob)

        client = app.test_client()
        self.assertEqual(client.get("/preferenze/fasi").status_code, 302)
        with client.session_transaction() as sess:
            sess["_user_id"] = str(alice_id)
            sess["_fresh"] = True

        def form(url="/preferenze/fasi", code="1", name="Collaudo"):
            response = client.get(url)
            self.assertEqual(response.status_code, 200)
            html = response.get_data(as_text=True)
            self.assertEqual(response.headers["Cache-Control"], "no-store")
            self.assertRegex(html, r'<a href="[^"]*/preferenze/fasi[^"]*" class="text-dark fs-4">')
            return {"csrf_token": re.search(r'name="csrf_token" value="([^"]+)"', html)[1],
                    "preference_user_id": re.search(r'name="preference_user_id" value="([^"]+)"', html)[1],
                    "phase_code": code, "phase_name": name}

        payload = form(name='<script>alert("x")</script>')
        response = client.post("/preferenze/fasi", data=payload)
        self.assertEqual(response.status_code, 303)
        html = client.get(response.location).get_data(as_text=True)
        self.assertIn("&lt;script&gt;", html)
        self.assertNotIn('<script>alert("x")</script>', html)
        with app.app_context():
            self.assertEqual(db.session.get(User, alice_id).get_pref("unrelated"), "keep")
            self.assertEqual(get_phase_labels(db.session.get(User, bob_id)), {"1": "Assemblaggio"})

        operator_url = f"/preferenze/fasi?tab_session={bob_token}"
        payload_bob = form(operator_url, name="Controllo qualità")
        self.assertEqual(payload_bob["preference_user_id"], str(bob_id))
        self.assertEqual(client.post(operator_url, data=payload).status_code, 400)
        response = client.post(operator_url, data=payload_bob)
        self.assertEqual(response.status_code, 303)
        self.assertIn(bob_token, response.location)
        with app.app_context():
            self.assertEqual(get_phase_labels(db.session.get(User, bob_id)), {"1": "Controllo qualità"})
            self.assertIn("<script>", get_phase_labels(db.session.get(User, alice_id))["1"])

        self.assertEqual(client.post(operator_url, data={**payload_bob, "csrf_token": "errato"}).status_code, 400)
        self.assertEqual(client.post(operator_url, data={**payload_bob, "csrf_token": "è"}).status_code, 400)
        self.assertEqual(client.get("/preferenze/fasi?tab_session=scaduto").status_code, 403)
        self.assertEqual(client.post("/preferenze/fasi?tab_session=scaduto", data=payload).status_code, 403)
        duplicate = MultiDict([*payload_bob.items(), ("phase_code", "01"), ("phase_name", "Duplicata")])
        self.assertEqual(client.post(operator_url, data=duplicate).status_code, 400)
        with self.assertLogs(app.logger, level="ERROR"), patch.object(db.session, "commit", side_effect=SQLAlchemyError("test")):
            self.assertEqual(client.post(operator_url, data={**payload_bob, "phase_name": "Non salvare"}).status_code, 500)
        with app.app_context():
            self.assertEqual(get_phase_labels(db.session.get(User, bob_id)), {"1": "Controllo qualità"})

        self.assertEqual(client.post(operator_url, data={**payload_bob, "phase_name": ""}).status_code, 303)
        self.assertIn('placeholder="1"', client.get(operator_url).get_data(as_text=True))
        with app.app_context():
            self.assertEqual(get_phase_labels(db.session.get(User, bob_id)), {})
            # Il permesso del login condiviso non autorizza un operatore diverso.
            bob = db.session.get(User, bob_id)
            bob.roles.clear()
            db.session.commit()
        self.assertEqual(client.get(operator_url).status_code, 403)
        self.assertEqual(client.post(operator_url, data=payload_bob).status_code, 403)
        self.assertEqual(client.get("/preferenze/fasi").status_code, 200)
        with app.app_context():
            alice = db.session.get(User, alice_id)
            alice.roles.clear()
            db.session.commit()
        self.assertEqual(client.get("/preferenze/fasi").status_code, 403)
        self.assertEqual(client.post("/preferenze/fasi", data=payload).status_code, 403)
        with app.test_request_context():
            from flask import g
            g.operator_user = db.session.get(User, alice_id)
            g.operator_token = "test"
            g.operator_policy = RbacPolicy(g.operator_user)
            nav = render_template("partials/_nav_offcanvas.j2")
            self.assertNotIn("Nomi delle fasi", nav)
            self.assertIn("<script>", get_phase_labels(g.operator_user)["1"])
        with app.app_context():
            db.session.remove()
            db.drop_all(bind_key=None)


if __name__ == "__main__":
    unittest.main()
