# LIBRERIE ESTERNE
from flask_login import current_user
from flask import render_template, request, redirect, url_for, flash, Blueprint
from flask_login import login_required, current_user
from models import User, input_odp
try:
    from icecream import ic
except:
    pass

main_bp = Blueprint("main", __name__)


def filtri_rbac(odp: list, user) -> list:

    return odp


@main_bp.route('/')
@login_required
def home():
    """
    Docstring per home
    """
    odp = input_odp.query.all()
    odp_filtrati = filtri_rbac(odp, current_user)
    return render_template("home.j2", ordini_produzione=odp)


# routes.py


@main_bp.route("/api/ordini")
@login_required
def api_ordini():
    """
    Docstring per api_ordini
    """
    q = input_odp.query
    allowed = [r.codice for r in current_user.reparti]  # o per ruolo/permesso
    if allowed:
        q = q.filter(input_odp.CodReparto.in_(allowed))
    # meglio serializzare solo i campi necessari
    return {"rows": [r.__dict__ for r in q.all()]}
