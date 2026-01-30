# region IMPORT
# LIBRERIE ESTERNE
from flask_login import current_user
from flask import render_template, request, redirect, url_for, flash, Blueprint
from flask_login import login_required, current_user
import pandas as pd
from sqlalchemy import select
# LIBRERIE INTERNE
from app_odp.models import User, InputOdp, db
try:
    from icecream import ic
except:
    pass
main_bp = Blueprint("main", __name__)

# region FUNZIONI


def odp_filtri_rbac(odp: pd.DataFrame, user):
    """
    Filtri RBAC sugli ordini di produzione
    Elenco filtri:
    - Lavorazioni
    - Risorse
    - Reparti
    - Famiglia
    - Macrofamiglia
    - Magazzini
    """
    # if user.is_admin:
    #     return odp
    # else:
    reparti_utente = [r.codice for r in user.reparti]

    ic(odp)
    odp.to_excel("temp/odp.xlsx", index=False)
    odp = odp[odp["CodReparto"].isin(reparti_utente)]

    # return odp_filtrati


# region PERCORSI
@main_bp.route('/')
@login_required
def home():
    """
    Home page
    """
    stmt = select(InputOdp)
    engine = db.session.get_bind()
    with engine.connect() as conn:
        df_odp = pd.read_sql(stmt, conn)
    odp_filtrati = odp_filtri_rbac(df_odp, current_user)
    return render_template("home.j2", ordini_produzione=odp_filtrati)


# routes.py


# @main_bp.route("/api/ordini")
# @login_required
# def api_ordini():
#     """
#
#     """
#     q = input_odp.query
#     allowed = [r.codice for r in current_user.reparti]  # o per ruolo/permesso
#     if allowed:
#         q = q.filter(input_odp.CodReparto.in_(allowed))
#     # meglio serializzare solo i campi necessari
#     return {"rows": [r.__dict__ for r in q.all()]}
