# region IMPORT
# LIBRERIE ESTERNE
from flask import render_template, Blueprint
from flask_login import login_required, current_user
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import selectinload

# LIBRERIE INTERNE
from app_odp.models import InputOdp, db, Roles
from app_odp.RBAC.decorator import require_perm
from app_odp.RBAC.policy import RbacPolicy

try:
    from icecream import ic
finally:
    pass
main_bp = Blueprint("main", __name__)

# region FUNZIONI


# region PERCORSI
@main_bp.route("/")
@login_required
@require_perm("home")
def home():
    """
    Home page
    """
    policy = RbacPolicy(current_user)
    q = InputOdp.query
    q = policy.filter_input_odp(q)
    odp = q.all()
    return render_template("home.j2", odp=odp, policy=policy)
