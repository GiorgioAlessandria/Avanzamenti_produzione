# LIBRERIE ESTERNE
from flask import render_template, request, redirect, url_for, flash, Blueprint
from flask_login import login_required
from models import User, ordini_produzione
try:
    from icecream import ic
except:
    pass

main_bp = Blueprint("main", __name__)


@main_bp.route('/')
@login_required
def home():
    odp = ordini_produzione.query.all()

    return render_template("home.j2", ordini_produzione=odp)
