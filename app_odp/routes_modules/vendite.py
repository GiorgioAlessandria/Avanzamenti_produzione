from flask import jsonify, render_template

from app_odp.policy.decorator import require_active_perm
from app_odp.routes_blueprint import main_bp
from app_odp.services.vendite_service import build_vendite_payload


@main_bp.get("/vendite")
@require_active_perm("vendite")
def vendite_page():
    return render_template("vendite.j2")


@main_bp.get("/api/vendite/ordini-macchina")
@require_active_perm("vendite")
def api_vendite_ordini_macchina():
    response = jsonify({"ok": True, "data": build_vendite_payload()})
    response.headers["Cache-Control"] = "no-store"
    return response, 200
