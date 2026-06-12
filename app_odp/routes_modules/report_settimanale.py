from __future__ import annotations

from datetime import timedelta
from zoneinfo import ZoneInfo

from flask import abort, jsonify, render_template, request
from app_odp.operator_session import (
    active_policy,
    active_user,
    active_token,
    operator_or_login_required,
)
from app_odp.routes import main_bp
from app_odp.services.report_settimanale_service import (
    build_report_settimanale_for_user,
    get_report_users,
)

ROME_TZ = ZoneInfo("Europe/Rome")


def _now_rome():
    from datetime import datetime

    return datetime.now(ROME_TZ)


def _last_7_days_range():
    end_dt = _now_rome()
    start_dt = end_dt - timedelta(days=7)
    return start_dt, end_dt


def _report_permissions():
    policy = active_policy()

    can_self = policy.can("report_settimanale_view")
    can_tot = policy.can("report_settimanale_view_tot")

    if not can_self and not can_tot:
        abort(403)

    return can_self, can_tot


@main_bp.get("/report-settimanale")
@operator_or_login_required
def report_settimanale_page():
    _can_self, can_tot = _report_permissions()

    user = active_user()

    if user is None or not getattr(user, "is_authenticated", False):
        abort(403)

    start_dt, end_dt = _last_7_days_range()

    tab_session = active_token() or request.args.get("tab_session")

    return render_template(
        "report_settimanale.j2",
        can_select_user=can_tot,
        default_user_id=user.id,
        period_start=start_dt.strftime("%d/%m/%Y %H:%M"),
        period_end=end_dt.strftime("%d/%m/%Y %H:%M"),
        tab_session=tab_session,
    )


@main_bp.get("/api/report-settimanale/bridge")
@operator_or_login_required
def api_report_settimanale_bridge():
    _can_self, can_tot = _report_permissions()

    user = active_user()

    if user is None or not getattr(user, "is_authenticated", False):
        abort(403)

    requested_user_id = request.args.get("user_id", type=int)

    if can_tot:
        selected_user_id = requested_user_id or user.id
    else:
        selected_user_id = user.id

    start_dt, end_dt = _last_7_days_range()

    users = get_report_users() if can_tot else []

    result = build_report_settimanale_for_user(
        selected_user_id=selected_user_id,
        start_dt=start_dt,
        end_dt=end_dt,
        can_select_user=can_tot,
        users=users,
    )

    status_code = 200 if result.get("ok") else 400

    return jsonify(result), status_code
