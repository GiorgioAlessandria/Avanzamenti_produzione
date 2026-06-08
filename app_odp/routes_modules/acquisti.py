# app_odp/routes_modules/acquisti.py

from io import BytesIO

from flask import abort, jsonify, render_template, request, send_file

from app_odp.routes import main_bp, _norm_text, _parse_bool_flag, _now_rome_dt

from app_odp.services.acquisti_service import (
    _build_acquisti_giacenze_rows,
    _build_acquisti_materiale_rows,
    _build_acquisti_ordini_rows,
    _filter_acquisti_giacenze_rows,
    _filter_acquisti_materiale_rows,
    _build_acquisti_excel_workbook,
)

from app_odp.policy.decorator import require_active_perm


@main_bp.get("/acquisti")
@require_active_perm("home_acquisti")
def home_acquisti():
    giacenze_rows = _build_acquisti_giacenze_rows()
    materiali_rows = _build_acquisti_materiale_rows()
    ordini_rows = _build_acquisti_ordini_rows()

    return render_template(
        "home_acquisti.j2",
        giacenze_rows=giacenze_rows,
        materiali_rows=materiali_rows,
        ordini_rows=ordini_rows,
    )


@main_bp.get("/api/acquisti/export/<section>")
@require_active_perm("home_acquisti")
def api_export_acquisti_excel(section):
    section = _norm_text(section).lower()

    if section == "giacenza":
        rows = _build_acquisti_giacenze_rows()
        rows = _filter_acquisti_giacenze_rows(
            rows,
            codart=request.args.get("codart", ""),
            variante=request.args.get("variante", ""),
            desart=request.args.get("desart", ""),
            only_negative=_parse_bool_flag(request.args.get("negative")),
            only_understock=_parse_bool_flag(request.args.get("understock")),
        )
        file_name = f"acquisti_giacenza_{_now_rome_dt().strftime('%Y%m%d_%H%M%S')}.xlsx"

    elif section == "materiale":
        rows = _build_acquisti_materiale_rows()
        rows = _filter_acquisti_materiale_rows(
            rows,
            codart=request.args.get("codart", ""),
            variante=request.args.get("variante", ""),
            desart=request.args.get("desart", ""),
            only_critical=_parse_bool_flag(request.args.get("critical")),
            only_understock=_parse_bool_flag(request.args.get("understock")),
        )
        file_name = (
            f"acquisti_materiale_{_now_rome_dt().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )

    else:
        abort(404)

    wb = _build_acquisti_excel_workbook(section, rows)

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=file_name,
    )


@main_bp.get("/api/acquisti/bridge")
@require_active_perm("home_acquisti")
def api_acquisti_bridge():
    giacenze_rows = _build_acquisti_giacenze_rows()
    materiali_rows = _build_acquisti_materiale_rows()
    ordini_rows = _build_acquisti_ordini_rows()

    fragments = {
        "tbody_acquisti_giacenza": render_template(
            "partials/_acquisti_giacenza_rows.j2",
            giacenze_rows=giacenze_rows,
        ),
        "tbody_acquisti_materiale": render_template(
            "partials/_acquisti_materiale_rows.j2",
            materiali_rows=materiali_rows,
        ),
        "acquisti_ordini_section": render_template(
            "partials/_acquisti_ordini_produzione.j2",
            ordini_rows=ordini_rows,
        ),
    }

    return jsonify(
        {
            "ok": True,
            "refreshed_at": _now_rome_dt().isoformat(timespec="seconds"),
            "fragments": fragments,
        }
    )
