from app_odp.models import (
    ProductionCapacityCalendar,
    User,
)

WEEKDAY_LABELS = {
    0: "Lunedì",
    1: "Martedì",
    2: "Mercoledì",
    3: "Giovedì",
    4: "Venerdì",
    5: "Sabato",
    6: "Domenica",
}


def _norm_text(value) -> str:
    return str(value or "").strip()


def _capacity_float(value, default: float = 0.0) -> float:
    raw = _norm_text(value).replace(",", ".")
    if not raw:
        return default

    try:
        out = float(raw)
    except (TypeError, ValueError):
        return default

    return max(out, 0.0)


def _capacity_scope_code_is_valid(scope_type: str, scope_code: str) -> bool:
    scope_type = _norm_text(scope_type)
    scope_code = _norm_text(scope_code)

    if scope_type != "operatore":
        return False

    try:
        user_id = int(scope_code)
    except (TypeError, ValueError):
        return False

    return (
        User.query.filter(
            User.id == user_id,
            User.active.is_(True),
        ).first()
        is not None
    )


def _capacity_row_to_dict(row: ProductionCapacityCalendar) -> dict:
    return {
        "id": row.id,
        "scope_type": row.scope_type,
        "scope_code": row.scope_code,
        "weekday": int(row.weekday),
        "weekday_label": WEEKDAY_LABELS.get(int(row.weekday), str(row.weekday)),
        "hours_capacity": float(row.hours_capacity or 0.0),
        "active": bool(row.active),
        "updated_at": row.updated_at or "",
        "updated_by": row.updated_by or "",
    }


def _capacity_settings_payload() -> dict:
    rows = (
        ProductionCapacityCalendar.query.filter(
            ProductionCapacityCalendar.scope_type == "operatore"
        )
        .order_by(
            ProductionCapacityCalendar.scope_code.asc(),
            ProductionCapacityCalendar.weekday.asc(),
        )
        .all()
    )

    operatori = (
        User.query.filter(User.active.is_(True)).order_by(User.username.asc()).all()
    )

    return {
        "weekdays": [
            {"weekday": key, "label": label} for key, label in WEEKDAY_LABELS.items()
        ],
        "capacity_rows": [_capacity_row_to_dict(row) for row in rows],
        "operatori": [
            {
                "codice": str(int(u.id)),
                "descrizione": f"{u.username or ''} - {u.RepartoPrinc or 'senza reparto'}",
                "username": u.username or "",
                "reparto_princ": u.RepartoPrinc or "",
            }
            for u in operatori
        ],
    }
