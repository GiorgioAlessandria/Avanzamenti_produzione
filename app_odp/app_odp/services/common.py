from sqlalchemy import func

from app_odp.models import db, InputOdpLog, OdpRuntimeLog


def _last_log_token() -> str:
    runtime_max = db.session.query(func.max(OdpRuntimeLog.log_id)).scalar() or 0
    input_max = db.session.query(func.max(InputOdpLog.log_id)).scalar() or 0
    return f"{input_max}:{runtime_max}"
