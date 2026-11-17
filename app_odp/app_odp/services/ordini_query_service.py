from sqlalchemy.orm import selectinload

from app_odp.models import InputOdp


def _base_odp_query():
    return InputOdp.query.options(
        selectinload(InputOdp.runtime_row),
    )
