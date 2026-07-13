from datetime import date
from types import SimpleNamespace

from app_odp.services.dashboard_service import _dashboard_data_fine_prevista


def test_dashboard_data_fine_prevista_reads_active_phase_from_json_list():
    ordine = SimpleNamespace(
        FaseAttiva="2",
        DataFinePrevista='["2026-07-10", "2026-07-13 08:30:00"]',
        DataFineSched="2026-07-20",
    )

    assert _dashboard_data_fine_prevista(ordine) == date(2026, 7, 13)


def test_dashboard_data_fine_prevista_falls_back_to_sched_and_ignores_invalid_dates():
    assert _dashboard_data_fine_prevista(
        SimpleNamespace(FaseAttiva="1", DataFinePrevista="", DataFineSched="09/07/2026")
    ) == date(2026, 7, 9)

    assert _dashboard_data_fine_prevista(
        SimpleNamespace(FaseAttiva="1", DataFinePrevista="not-a-date", DataFineSched="")
    ) is None

