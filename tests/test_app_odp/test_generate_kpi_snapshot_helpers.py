from datetime import date
from types import SimpleNamespace

from app_odp.tasks import generate_kpi_snapshot as task


def test_snapshot_helpers_use_final_orders_and_json_due_dates():
    runtime = SimpleNamespace(StatoOrdinePost="Chiusa", StatoOdpPost="")
    log = SimpleNamespace(
        StatoOrdinePost="Chiusa",
        GestioneMatricola="si",
        DataFineSched='["2026-06-24 00:00:00"]',
        NumFase="[1.0, 2.0]",
        FaseConsuntivata="2",
        FaseAttiva="",
    )

    assert task._is_order_completed(runtime, log) is True
    assert task._macchine_prodotte_for_log(runtime, log) == 1.0
    assert task._data_fine_prevista(log) == date(2026, 6, 24)


def test_snapshot_bucket_counts_all_phases_but_only_completed_orders():
    bucket = task._new_bucket("global", "*")
    base = {
        "macchine_prodotte": 0.0,
        "ritardo_giorni": 3,
        "tempo_previsto_ore": 2.0,
        "tempo_reale_ore": 2.5,
    }

    task._apply_to_bucket(bucket, {**base, "is_order_completed": False})
    task._apply_to_bucket(
        bucket,
        {**base, "is_order_completed": True, "macchine_prodotte": 1.0},
    )
    result = task._finalize_bucket(
        bucket,
        snapshot_month="2026-06",
        period_start=date(2026, 6, 1),
        period_end=date(2026, 6, 30),
    )

    assert result["ordini_chiusi"] == 1
    assert result["ordini_in_ritardo"] == 1
    assert result["macchine_prodotte"] == 1.0
    assert result["tempo_medio_ordine"] is None
    assert result["tempo_medio_fase"] == 2.5
