from decimal import Decimal
from types import SimpleNamespace

import pytest

from app_odp.routes_modules.ordini import _parse_tempo_avanzamento_override
from app_odp.services import erp_export_service


@pytest.mark.parametrize(
    ("minutes", "expected_hours"),
    [
        ("1", "0.02"),
        ("5", "0.08"),
        ("10", "0.17"),
        ("30", "0.50"),
        ("60", "1.00"),
    ],
)
def test_tempo_avanzamento_converts_integer_minutes_with_half_up_rounding(
    minutes,
    expected_hours,
):
    assert _parse_tempo_avanzamento_override(
        minutes,
        allowed=True,
    ) == (int(minutes), expected_hours)


@pytest.mark.parametrize("invalid_value", ["0", "-1", "1.5", "1,5", "abc"])
def test_tempo_avanzamento_rejects_non_positive_or_non_integer_values(
    invalid_value,
):
    with pytest.raises(ValueError, match="minuti interi maggiori di 0"):
        _parse_tempo_avanzamento_override(invalid_value, allowed=True)


def test_tempo_avanzamento_is_ignored_without_permission():
    assert _parse_tempo_avanzamento_override("10", allowed=False) == (None, None)


def test_phase_payload_audits_calculated_and_forced_time(monkeypatch):
    monkeypatch.setattr(erp_export_service, "_current_username", lambda: "operatore")
    ordine = SimpleNamespace(
        IdDocumento="DOC",
        IdRiga="1",
        RifRegistraz="2026.1.1.1,00",
        CodArt="ART",
        DesArt="Articolo",
        NumProgrRiga="12",
        NumFase='["1"]',
        FaseAttiva="1",
    )

    payload = erp_export_service._build_phase_payload(
        ordine=ordine,
        distinta_base="[]",
        fase_corrente="1",
        q_ok=Decimal("1"),
        q_nok=Decimal("0"),
        tempo_finale="1.25",
        lotti_input=[],
        lotto_prodotto=None,
        note="",
        now_iso="2026-07-30T10:00:00+02:00",
        tempo_avanzamento_minuti=10,
        tempo_avanzamento_ore="0.17",
    )

    assert payload["tempo_funzionamento"] == "1.25"
    assert payload["tempo_funzionamento_calcolato"] == "1.25"
    assert payload["tempo_avanzamento_forzato"] is True
