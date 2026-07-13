from datetime import datetime, time
from types import SimpleNamespace

from app_odp.services import storico_ordini_service as service


def test_parse_date_returns_day_boundaries_or_none():
    assert service._parse_date("2026-07-09") == datetime.combine(
        datetime(2026, 7, 9).date(),
        time.min,
    )
    assert service._parse_date("2026-07-09", end_of_day=True) == datetime.combine(
        datetime(2026, 7, 9).date(),
        time.max,
    )
    assert service._parse_date("") is None
    assert service._parse_date("not-date") is None


def test_parse_dt_and_format_dt_handle_valid_and_invalid_values():
    parsed = service._parse_dt("2026-07-09T14:30:10")

    assert parsed == datetime(2026, 7, 9, 14, 30, 10)
    assert service._parse_dt("bad") is None
    assert service._format_dt("2026-07-09T14:30:10") == "09/07/2026 14:30:10"
    assert service._format_dt("bad") == "bad"
    assert service._format_dt("") == "-"


def test_payload_returns_dict_raw_or_wrapped_value():
    assert service._payload(SimpleNamespace(PayloadJson='{"a": 1}')) == {"a": 1}
    assert service._payload(SimpleNamespace(PayloadJson="[1, 2]")) == {"value": [1, 2]}
    assert service._payload(SimpleNamespace(PayloadJson="{bad")) == {"_raw": "{bad"}
    assert service._payload(SimpleNamespace(PayloadJson="")) == {}


def test_order_key_and_row_event_at_normalize_fields():
    row = SimpleNamespace(
        IdDocumento=" DOC ",
        IdRiga=" 10 ",
        EventAt="",
        ClosedAt="2026-07-09T14:30:10",
    )

    assert service._order_key(row) == ("DOC", "10")
    assert service._row_event_at(row) == "2026-07-09T14:30:10"


def test_event_in_group_window_respects_start_and_five_minute_end_tolerance():
    group = SimpleNamespace(
        CreatedAt="2026-07-09T08:00:00",
        ClosedAt="2026-07-09T10:00:00",
        DissolvedAt="",
    )

    assert service._event_in_group_window("2026-07-09T09:00:00", group) is True
    assert service._event_in_group_window("2026-07-09T07:59:59", group) is False
    assert service._event_in_group_window("2026-07-09T10:05:00", group) is True
    assert service._event_in_group_window("2026-07-09T10:05:01", group) is False
    assert service._event_in_group_window("bad", group) is False
    assert service._event_in_group_window("2026-07-09T09:00:00", None) is False


def test_operation_ids_returns_non_blank_unique_operation_ids():
    rows = [
        SimpleNamespace(OperationGroupId=" op-1 "),
        SimpleNamespace(OperationGroupId="op-1"),
        SimpleNamespace(OperationGroupId=""),
        SimpleNamespace(OperationGroupId="op-2"),
    ]

    assert set(service._operation_ids(rows)) == {"op-1", "op-2"}


def test_event_description_formats_sections_and_omits_empty_ones():
    row = SimpleNamespace(
        Azione="sospensione",
        UtenteOperazione="mario",
        StatoOrdinePre="Attivo",
        StatoOdpPre="",
        StatoOrdinePost="Sospeso",
        StatoOdpPost="",
        QuantitaConforme="3",
        QuantitaNonConforme="",
        Note="Cambio utensile",
        Motivo="",
    )

    assert service._event_description(row, {}) == (
        "mario - Sospensione | stato Attivo -> Sospeso | "
        "OK 3 / KO 0 | Cambio utensile"
    )

    minimal = SimpleNamespace(
        Azione="riattivazione",
        UtenteOperazione="",
        StatoOrdinePre="",
        StatoOdpPre="",
        StatoOrdinePost="",
        StatoOdpPost="",
        QuantitaConforme="",
        QuantitaNonConforme="",
        Note="",
        Motivo="",
    )

    assert service._event_description(minimal, {"utente": "luigi"}) == (
        "luigi - Riattivazione"
    )


def test_row_matches_python_filters_combines_resource_and_exact_group_type():
    entry = {
        "risorse": ["R-01", "Pressa Nord"],
        "group_type": "multiplo",
    }

    assert service._row_matches_python_filters(
        entry,
        {"risorsa": "PRESSA", "tipo_gruppo": "MULTIPLO"},
    ) is True
    assert service._row_matches_python_filters(
        entry,
        {"risorsa": "taglio", "tipo_gruppo": "MULTIPLO"},
    ) is False
    assert service._row_matches_python_filters(
        entry,
        {"risorsa": "pressa", "tipo_gruppo": "MISTO"},
    ) is False
