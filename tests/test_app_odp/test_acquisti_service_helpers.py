from datetime import datetime
from types import SimpleNamespace

import pytest

from app_odp.services import acquisti_service as service


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2,50", "2.5"),
        ("3.00", ""),
        ("bad", ""),
    ],
)
def test_decimal_fraction_text_returns_only_fractional_quantities(value, expected):
    assert service._decimal_fraction_text(value) == expected


def test_new_acq_material_row_uses_minimal_defaults():
    row = service._new_acq_material_row(" ART-1 ", " A ")

    assert row["CodArt"] == "ART-1"
    assert row["VarianteArt"] == "A"
    assert row["QtyMag0"] is None
    assert row["MaterialeDaConsumare"] == 0.0
    assert row["Mag0Missing"] is True
    assert row["DistintaDettagli"] == []
    assert row["OrdineDettagli"] == []


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", (0, "")),
        ("A", (1, "A")),
        ("c", (3, "C")),
        ("AA", (0, "AA")),
    ],
)
def test_acq_revision_rank_orders_single_letters(value, expected):
    assert service._acq_revision_rank(value) == expected


def test_contains_insensitive_matches_case_insensitive_and_empty_needle():
    assert service._contains_insensitive("Banco Montaggio", "mont") is True
    assert service._contains_insensitive("Banco Montaggio", "taglio") is False
    assert service._contains_insensitive("Banco Montaggio", "") is True


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (" 6.0 ", "6"),
        ("6,0", "6"),
        ("6,5", "6,5"),
        (" MAG ", "MAG"),
    ],
)
def test_normalize_acq_mag_code_strips_integral_decimal_codes(value, expected):
    assert service._normalize_acq_mag_code(value) == expected


def test_material_key_normalizes_article_and_variant():
    assert service._material_key(" ART-1 ", " X ") == ("ART-1", "")
    assert service._material_key(" ART-1 ", " B ") == ("ART-1", "B")


def test_first_not_blank_text_returns_first_normalized_value():
    assert service._first_not_blank_text(None, " ", "  abc  ", "def") == "abc"
    assert service._first_not_blank_text(None, " ") == ""


def test_extract_comp_udm_prefers_component_values_then_article():
    articolo = SimpleNamespace(MagUM="KG")

    assert service._extract_comp_udm({"TecniciUm": " PZ ", "MagUM": "KG"}, articolo) == "PZ"
    assert service._extract_comp_udm({"TecniciUm": "", "MagUM": ""}, articolo) == "KG"


@pytest.mark.parametrize("value", [None, "nan", "None", "NULL", "X", "-"])
def test_norm_variante_art_clears_empty_sentinel_values(value):
    assert service._norm_variante_art(value) == ""


@pytest.mark.parametrize("value", ["", None, "X", "-"])
def test_normalize_variante_art_clears_empty_sentinel_values(value):
    assert service._normalize_variante_art(value) == ""


def test_ordine_stato_effettivo_prefers_runtime_state():
    ordine = SimpleNamespace(
        StatoOrdine="Pianificata",
        runtime_row=SimpleNamespace(Stato_odp="Attiva"),
    )

    assert service._ordine_stato_effettivo(ordine) == "Attiva"


@pytest.mark.parametrize(
    ("stato", "expected"),
    [
        ("Chiusa", False),
        ("Attiva", True),
        ("Pianificata", True),
        ("Sospesa", True),
        ("Aperta", True),
        ("Annullata", False),
    ],
)
def test_is_open_order_state_matches_open_status_words(stato, expected):
    assert service._is_open_order_state(stato) is expected


def test_format_datetime_it_formats_iso_datetime_or_returns_raw_value():
    assert service._format_datetime_it("2026-07-09T14:30:00") == "09/07/2026 14:30"
    assert service._format_datetime_it("non-data") == "non-data"
    assert service._format_datetime_it("") == ""


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ""),
        ("2.50", "2.5"),
        (3, "3"),
    ],
)
def test_num_text_formats_decimal_values(value, expected):
    assert service._num_text(value) == expected


def test_parse_iso_datetime_parses_valid_values_and_rejects_invalid():
    parsed = service._parse_iso_datetime("2026-07-09T14:30:00")

    assert isinstance(parsed, datetime)
    assert parsed.year == 2026
    assert parsed.month == 7
    assert service._parse_iso_datetime("non-data") is None
    assert service._parse_iso_datetime("") is None


def test_parse_scorta_qrcode_accepts_three_parts_and_rejects_invalid_values():
    assert service._parse_scorta_qrcode(" ART-1 | VAR | REV \n") == (
        "ART-1",
        "VAR",
        "REV",
    )
    assert service._parse_scorta_qrcode("ART-1||") == ("ART-1", "", "")

    with pytest.raises(ValueError, match="Formato QR non valido"):
        service._parse_scorta_qrcode("ART-1|VAR")
    with pytest.raises(ValueError, match="Codice articolo mancante"):
        service._parse_scorta_qrcode("|VAR|REV")


def test_scorta_operator_payload_uses_username_and_coded_department():
    user = SimpleNamespace(username="mario", RepartoPrinc=" REP-01 ")

    assert service._scorta_operator_payload(user) == ("mario", "REP-01")


def test_scorta_to_row_serializes_manual_report_for_ui(monkeypatch):
    row = SimpleNamespace(
        id=10,
        DataLettura="2026-07-09T14:30:00",
        RawQrCode="Materiale manuale",
        CodArt="MANUALE-interno",
        VarianteArt="",
        IndiceModifica="",
        DesArt="Materiale manuale",
        PuntoRiordino="2.50",
        LottoRiordino=3,
        PianTempoApprovFisso=4,
        Stato="Aperta",
        Annullata=0,
        Note=None,
        SegnalatoDa="mario",
        RepartoSegnalatore="REP-01",
        LookupTrovato=1,
        StatoChangedAt="2026-07-10T08:15:00",
    )
    monkeypatch.setattr(
        service,
        "_is_scorta_segnalazione_libera",
        lambda _row: True,
    )
    monkeypatch.setattr(
        service,
        "_is_scorta_aperta_oltre_3_giorni",
        lambda _row: True,
    )

    assert service._scorta_to_row(row) == {
        "Id": 10,
        "DataLettura": "2026-07-09T14:30:00",
        "DataLetturaText": "09/07/2026 14:30",
        "RawQrCode": "Materiale manuale",
        "CodArt": "",
        "VarianteArt": "",
        "IndiceModifica": "",
        "DesArt": "Materiale manuale",
        "PuntoRiordino": "2.50",
        "PuntoRiordinoText": "2.5",
        "LottoRiordino": 3,
        "LottoRiordinoText": "3",
        "PianTempoApprovFisso": 4,
        "Stato": "Aperta",
        "Annullata": False,
        "Note": "",
        "SegnalatoDa": "mario",
        "RepartoSegnalatore": "REP-01",
        "LookupTrovato": True,
        "ScortaApertaOltre3Giorni": True,
        "StatoChangedAt": "2026-07-10T08:15:00",
        "StatoChangedAtText": "10/07/2026 08:15",
    }


def test_filter_acquisti_scorte_rows_handles_status_text_and_cancelled_rows():
    rows = [
        {
            "Id": "open",
            "CodArt": "ABC-1",
            "VarianteArt": "V1",
            "DesArt": "Pompa principale",
            "SegnalatoDa": "Mario Rossi",
            "Stato": "Aperta",
            "Annullata": False,
        },
        {
            "Id": "closed",
            "CodArt": "DEF-2",
            "VarianteArt": "",
            "DesArt": "Valvola",
            "SegnalatoDa": "Luigi",
            "Stato": "Chiusa",
            "Annullata": False,
        },
        {
            "Id": "cancelled",
            "CodArt": "ABC-1",
            "VarianteArt": "V1",
            "DesArt": "Pompa principale",
            "SegnalatoDa": "Mario Rossi",
            "Stato": "Aperta",
            "Annullata": True,
        },
    ]

    def ids(**filters):
        filtered = service._filter_acquisti_scorte_rows(rows, **filters)
        return [row["Id"] for row in filtered]

    assert ids() == ["open", "closed"]
    assert ids(stato="annullata") == ["cancelled"]
    assert ids(
        codart="abc",
        variante="v1",
        desart="pompa",
        segnalato_da="rossi",
        stato="aperta",
    ) == ["open"]
    assert ids(include_annullate=True) == ["open", "closed", "cancelled"]


def test_find_scorta_lookup_handles_exact_compatible_missing_and_ambiguous(
    monkeypatch,
):
    rows = []

    def filter_by(**values):
        assert values == {"CodArt": "ART-1"}
        return SimpleNamespace(all=lambda: list(rows))

    monkeypatch.setattr(
        service,
        "AcqArticoliLookup",
        SimpleNamespace(query=SimpleNamespace(filter_by=filter_by)),
    )
    exact = SimpleNamespace(VarianteArt="V1", IndiceModifica="R1")
    other = SimpleNamespace(VarianteArt="V2", IndiceModifica="R2")
    rows[:] = [exact, other]

    assert service._find_scorta_lookup("ART-1", "V1", "R1") is exact
    assert service._find_scorta_lookup("ART-1", "V1", "") is exact
    assert service._find_scorta_lookup("ART-1", "V3", "") is None
    with pytest.raises(ValueError, match="Articolo ambiguo"):
        service._find_scorta_lookup("ART-1", "", "")

    rows.clear()
    assert service._find_scorta_lookup("ART-1", "", "") is None
