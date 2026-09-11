from types import SimpleNamespace

from app_odp.services import acquisti_service as service


def _giacenza_row(udm: str) -> dict:
    return {
        "CodArt": "ART-1",
        "VarianteArt": "",
        "IndiceModifica": "",
        "DesArt": "Articolo test",
        "MagUM": udm,
        "Mag_6": 0,
        "Mag_0": 0,
        "Mag_10": 0,
        "Mag_11": 0,
        "Mag_12": 0,
        "Mag_13": 0,
        "PuntoRiordino": 0,
        "LottoRiordino": 0,
        "PianTempoApprovFisso": 0,
        "DataPrevistaApprovvigionamento": "",
    }


def test_acquisti_giacenza_control_flags_decimal_quantities_for_integer_udm():
    row = _giacenza_row("PZ.")
    row.update({"Mag_6": 1.5, "Mag_0": "2,25", "Mag_10": "3.00"})

    service._apply_acquisti_giacenza_controls(row)

    assert row["HasMagazziniDecimaliNonValidi"] is True
    assert row["MagazziniDecimaliNonValidi"] == [
        "6-Accettazione: 1.5",
        "0-Principale: 2.25",
    ]
    assert "UdM intera con decimali" in row["MagazziniDecimaliNonValidiText"]


def test_acquisti_giacenza_control_allows_decimal_quantities_for_decimal_udm():
    row = _giacenza_row("KG")
    row.update({"Mag_6": 1.5, "Mag_0": "2,25"})

    service._apply_acquisti_giacenza_controls(row)

    assert row["HasMagazziniDecimaliNonValidi"] is False
    assert row["MagazziniDecimaliNonValidi"] == []
    assert row["MagazziniDecimaliNonValidiText"] == ""


def test_acquisti_giacenza_excel_contains_control_column_when_needed():
    row = _giacenza_row("N.")
    row.update({"Mag_13": "4,75"})
    service._apply_acquisti_giacenza_controls(row)

    wb = service._build_acquisti_excel_workbook("giacenza", [row])
    ws = wb.active
    headers = [cell.value for cell in ws[1]]
    control_col = headers.index("Controllo") + 1

    assert ws.cell(row=2, column=control_col).value == row[
        "MagazziniDecimaliNonValidiText"
    ]


def test_acquisti_giacenza_excel_omits_control_column_without_warnings():
    row = _giacenza_row("KG")
    row.update({"Mag_13": "4,75"})
    service._apply_acquisti_giacenza_controls(row)

    wb = service._build_acquisti_excel_workbook("giacenza", [row])
    ws = wb.active
    headers = [cell.value for cell in ws[1]]

    assert "Controllo" not in headers


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


def test_build_acquisti_materiale_rows_returns_empty_list_without_orders(monkeypatch):
    monkeypatch.setattr(service, "_base_odp_query", lambda: _FakeQuery([]))
    monkeypatch.setattr(service, "_build_acquisti_ordini_fornitore_rows", lambda: [])
    monkeypatch.setattr(service, "AcqArticoli", SimpleNamespace(query=_FakeQuery([])))
    monkeypatch.setattr(service, "AcqArticoliLookup", SimpleNamespace(query=_FakeQuery([])))
    monkeypatch.setattr(service, "AcqGiacenze", SimpleNamespace(query=_FakeQuery([])))

    assert service._build_acquisti_materiale_rows() == []


def test_purchase_orders_are_added_to_material_balance(monkeypatch):
    monkeypatch.setattr(service, "_base_odp_query", lambda: _FakeQuery([]))
    monkeypatch.setattr(
        service,
        "_build_acquisti_ordini_fornitore_rows",
        lambda: [
            {
                "CodArt": "ART-1",
                "DesArt": "Articolo test",
                "UmDoc": "PZ",
                "Quantita": "5",
                "Ordine": "OF 10",
                "Fornitore": "Fornitore Test",
                "CodFornitore": "F1",
                "DataConsegnaText": "10/09/2026",
                "Stato": "Aperto",
            }
        ],
    )
    monkeypatch.setattr(service, "AcqArticoli", SimpleNamespace(query=_FakeQuery([])))
    monkeypatch.setattr(service, "AcqArticoliLookup", SimpleNamespace(query=_FakeQuery([])))
    monkeypatch.setattr(service, "AcqGiacenze", SimpleNamespace(query=_FakeQuery([])))

    row = service._build_acquisti_materiale_rows()[0]

    assert row["Acquisti"] == 5
    assert row["RimanenzaMateriale"] == 5
    assert row["AcquistiDettagli"][0]["Ordine"] == "OF 10"


def test_material_excel_has_aligned_purchase_column():
    row = {
        "CodArt": "ART-1",
        "AcquistiText": "5",
        "Mag0Missing": False,
    }

    ws = service._build_acquisti_excel_workbook("materiale", [row]).active

    headers = [cell.value for cell in ws[1]]
    assert len(headers) == len(ws[2])
    assert ws.cell(2, headers.index("Acquisti") + 1).value == "5"
