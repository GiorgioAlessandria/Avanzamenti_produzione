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