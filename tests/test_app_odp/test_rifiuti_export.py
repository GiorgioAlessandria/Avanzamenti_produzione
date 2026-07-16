from decimal import Decimal
from types import SimpleNamespace

from openpyxl import load_workbook

from app_odp.services.rifiuti_service import build_rifiuti_export


def test_build_rifiuti_export_contains_expected_columns_and_values():
    carico = SimpleNamespace(
        id=12,
        caricato_il="2026-07-15T10:30:00+02:00",
        cer=SimpleNamespace(codice="15 01 01", descrizione="Carta e cartone"),
        peso_kg=Decimal("12.500"),
        stato="SMALTITO",
        caricato_da_nome="Mario Rossi",
        smaltito_il="2026-07-15T11:00:00+02:00",
        smaltito_da_nome="Anna Bianchi",
        note="Prova",
    )

    workbook = load_workbook(build_rifiuti_export([carico]))
    sheet = workbook["Rifiuti"]

    assert [cell.value for cell in sheet[1]] == [
        "ID",
        "Data caricamento",
        "Codice CER",
        "Descrizione CER",
        "Peso kg",
        "Stato",
        "Caricato da",
        "Data smaltimento",
        "Smaltito da",
        "Note",
    ]
    assert [cell.value for cell in sheet[2]] == [
        12,
        "15/07/2026 10:30",
        "15 01 01",
        "Carta e cartone",
        12.5,
        "SMALTITO",
        "Mario Rossi",
        "15/07/2026 11:00",
        "Anna Bianchi",
        "Prova",
    ]
