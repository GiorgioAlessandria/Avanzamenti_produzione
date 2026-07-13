from datetime import date

import pandas as pd

from sync import sync_acq


def test_build_acq_articoli_cleans_rows_and_keeps_last_duplicate(monkeypatch):
    monkeypatch.setattr(sync_acq, "_now_local_date", lambda: date(2026, 7, 10))
    df = pd.DataFrame(
        [
            {
                "CodArt": " A1 ",
                "DesArt": "vecchia",
                "IndiceModifica": "-",
                "MagUM": " PZ ",
                "LottoRiordino": "1,5",
                "PuntoRiordino": "2",
                "PianTempoApprovFisso": "1",
            },
            {
                "CodArt": "A1",
                "DesArt": "nuova",
                "IndiceModifica": "X",
                "MagUM": "KG",
                "LottoRiordino": "3",
                "PuntoRiordino": "4",
                "PianTempoApprovFisso": "2",
            },
            {"CodArt": " "},
        ]
    )

    rows = sync_acq.build_acq_articoli(df).to_dict("records")

    assert len(rows) == 1
    assert rows[0]["CodArt"] == "A1"
    assert rows[0]["DesArt"] == "nuova"
    assert rows[0]["IndiceModifica"] == ""
    assert rows[0]["MagUM"] == "KG"
    assert rows[0]["LottoRiordino"] == 3.0
    assert rows[0]["PuntoRiordino"] == 4.0
    assert rows[0]["PianTempoApprovFisso"] == 2
    assert rows[0]["DataPrevistaApprovvigionamento"] == "2026-07-14"


def test_build_acq_giacenze_sums_by_article_variant_and_warehouse():
    df = pd.DataFrame(
        [
            {"CodArt": " A1 ", "VarianteArt": "-", "CodMag": " 0 ", "Giacenza": "1,5"},
            {"CodArt": "A1", "VarianteArt": "", "CodMag": "0", "Giacenza": "2"},
            {"CodArt": "A1", "VarianteArt": "V1", "CodMag": "0", "Giacenza": "3"},
            {"CodArt": "", "VarianteArt": "", "CodMag": "0", "Giacenza": "9"},
        ]
    )

    rows = sync_acq.build_acq_giacenze(df).sort_values(["VarianteArt"]).to_dict("records")

    assert len(rows) == 2
    assert rows[0]["CodArt"] == "A1"
    assert rows[0]["VarianteArt"] == ""
    assert rows[0]["CodMag"] == "0"
    assert rows[0]["Giacenza"] == 3.5
    assert rows[1]["VarianteArt"] == "V1"
    assert rows[1]["Giacenza"] == 3.0

