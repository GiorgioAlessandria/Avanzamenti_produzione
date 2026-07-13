import json

import pandas as pd

from sync import sync_acq


def test_build_acq_fabbisogno_odp_expands_distinta_and_groups_duplicates(monkeypatch):
    monkeypatch.setattr(sync_acq, "ELEMENTI_ESCLUSI", {"CodArt": ["SKIP"]})
    distinta = [
        {"CodArt": "COMP1", "NumFase": "10", "VarianteArt": "-", "Quantita": "1,5"},
        {"CodArt": "COMP1", "NumFase": "10", "VarianteArt": "", "Quantita": "2"},
        {"CodArt": "SKIP", "NumFase": "10", "Quantita": "9"},
        {"CodArt": "", "NumFase": "10", "Quantita": "9"},
        {"CodArt": "COMP2", "NumFase": "", "Quantita": "9"},
    ]
    df = pd.DataFrame(
        [
            {
                "IdDocumento": "DOC1",
                "IdRiga": "1",
                "DistintaMateriale": json.dumps(distinta),
            }
        ]
    )

    rows = sync_acq.build_acq_fabbisogno_odp(df).to_dict("records")

    assert len(rows) == 1
    assert rows[0]["IdDocumento"] == "DOC1"
    assert rows[0]["IdRiga"] == "1"
    assert rows[0]["NumFase"] == "10"
    assert rows[0]["CodArt"] == "COMP1"
    assert rows[0]["VarianteArt"] == ""
    assert rows[0]["QuantitaNecessaria"] == 3.5


def test_build_acq_fabbisogno_odp_returns_expected_columns_for_empty_input():
    out = sync_acq.build_acq_fabbisogno_odp(pd.DataFrame())

    assert list(out.columns) == [
        "IdDocumento",
        "IdRiga",
        "NumFase",
        "CodArt",
        "VarianteArt",
        "QuantitaNecessaria",
        "synced_at",
    ]
    assert out.empty


def test_build_acq_riepilogo_materiali_merges_stock_and_article_settings():
    fabbisogno = pd.DataFrame(
        [
            {
                "IdDocumento": "DOC1",
                "IdRiga": "1",
                "NumFase": "10",
                "CodArt": "COMP1",
                "VarianteArt": "",
                "QuantitaNecessaria": 3.5,
            }
        ]
    )
    articoli = pd.DataFrame(
        [
            {
                "CodArt": "COMP1",
                "LottoRiordino": 10.0,
                "PuntoRiordino": 5.0,
                "PianTempoApprovFisso": 2,
                "DataPrevistaApprovvigionamento": "2026-07-14",
            }
        ]
    )
    giacenze = pd.DataFrame(
        [
            {"CodArt": "COMP1", "VarianteArt": "", "Giacenza": 1.0},
            {"CodArt": "COMP1", "VarianteArt": "", "Giacenza": 2.5},
        ]
    )

    rows = sync_acq.build_acq_riepilogo_materiali(
        fabbisogno,
        articoli,
        giacenze,
    ).to_dict("records")

    assert len(rows) == 1
    assert rows[0]["CodArt"] == "COMP1"
    assert rows[0]["QuantitaNecessaria"] == 3.5
    assert rows[0]["GiacenzaTotale"] == 3.5
    assert rows[0]["LottoRiordino"] == 10.0
    assert rows[0]["PuntoRiordino"] == 5.0
    assert rows[0]["PianTempoApprovFisso"] == 2
    assert rows[0]["DataPrevistaApprovvigionamento"] == "2026-07-14"

