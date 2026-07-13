import importlib
import sys

import pandas as pd
import pytest

MODULE_PATH = "sync.estrazione_inventario"


@pytest.fixture()
def mod(tmp_path, monkeypatch):
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "filtri_sync.toml").write_text(
        "[Elementi_esclusi]\nCodArt = []\n[Elementi_selezionati]\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    sys.modules.pop(MODULE_PATH, None)
    return importlib.import_module(MODULE_PATH)


def test_collapse_by_keys_sums_quantities_and_numeric_columns(mod):
    df = pd.DataFrame(
        [
            {"CodArt": "A1", "CodMag": "0", "Giacenza": 1.5, "Valore": 10, "DesArt": "prima"},
            {"CodArt": "A1", "CodMag": "0", "Giacenza": 2.5, "Valore": 20, "DesArt": "seconda"},
            {"CodArt": "B1", "CodMag": "0", "Giacenza": 3.0, "Valore": 5, "DesArt": "terza"},
        ]
    )

    rows = mod._collapse_by_keys(df, keys=("CodArt", "CodMag"), qty_col="Giacenza").to_dict(
        "records"
    )

    assert rows == [
        {"CodArt": "A1", "CodMag": "0", "Giacenza": 4.0, "Valore": 30, "DesArt": "prima"},
        {"CodArt": "B1", "CodMag": "0", "Giacenza": 3.0, "Valore": 5, "DesArt": "terza"},
    ]


def test_expand_giacenza_with_lotti_uses_lot_quantities_and_keeps_unlotted_rows(mod):
    df_giacenza = pd.DataFrame(
        [
            {"CodArt": "A1", "CodMag": "0", "Giacenza": 10.0, "DesArt": "articolo A"},
            {"CodArt": "B1", "CodMag": "0", "Giacenza": 5.0, "DesArt": "articolo B"},
        ]
    )
    df_lotti = pd.DataFrame(
        [
            {"CodArt": "A1", "CodMag": "0", "RifLottoAlfa": " L1 ", "Giacenza": 2.0},
            {"CodArt": "A1", "CodMag": "0", "RifLottoAlfa": "L1", "Giacenza": 3.0},
            {"CodArt": "A1", "CodMag": "0", "RifLottoAlfa": "L2", "Giacenza": 4.0},
        ]
    )

    rows = (
        mod.expand_giacenza_with_lotti(df_giacenza, df_lotti)
        .sort_values(["CodArt", "RifLottoAlfa"], na_position="last")
        .to_dict("records")
    )

    assert rows[0]["CodArt"] == "A1"
    assert rows[0]["RifLottoAlfa"] == "L1"
    assert rows[0]["Giacenza"] == 5.0
    assert rows[1]["RifLottoAlfa"] == "L2"
    assert rows[1]["Giacenza"] == 4.0
    assert rows[2]["CodArt"] == "B1"
    assert pd.isna(rows[2]["RifLottoAlfa"])
    assert rows[2]["Giacenza"] == 5.0
