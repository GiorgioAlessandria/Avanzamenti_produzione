import importlib
import sys

import pandas as pd
import pytest

MODULE_PATH = "sync.sync_giacenze"


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


def test_inserimento_descrizione_famiglia_merges_requested_columns_and_renames(mod):
    df = pd.DataFrame(
        [
            {"CodArt": "A1", "CodFamiglia": "F1"},
            {"CodArt": "B1", "CodFamiglia": "F2"},
        ]
    )
    famiglie = pd.DataFrame(
        [
            {"CodFamiglia": "F1", "Des": "Famiglia uno"},
            {"CodFamiglia": "F3", "Des": "Famiglia tre"},
        ]
    )

    rows = mod.inserimento_descrizione_famiglia(
        df,
        famiglie,
        colonna_merge="CodFamiglia",
        lista_colonne_da_inserire=["CodFamiglia", "Des"],
        colonna_da_rinominare="Des",
        colonna_rinominata="DesFamiglia",
    ).to_dict("records")

    assert rows[0]["CodArt"] == "A1"
    assert rows[0]["DesFamiglia"] == "Famiglia uno"
    assert rows[1]["CodArt"] == "B1"
    assert pd.isna(rows[1]["DesFamiglia"])
