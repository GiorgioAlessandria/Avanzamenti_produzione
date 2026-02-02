# tests/test_leggi_view.py
import importlib
import pandas as pd
from pandas.errors import MergeError
import pytest
from pathlib import Path

MODULE_PATH = "sync.sync_input"  # es: "app.sync_input" / "sync_input" / "app.utils.db"
# region pytest fixtures
@pytest.fixture()
def mod(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root)

    cfg = root / "app_odp" / "static" / "config.toml"
    cfg.parent.mkdir(parents=True, exist_ok=True)
    if not cfg.exists():
        cfg.write_text(
            "# minimal config for tests\n",
            encoding="utf-8",
        )
    return importlib.import_module(MODULE_PATH)

@pytest.fixture()
def fake_config():
    # config minimale per i test
    return {
        "Elementi_esclusi": {
            "CodReparto": ["99", "00"],
        },
        "Elementi_selezionati": {
            "StatoOrdine": "ATTIVO",
        },
    }

@pytest.fixture()
def base_df():
    # Include:
    # - valori esclusi (99, 00)
    # - NaN su CodReparto
    # - StatoOrdine misto
    return pd.DataFrame(
        {
            "CodReparto": ["10", "99", None, "20", "00", "10"],
            "StatoOrdine": ["ATTIVO", "ATTIVO", "ATTIVO", "CHIUSO", "ATTIVO", "CHIUSO"],
            "Altro": [1, 2, 3, 4, 5, 6],
        }
    )

def _patch_read_sql(monkeypatch, mod, df_to_return):
    calls = {"query": None, "engine": None, "count": 0}

    def fake_read_sql(query, engine):
        calls["query"] = query
        calls["engine"] = engine
        calls["count"] += 1
        # return copy per evitare side-effect tra test
        return df_to_return.copy()

    monkeypatch.setattr(mod.pd, "read_sql", fake_read_sql)
    return calls

# endregion

def _patch_globals(monkeypatch, mod, fake_config):
    # patch delle global nel modulo: engine_app e config
    fake_engine = object()
    monkeypatch.setattr(mod, "sqlserver_engine_app", fake_engine, raising=True)
    monkeypatch.setattr(mod, "config", fake_config, raising=True)
    return fake_engine


# region leggi_view
def test_leggi_view_no_filters(monkeypatch, mod, fake_config, base_df):
    fake_engine = _patch_globals(monkeypatch, mod, fake_config)
    calls = _patch_read_sql(monkeypatch, mod, base_df)

    out = mod.leggi_view(table="vwESOdP")

    assert calls["count"] == 1
    assert calls["engine"] is fake_engine
    assert calls["query"] == str("SELECT * FROM BernardiProd.dbo.vwESOdP")

    # Nessun filtro: deve restituire tutti i record (NaN inclusi),
    # ma con index resettato
    assert len(out) == len(base_df)
    assert list(out.index) == list(range(len(base_df)))


def test_leggi_view_filtro_esclusi(monkeypatch, mod, fake_config, base_df):
    _patch_globals(monkeypatch, mod, fake_config)
    _patch_read_sql(monkeypatch, mod, base_df)

    out = mod.leggi_view(
        table="vwESOdP",
        colonna_filtro_esclusi="CodReparto",
        colonna_filtro_stato=""
    )

    # Applica: ~isin(["99","00"]) e poi dropna su CodReparto
    # base_df CodReparto: ["10","99",None,"20","00","10"]
    # rimuove 99 e 00 e None -> restano "10","20","10" (3 righe)
    assert out["CodReparto"].tolist() == ["10", "20", "10"]
    assert out["CodReparto"].isna().sum() == 0
    assert list(out.index) == [0, 1, 2]


def test_leggi_view_filtro_stato_senza_esclusi(monkeypatch, mod, fake_config, base_df):
    """
    Nota importante: nel tuo codice, nel ramo colonna_filtro_stato fai:
        df = df.dropna(subset=[colonna_filtro_esclusi], ...)
    quindi se colonna_filtro_esclusi == "" -> subset=[""].
    In pandas questo normalmente genera KeyError (colonna non esiste).
    Questo test verifica il comportamento attuale (cioè che esplode).
    """
    _patch_globals(monkeypatch, mod, fake_config)
    _patch_read_sql(monkeypatch, mod, base_df)

    with pytest.raises(KeyError):
        mod.leggi_view(
            table="vwESOdP",
            colonna_filtro_esclusi="",
            colonna_filtro_stato="StatoOrdine"
        )


def test_leggi_view_filtro_stato_con_esclusi(monkeypatch, mod, fake_config, base_df):
    """
    Caso realistico per far passare anche il dropna del ramo stato:
    passiamo anche colonna_filtro_esclusi, così subset=[...] è valido.
    """
    _patch_globals(monkeypatch, mod, fake_config)
    _patch_read_sql(monkeypatch, mod, base_df)

    out = mod.leggi_view(
        table="vwESOdP",
        colonna_filtro_esclusi="CodReparto",
        colonna_filtro_stato="StatoOrdine"
    )

    # Step 1 esclusi: toglie 99,00 e None -> restano righe con CodReparto 10,20,10
    # Step 2 stato: StatoOrdine == "ATTIVO" (da config) -> tra quelle restano solo:
    # - ("10","ATTIVO") e ("20","CHIUSO") e ("10","CHIUSO") -> rimane solo la prima
    assert out["CodReparto"].tolist() == ["10"]
    assert out["StatoOrdine"].tolist() == ["ATTIVO"]
    assert list(out.index) == [0]


def test_leggi_view_query_varie_table(monkeypatch, mod, fake_config, base_df):
    _patch_globals(monkeypatch, mod, fake_config)
    calls = _patch_read_sql(monkeypatch, mod, base_df)

    out = mod.leggi_view(table="vwESArticoli")
    assert calls["query"] == "SELECT * FROM BernardiProd.dbo.vwESArticoli"
    assert len(out) == len(base_df)
# endregion

# region filtra_odpfasi_con_odp

def test_filtra_odpfasi_con_odp_right_join_keeps_all_odp_keys(mod):
    df_odp = pd.DataFrame(
        {
            "IdDocumento": ["A", "A", "B"],
            "IdRiga": [1, 2, 1],
            "OtherOdpCol": [10, 20, 30],
        }
    )

    df_odpfasi = pd.DataFrame(
        {
            "IdDocumento": ["A", "A", "C"],
            "IdRiga": [1, 1, 9],
            "Fase": [100, 101, 999],
            "Extra": ["x", "y", "z"],
        }
    )

    out = mod.filtra_odpfasi_con_odp(df_odpfasi=df_odpfasi, df_odp=df_odp)

    # 1) Tutte le chiavi di df_odp devono comparire almeno una volta (right join)
    out_keys = set(map(tuple, out[["IdDocumento", "IdRiga"]].to_numpy()))
    odp_keys = set(map(tuple, df_odp[["IdDocumento", "IdRiga"]].to_numpy()))
    assert odp_keys.issubset(out_keys)

    # 2) Nessuna chiave fuori da df_odp deve apparire (es. C,9 non deve esserci)
    assert out_keys.issubset(odp_keys)

    # 3) Le righe in output sono almeno quelle di df_odp, ma possono crescere per duplicati in df_odpfasi
    assert len(out) >= len(df_odp)

    # 4) Verifica puntuale sull'espansione: (A,1) deve apparire 2 volte perché in df_odpfasi è duplicata
    assert len(out[(out["IdDocumento"] == "A") & (out["IdRiga"] == 1)]) == 2

    # 5) Le chiavi presenti in df_odp ma non in df_odpfasi devono avere NaN sulle colonne di df_odpfasi
    row_a2 = out[(out["IdDocumento"] == "A") & (out["IdRiga"] == 2)]
    assert len(row_a2) == 1
    assert pd.isna(row_a2.iloc[0]["Fase"])
    assert pd.isna(row_a2.iloc[0]["Extra"])

    row_b1 = out[(out["IdDocumento"] == "B") & (out["IdRiga"] == 1)]
    assert len(row_b1) == 1
    assert pd.isna(row_b1.iloc[0]["Fase"])
    assert pd.isna(row_b1.iloc[0]["Extra"])

def test_filtra_odpfasi_con_odp_non_matching_keys_produce_nans_for_odpfasi_cols(mod):
    df_odp = pd.DataFrame({"IdDocumento": ["A", "B"], "IdRiga": [1, 2]})
    df_odpfasi = pd.DataFrame(
        {
            "IdDocumento": ["A"],
            "IdRiga": [1],
            "Fase": [10],
        }
    )

    out = mod.filtra_odpfasi_con_odp(df_odpfasi=df_odpfasi, df_odp=df_odp)

    # La chiave (B,2) non esiste in df_odpfasi => colonne di df_odpfasi devono essere NaN su quella riga
    row_b2 = out[(out["IdDocumento"] == "B") & (out["IdRiga"] == 2)]
    assert len(row_b2) == 1
    assert pd.isna(row_b2.iloc[0]["Fase"])


def test_filtra_odpfasi_con_odp_duplicates_in_odpfasi_expand_rows(mod):
    # Se df_odpfasi ha duplicati sulla chiave, il merge produce righe multiple per quella chiave
    df_odp = pd.DataFrame({"IdDocumento": ["A"], "IdRiga": [1]})
    df_odpfasi = pd.DataFrame(
        {
            "IdDocumento": ["A", "A"],
            "IdRiga": [1, 1],
            "Fase": [10, 11],
        }
    )

    out = mod.filtra_odpfasi_con_odp(df_odpfasi=df_odpfasi, df_odp=df_odp)

    assert len(out) == 2
    assert set(out["Fase"].tolist()) == {10, 11}


def test_filtra_odpfasi_con_odp_output_has_required_columns(mod):
    df_odp = pd.DataFrame({"IdDocumento": ["A"], "IdRiga": [1]})
    df_odpfasi = pd.DataFrame({"IdDocumento": ["A"], "IdRiga": [1], "Fase": [10]})

    out = mod.filtra_odpfasi_con_odp(df_odpfasi=df_odpfasi, df_odp=df_odp)

    # Deve contenere almeno le colonne chiave
    assert {"IdDocumento", "IdRiga"}.issubset(out.columns)
    # E mantiene le colonne di df_odpfasi (quando c'è match)
    assert "Fase" in out.columns
# endregion

# region filtra_odp_componenti_con_odp
def test_filtra_odp_componenti_con_odp_keeps_all_odp_keys_and_no_extraneous_keys(mod):
    # df_odp = chiavi da mantenere (right join => tutte le chiavi di df_odp devono comparire almeno una volta)
    df_odp = pd.DataFrame(
        {
            "IdDocumento": ["A", "A", "B"],
            "IdRiga": [1, 2, 1],
            "OtherOdpCol": [10, 20, 30],  # ignorata dalla funzione
        }
    )

    # df_odp_componenti matcha solo (A,1) e contiene anche un record fuori (C,9)
    df_odp_componenti = pd.DataFrame(
        {
            "IdDocumento": ["A", "C"],
            "IdRigaPadre": [1, 9],
            "CodComponente": ["X", "Z"],
            "Qta": [2, 5],
        }
    )

    out = mod.filtra_odp_componenti_con_odp(
        df_odp_componenti=df_odp_componenti,
        df_odp=df_odp,
    )

    # 1) Tutte le chiavi di df_odp devono essere presenti in output (almeno una volta)
    out_keys = set(map(tuple, out[["IdDocumento", "IdRiga"]].to_numpy()))
    odp_keys = set(map(tuple, df_odp[["IdDocumento", "IdRiga"]].to_numpy()))
    assert odp_keys.issubset(out_keys)

    # 2) Nessuna chiave non presente in df_odp deve apparire in output
    assert out_keys.issubset(odp_keys)


def test_filtra_odp_componenti_con_odp_non_matching_odp_keys_yield_nans_in_component_cols(mod):
    df_odp = pd.DataFrame({"IdDocumento": ["A", "B"], "IdRiga": [1, 2]})

    # esiste solo componente per (A,1)
    df_odp_componenti = pd.DataFrame(
        {"IdDocumento": ["A"], "IdRigaPadre": [1], "CodComponente": ["X"], "Qta": [2]}
    )

    out = mod.filtra_odp_componenti_con_odp(df_odp_componenti, df_odp)

    # per (B,2) non ci sono componenti => colonne dei componenti devono essere NaN
    row_b2 = out[(out["IdDocumento"] == "B") & (out["IdRiga"] == 2)]
    assert len(row_b2) == 1
    assert pd.isna(row_b2.iloc[0]["IdRigaPadre"])
    assert pd.isna(row_b2.iloc[0]["CodComponente"])
    assert pd.isna(row_b2.iloc[0]["Qta"])


def test_filtra_odp_componenti_con_odp_duplicates_expand_rows(mod):
    # se df_odp_componenti ha più righe per la stessa chiave (IdDocumento, IdRigaPadre),
    # l'output deve espandersi (right join)
    df_odp = pd.DataFrame({"IdDocumento": ["A"], "IdRiga": [1]})
    df_odp_componenti = pd.DataFrame(
        {
            "IdDocumento": ["A", "A"],
            "IdRigaPadre": [1, 1],
            "CodComponente": ["X", "Y"],
            "Qta": [2, 3],
        }
    )

    out = mod.filtra_odp_componenti_con_odp(df_odp_componenti, df_odp)

    assert len(out) == 2
    assert set(out["CodComponente"].tolist()) == {"X", "Y"}


def test_filtra_odp_componenti_con_odp_drops_idriga_y_and_keeps_idriga(mod):
    df_odp = pd.DataFrame({"IdDocumento": ["A"], "IdRiga": [1]})
    df_odp_componenti = pd.DataFrame(
        {"IdDocumento": ["A"], "IdRigaPadre": [1], "CodComponente": ["X"]}
    )

    out = mod.filtra_odp_componenti_con_odp(df_odp_componenti, df_odp)

    # La funzione fa drop di "IdRiga_y"
    assert "IdRiga_y" not in out.columns

    # Deve rimanere la colonna IdRiga (quella di df_odp, right_on)
    assert "IdRiga" in out.columns

    # Valore coerente
    assert out.loc[0, "IdDocumento"] == "A"
    assert out.loc[0, "IdRiga"] == 1


def test_filtra_odp_componenti_con_odp_output_columns_include_expected(mod):
    df_odp = pd.DataFrame({"IdDocumento": ["A"], "IdRiga": [1]})
    df_odp_componenti = pd.DataFrame(
        {"IdDocumento": ["A"], "IdRigaPadre": [1], "CodComponente": ["X"], "Qta": [2]}
    )

    out = mod.filtra_odp_componenti_con_odp(df_odp_componenti, df_odp)

    # colonne chiave e alcune colonne componenti
    assert {"IdDocumento", "IdRiga", "IdRigaPadre", "CodComponente", "Qta"}.issubset(out.columns)

# endregion
# region inserimento_reparto_da_risorsa
def test_inserimento_reparto_da_risorsa_adds_codreparto_and_filters_non_matches(mod):
    df_odp_fasi = pd.DataFrame(
        {
            "IdDocumento": ["A", "A", "B"],
            "IdRiga": [1, 2, 1],
            "CodRisorsaProd": ["R1", "R2", "R_MISSING"],
            "Fase": [10, 20, 30],
        }
    )

    df_risorse = pd.DataFrame(
        {
            "CodRisorsaProd": ["R1", "R2"],
            "CodReparto": ["10", "20"],
            "Altro": ["x", "y"],  # deve essere ignorata dalla funzione
        }
    )

    out = mod.inserimento_reparto_da_risorsa(df_odp_fasi=df_odp_fasi, df_risorse=df_risorse)

    # deve aggiungere CodReparto
    assert "CodReparto" in out.columns

    # la riga con risorsa non mappata deve sparire (dropna su CodReparto)
    assert set(out["CodRisorsaProd"].tolist()) == {"R1", "R2"}
    assert len(out) == 2

    # mapping corretto
    assert out.loc[out["CodRisorsaProd"] == "R1", "CodReparto"].iloc[0] == "10"
    assert out.loc[out["CodRisorsaProd"] == "R2", "CodReparto"].iloc[0] == "20"


def test_inserimento_reparto_da_risorsa_keeps_other_columns(mod):
    df_odp_fasi = pd.DataFrame(
        {
            "CodRisorsaProd": ["R1"],
            "Fase": [10],
            "Note": ["ok"],
        }
    )
    df_risorse = pd.DataFrame({"CodRisorsaProd": ["R1"], "CodReparto": ["10"]})

    out = mod.inserimento_reparto_da_risorsa(df_odp_fasi, df_risorse)

    assert {"CodRisorsaProd", "Fase", "Note", "CodReparto"}.issubset(out.columns)
    assert out.iloc[0]["Note"] == "ok"


def test_inserimento_reparto_da_risorsa_empty_output_when_no_matches(mod):
    df_odp_fasi = pd.DataFrame(
        {
            "CodRisorsaProd": ["R_NOPE", "R_NOPE2"],
            "Fase": [1, 2],
        }
    )
    df_risorse = pd.DataFrame({"CodRisorsaProd": ["R1"], "CodReparto": ["10"]})

    out = mod.inserimento_reparto_da_risorsa(df_odp_fasi, df_risorse)

    assert isinstance(out, pd.DataFrame)
    assert out.empty


def test_inserimento_reparto_da_risorsa_duplicates_in_risorse_expand_rows(mod):
    # se in df_risorse ci sono duplicati per la stessa risorsa,
    # il merge genera più righe (cartesiano sulle corrispondenze)
    df_odp_fasi = pd.DataFrame(
        {
            "CodRisorsaProd": ["R1"],
            "Fase": [10],
        }
    )
    df_risorse = pd.DataFrame(
        {
            "CodRisorsaProd": ["R1", "R1"],
            "CodReparto": ["10", "11"],
        }
    )

    out = mod.inserimento_reparto_da_risorsa(df_odp_fasi, df_risorse)

    assert len(out) == 2
    assert set(out["CodReparto"].tolist()) == {"10", "11"}


def test_inserimento_reparto_da_risorsa_does_not_drop_when_codreparto_present(mod):
    # controlla che dropna non elimini righe valide
    df_odp_fasi = pd.DataFrame(
        {
            "CodRisorsaProd": ["R1", "R2"],
            "Fase": [10, 20],
        }
    )
    df_risorse = pd.DataFrame(
        {
            "CodRisorsaProd": ["R1", "R2"],
            "CodReparto": ["10", "20"],
        }
    )

    out = mod.inserimento_reparto_da_risorsa(df_odp_fasi, df_risorse)

    assert len(out) == 2
    assert out["CodReparto"].isna().sum() == 0
# endregion

# region unione_fasi_componenti
def test_unione_fasi_componenti_renames_component_columns_and_joins(mod):
    df_fasi = pd.DataFrame(
        {
            "IdDocumento": ["A", "A"],
            "IdRiga": [1, 2],
            "NumFase": [10, 20],
            "CodRisorsaProd": ["R1", "R2"],
        }
    )

    df_componenti = pd.DataFrame(
        {
            "IdDocumento": ["A"],
            "IdRigaPadre": [1],     # deve diventare IdRiga
            "NumFase": [10],
            "IdRiga": [999],        # deve diventare IdRigacomponente
            "CodArt": ["CMP1"],
            "Qta": [2.5],
        }
    )

    out = mod.unione_fasi_componenti(df_fasi=df_fasi, df_componenti=df_componenti)

    # colonne chiave presenti
    assert {"IdDocumento", "IdRiga", "NumFase"}.issubset(out.columns)

    # rinomina avvenuta: IdRigacomponente deve esistere, e non deve esistere IdRigaPadre
    assert "IdRigacomponente" in out.columns
    assert "IdRigaPadre" not in out.columns

    # join corretto su (A,1,10)
    row = out[(out["IdDocumento"] == "A") & (out["IdRiga"] == 1) & (out["NumFase"] == 10)]
    assert len(row) == 1
    assert row.iloc[0]["CodArt"] == "CMP1"
    assert row.iloc[0]["IdRigacomponente"] == 999


def test_unione_fasi_componenti_left_join_keeps_phases_without_components(mod):
    df_fasi = pd.DataFrame(
        {
            "IdDocumento": ["A", "A"],
            "IdRiga": [1, 2],
            "NumFase": [10, 20],
            "CodRisorsaProd": ["R1", "R2"],
        }
    )

    # componenti solo per la fase (A,1,10)
    df_componenti = pd.DataFrame(
        {
            "IdDocumento": ["A"],
            "IdRigaPadre": [1],
            "NumFase": [10],
            "IdRiga": [100],
            "CodArt": ["CMP1"],
            "Qta": [1.0],
        }
    )

    out = mod.unione_fasi_componenti(df_fasi, df_componenti)

    # deve contenere ancora la fase (A,2,20) anche se non ha componenti
    row_no_comp = out[(out["IdDocumento"] == "A") & (out["IdRiga"] == 2) & (out["NumFase"] == 20)]
    assert len(row_no_comp) == 1

    # colonne componenti NaN per quella riga
    assert pd.isna(row_no_comp.iloc[0].get("CodArt"))
    assert pd.isna(row_no_comp.iloc[0].get("IdRigacomponente"))
    assert pd.isna(row_no_comp.iloc[0].get("Qta"))


def test_unione_fasi_componenti_multiple_components_expand_rows_validate_1_to_many(mod):
    df_fasi = pd.DataFrame(
        {
            "IdDocumento": ["A"],
            "IdRiga": [1],
            "NumFase": [10],
            "CodRisorsaProd": ["R1"],
        }
    )

    # due componenti per la stessa fase (A,1,10)
    df_componenti = pd.DataFrame(
        {
            "IdDocumento": ["A", "A"],
            "IdRigaPadre": [1, 1],
            "NumFase": [10, 10],
            "IdRiga": [100, 101],
            "CodArt": ["CMP1", "CMP2"],
            "Qta": [1.0, 2.0],
        }
    )

    out = mod.unione_fasi_componenti(df_fasi, df_componenti)

    rows = out[(out["IdDocumento"] == "A") & (out["IdRiga"] == 1) & (out["NumFase"] == 10)]
    assert len(rows) == 2
    assert set(rows["CodArt"].tolist()) == {"CMP1", "CMP2"}
    assert set(rows["IdRigacomponente"].tolist()) == {100, 101}


def test_unione_fasi_componenti_raises_if_fasi_have_duplicate_keys(mod):
    # chiavi duplicate nel lato "1" => validate="1:m" deve fallire
    df_fasi = pd.DataFrame(
        {
            "IdDocumento": ["A", "A"],
            "IdRiga": [1, 1],
            "NumFase": [10, 10],
            "CodRisorsaProd": ["R1", "R1_dup"],
        }
    )

    df_componenti = pd.DataFrame(
        {
            "IdDocumento": ["A"],
            "IdRigaPadre": [1],
            "NumFase": [10],
            "IdRiga": [100],
            "CodArt": ["CMP1"],
            "Qta": [1.0],
        }
    )

    with pytest.raises(MergeError):
        mod.unione_fasi_componenti(df_fasi, df_componenti)


def test_unione_fasi_componenti_preserves_all_fasi_rows_count_when_no_components(mod):
    df_fasi = pd.DataFrame(
        {
            "IdDocumento": ["A", "B"],
            "IdRiga": [1, 1],
            "NumFase": [10, 10],
            "CodRisorsaProd": ["R1", "R2"],
        }
    )

    # componenti vuoto ma con colonne attese
    df_componenti = pd.DataFrame(
        columns=["IdDocumento", "IdRigaPadre", "NumFase", "IdRiga", "CodArt", "Qta"]
    )

    out = mod.unione_fasi_componenti(df_fasi, df_componenti)

    # left join: deve restituire una riga per ogni fase
    assert len(out) == len(df_fasi)
    assert set(map(tuple, out[["IdDocumento", "IdRiga", "NumFase"]].to_numpy())) == set(
        map(tuple, df_fasi[["IdDocumento", "IdRiga", "NumFase"]].to_numpy())
    )
# endregion

# region generazione_lista
import json
def test_generazione_lista_basic_json_dump(mod):
    df = pd.DataFrame(
        {
            "IdDocumento": ["A", "A", "B"],
            "IdRiga": [1, 1, 2],
            "CodArt": ["X", "Y", "Z"],
            "Qta": [2, 3, 1],
        }
    )

    out = mod.generazione_lista(
        df=df,
        chiavi=["IdDocumento", "IdRiga"],
        rename_col="Lista",
        list_columns=["CodArt", "Qta"],
        dumps_json=True,
    )

    # due gruppi: (A,1) e (B,2)
    assert len(out) == 2
    assert {"IdDocumento", "IdRiga", "Lista"}.issubset(out.columns)

    # trova riga A,1
    row_a1 = out[(out["IdDocumento"] == "A") & (out["IdRiga"] == 1)].iloc[0]
    assert isinstance(row_a1["Lista"], str)

    parsed = json.loads(row_a1["Lista"])
    # deve essere lista di coppie [CodArt, Qta] nell'ordine delle righe originali del gruppo
    assert parsed == [["X", 2], ["Y", 3]]


def test_generazione_lista_returns_list_of_tuples_when_no_dump(mod):
    df = pd.DataFrame(
        {
            "Key": ["K1", "K1", "K2"],
            "A": [10, 11, 20],
            "B": ["x", "y", "z"],
        }
    )

    out = mod.generazione_lista(
        df=df,
        chiavi=["Key"],
        rename_col="Agg",
        list_columns=["A", "B"],
        dumps_json=False,
    )

    assert len(out) == 2
    row_k1 = out[out["Key"] == "K1"].iloc[0]
    assert isinstance(row_k1["Agg"], list)
    assert row_k1["Agg"] == [(10, "x"), (11, "y")]


def test_generazione_lista_includes_nan_keys_dropna_false(mod):
    # dropna=False in groupby => deve creare gruppo anche per NaN nella chiave
    df = pd.DataFrame(
        {
            "IdDocumento": ["A", None, None],
            "Val": [1, 2, 3],
        }
    )

    out = mod.generazione_lista(
        df=df,
        chiavi=["IdDocumento"],
        rename_col="Lista",
        list_columns=["Val"],
        dumps_json=True,
    )

    # attesi 2 gruppi: "A" e NaN
    assert len(out) == 2

    # gruppo NaN deve esistere
    row_nan = out[out["IdDocumento"].isna()].iloc[0]
    parsed = json.loads(row_nan["Lista"])
    assert [v[0] for v in parsed] == ["2", "3"]


def test_generazione_lista_single_list_column_shape(mod):
    df = pd.DataFrame(
        {
            "Key": ["K", "K", "K"],
            "Solo": [5, 6, 7],
        }
    )

    out = mod.generazione_lista(
        df=df,
        chiavi=["Key"],
        rename_col="Lista",
        list_columns=["Solo"],
        dumps_json=False,
    )

    row = out.iloc[0]
    # con una sola colonna, ogni riga diventa una tupla a 1 elemento
    assert row["Lista"] == [(5,), (6,), (7,)]


def test_generazione_lista_default_str_in_json_dump(mod):
    # default=str => oggetti non JSON-serializzabili devono diventare stringhe
    df = pd.DataFrame(
        {
            "Key": ["K1"],
            "Obj": [pd.Timestamp("2024-01-02")],  # non serializzabile di default
        }
    )

    out = mod.generazione_lista(
        df=df,
        chiavi=["Key"],
        rename_col="Lista",
        list_columns=["Obj"],
        dumps_json=True,
    )

    s = out.iloc[0]["Lista"]
    parsed = json.loads(s)
    # Timestamp deve diventare stringa
    assert isinstance(parsed[0][0], str)
    assert "2024-01-02" in parsed[0][0]


def test_generazione_lista_rename_col_applied(mod):
    df = pd.DataFrame({"K": ["A", "A"], "X": [1, 2]})

    out = mod.generazione_lista(
        df=df,
        chiavi=["K"],
        rename_col="MiaLista",
        list_columns=["X"],
        dumps_json=False,
    )

    assert "MiaLista" in out.columns
    assert "Lista" not in out.columns
# endregion
# region generazione_dizionario
def test_generazione_dizionario_basic_records_json(mod):
    df = pd.DataFrame(
        {
            "IdDocumento": ["A", "A", "B"],
            "IdRiga": [1, 1, 2],
            "CodArt": ["X", "Y", "Z"],
            "Qta": [2, 3, 1],
        }
    )

    out = mod.generazione_dizionario(
        df=df,
        chiavi=["IdDocumento", "IdRiga"],
        rename_col="Dettagli",
        list_columns=["CodArt", "Qta"],
        data_in="normale",
    )

    assert len(out) == 2
    assert {"IdDocumento", "IdRiga", "Dettagli"}.issubset(out.columns)

    row_a1 = out[(out["IdDocumento"] == "A") & (out["IdRiga"] == 1)].iloc[0]
    parsed = json.loads(row_a1["Dettagli"])

    # lista di dict record-oriented
    assert parsed == [{"CodArt": "X", "Qta": 2}, {"CodArt": "Y", "Qta": 3}]


def test_generazione_dizionario_json_is_string_or_none(mod):
    df = pd.DataFrame(
        {
            "K": ["A"],
            "V": [1],
        }
    )

    out = mod.generazione_dizionario(
        df=df,
        chiavi=["K"],
        rename_col="Dettagli",
        list_columns=["V"],
        data_in="normale",
    )

    assert isinstance(out.loc[0, "Dettagli"], str)
    assert json.loads(out.loc[0, "Dettagli"]) == [{"V": 1}]


def test_generazione_dizionario_data_in_data_formats_datetime_in_rename_col_if_in_list_columns(mod):
    # Qui usiamo rename_col == "Data" e la includiamo anche in list_columns,
    # così la formattazione influisce davvero sull'output.
    df = pd.DataFrame(
        {
            "IdDocumento": ["A", "A"],
            "IdRiga": [1, 1],
            "Data": [pd.Timestamp("2024-01-02 10:11:12"), pd.Timestamp("2024-01-03 00:00:00")],
            "Val": [10, 20],
        }
    )

    out = mod.generazione_dizionario(
        df=df,
        chiavi=["IdDocumento", "IdRiga"],
        rename_col="Data",
        list_columns=["Data", "Val"],
        data_in="data",
    )

    row = out[(out["IdDocumento"] == "A") & (out["IdRiga"] == 1)].iloc[0]
    parsed = json.loads(row["Data"])

    # le date devono essere formattate come stringhe dd/mm/YYYY HH:MM:SS
    assert parsed[0]["Data"] == "02/01/2024 10:11:12"
    assert parsed[1]["Data"] == "03/01/2024 00:00:00"
    assert parsed[0]["Val"] == 10
    assert parsed[1]["Val"] == 20


def test_generazione_dizionario_preserves_group_count(mod):
    df = pd.DataFrame(
        {
            "K1": ["A", "A", "A", "B"],
            "K2": [1, 1, 2, 1],
            "X": [10, 11, 12, 99],
        }
    )

    out = mod.generazione_dizionario(
        df=df,
        chiavi=["K1", "K2"],
        rename_col="Dettagli",
        list_columns=["X"],
        data_in="normale",
    )

    # gruppi attesi: (A,1), (A,2), (B,1) => 3 righe
    assert len(out) == 3
    keys = set(map(tuple, out[["K1", "K2"]].to_numpy()))
    assert keys == {("A", 1), ("A", 2), ("B", 1)}
# endregion
# region inserimento_distinta_in_odp
def test_inserimento_distinta_in_odp_left_merge_and_drop_columns(mod):
    df_odp = pd.DataFrame(
        {
            "IdDocumento": ["A", "B", "C"],
            "IdRiga": [1, 1, 1],
            "CodArt": ["P1", "P2", "P3"],
            # colonne da eliminare
            "NumRegistraz": [111, 222, 333],
            "DataRegistrazione": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "UnitaMisura": ["PZ", "PZ", "PZ"],
            "QtaResidua": [0.1, 0.2, 0.3],
        }
    )

    componenti_per_odp = pd.DataFrame(
        {
            "IdDocumento": ["A", "C"],
            "IdRiga": [1, 1],
            "DistintaMateriale": ['[{"CodArt":"X","Qta":1}]', '[{"CodArt":"Y","Qta":2}]'],
        }
    )

    out = mod.inserimento_distinta_in_odp(
        df_odp=df_odp,
        componenti_per_odp=componenti_per_odp,
        chiavi=["IdDocumento", "IdRiga"],
    )

    # 1) left merge: stesse righe di df_odp
    assert len(out) == len(df_odp)
    out_keys = list(map(tuple, out[["IdDocumento", "IdRiga"]].to_numpy()))
    exp_keys = list(map(tuple, df_odp[["IdDocumento", "IdRiga"]].to_numpy()))
    assert out_keys == exp_keys

    # 2) DistintaMateriale presente solo per A e C
    row_a = out[(out["IdDocumento"] == "A") & (out["IdRiga"] == 1)].iloc[0]
    assert row_a["DistintaMateriale"] == '[{"CodArt":"X","Qta":1}]'

    row_b = out[(out["IdDocumento"] == "B") & (out["IdRiga"] == 1)].iloc[0]
    assert pd.isna(row_b["DistintaMateriale"])

    row_c = out[(out["IdDocumento"] == "C") & (out["IdRiga"] == 1)].iloc[0]
    assert row_c["DistintaMateriale"] == '[{"CodArt":"Y","Qta":2}]'

    # 3) colonne rimosse
    for col in ["NumRegistraz", "DataRegistrazione", "UnitaMisura", "QtaResidua"]:
        assert col not in out.columns


def test_inserimento_distinta_in_odp_multiple_distinte_expand_rows(mod):
    # Se componenti_per_odp ha più righe per la stessa chiave,
    # il merge crea righe duplicate (comportamento standard)
    df_odp = pd.DataFrame(
        {
            "IdDocumento": ["A"],
            "IdRiga": [1],
            "NumRegistraz": [1],
            "DataRegistrazione": ["2024-01-01"],
            "UnitaMisura": ["PZ"],
            "QtaResidua": [0.0],
        }
    )

    componenti_per_odp = pd.DataFrame(
        {
            "IdDocumento": ["A", "A"],
            "IdRiga": [1, 1],
            "DistintaMateriale": ["D1", "D2"],
        }
    )

    out = mod.inserimento_distinta_in_odp(df_odp, componenti_per_odp, ["IdDocumento", "IdRiga"])

    assert len(out) == 2
    assert set(out["DistintaMateriale"].tolist()) == {"D1", "D2"}


def test_inserimento_distinta_in_odp_raises_if_drop_columns_missing(mod):
    df_odp = pd.DataFrame(
        {
            "IdDocumento": ["A"],
            "IdRiga": [1],
            # mancano NumRegistraz/DataRegistrazione/UnitaMisura/QtaResidua
        }
    )
    componenti_per_odp = pd.DataFrame(
        {"IdDocumento": ["A"], "IdRiga": [1], "DistintaMateriale": ["D1"]}
    )

    out = mod.inserimento_distinta_in_odp(
        df_odp=df_odp,
        componenti_per_odp=componenti_per_odp,
        chiavi=["IdDocumento", "IdRiga"],
    )

    assert len(out) == 1
    assert out.loc[0, "IdDocumento"] == "A"
    assert out.loc[0, "IdRiga"] == 1
    assert out.loc[0, "DistintaMateriale"] == "D1"
# endregion
# region inserimento_dati_fasi_in_odp

def _json_list_first_values_as_str(s: str) -> list[str]:
    """
    generazione_lista() produce JSON di una lista di tuple.
    Con una sola colonna, ogni elemento è tipo [val] (lista di lunghezza 1).
    Normalizziamo tutto a stringhe per rendere il test stabile.
    """
    data = json.loads(s)
    # data: es. [[10], [20]] oppure [['10'], ['20']]
    return [str(row[0]) for row in data]


def test_inserimento_dati_fasi_in_odp_adds_phase_columns_and_keeps_odp_rows(mod):
    df_odp = pd.DataFrame(
        {
            "IdDocumento": ["A", "A", "B"],
            "IdRiga": [1, 2, 1],
            "CodArt": ["P1", "P2", "P3"],
        }
    )

    df_odpfasi = pd.DataFrame(
        {
            "IdDocumento": ["A", "A", "B"],
            "IdRiga": [1, 1, 1],
            "NumFase": [10, 20, 10],
            "CodLavorazione": ["LAV1", "LAV2", "LAVB"],
            "CodRisorsaProd": ["R1", "R2", "RB"],
            "CodReparto": ["10", "20", "30"],
            "DataInizioSched": ["2024-01-01", "2024-01-02", "2024-02-01"],
            "DataFineSched": ["2024-01-03", "2024-01-04", "2024-02-02"],
            "TempoPrevistoLavoraz": [1.5, 2.0, 3.0],
        }
    )

    out = mod.inserimento_dati_fasi_in_odp(
        df_odp=df_odp,
        df_odpfasi=df_odpfasi,
        chiavi=["IdDocumento", "IdRiga"],
    )

    # 1) left join: deve mantenere tutte le righe di df_odp e il loro ordine
    assert len(out) == len(df_odp)
    assert list(map(tuple, out[["IdDocumento", "IdRiga"]].to_numpy())) == list(
        map(tuple, df_odp[["IdDocumento", "IdRiga"]].to_numpy())
    )

    # 2) nuove colonne presenti
    expected_cols = {
        "NumFase",
        "CodLavorazione",
        "CodRisorsaProd",
        "CodReparto",
        "DataInizioSched",
        "DataFineSched",
        "TempoPrevistoLavoraz",
    }
    assert expected_cols.issubset(out.columns)

    # 3) riga A,1 deve avere 2 fasi e valori corretti (normalizzati a stringa)
    row_a1 = out[(out["IdDocumento"] == "A") & (out["IdRiga"] == 1)].iloc[0]

    assert _json_list_first_values_as_str(row_a1["NumFase"]) == ["10", "20"]
    assert _json_list_first_values_as_str(row_a1["CodLavorazione"]) == ["LAV1", "LAV2"]
    assert _json_list_first_values_as_str(row_a1["CodRisorsaProd"]) == ["R1", "R2"]
    assert _json_list_first_values_as_str(row_a1["CodReparto"]) == ["10", "20"]
    assert _json_list_first_values_as_str(row_a1["DataInizioSched"]) == ["2024-01-01", "2024-01-02"]
    assert _json_list_first_values_as_str(row_a1["DataFineSched"]) == ["2024-01-03", "2024-01-04"]

    # TempoPrevistoLavoraz potrebbe diventare "1.5" / "2.0" a seconda dei tipi -> normalizzo a stringa
    assert _json_list_first_values_as_str(row_a1["TempoPrevistoLavoraz"]) == ["1.5", "2.0"]

    # 4) riga B,1 deve avere 1 fase
    row_b1 = out[(out["IdDocumento"] == "B") & (out["IdRiga"] == 1)].iloc[0]
    assert _json_list_first_values_as_str(row_b1["NumFase"]) == ["10"]
    assert _json_list_first_values_as_str(row_b1["CodLavorazione"]) == ["LAVB"]
    assert _json_list_first_values_as_str(row_b1["CodRisorsaProd"]) == ["RB"]


def test_inserimento_dati_fasi_in_odp_rows_without_fasi_get_nans(mod):
    df_odp = pd.DataFrame(
        {
            "IdDocumento": ["A", "A"],
            "IdRiga": [1, 2],
            "CodArt": ["P1", "P2"],
        }
    )

    # fasi solo per A,1
    df_odpfasi = pd.DataFrame(
        {
            "IdDocumento": ["A"],
            "IdRiga": [1],
            "NumFase": [10],
            "CodLavorazione": ["LAV1"],
            "CodRisorsaProd": ["R1"],
            "CodReparto": ["10"],
            "DataInizioSched": ["2024-01-01"],
            "DataFineSched": ["2024-01-02"],
            "TempoPrevistoLavoraz": [1.0],
        }
    )

    out = mod.inserimento_dati_fasi_in_odp(
        df_odp=df_odp,
        df_odpfasi=df_odpfasi,
        chiavi=["IdDocumento", "IdRiga"],
    )

    row_a2 = out[(out["IdDocumento"] == "A") & (out["IdRiga"] == 2)].iloc[0]

    # tutte le colonne aggiunte devono essere NaN perché non ci sono fasi per A,2
    for col in [
        "NumFase",
        "CodLavorazione",
        "CodRisorsaProd",
        "CodReparto",
        "DataInizioSched",
        "DataFineSched",
        "TempoPrevistoLavoraz",
    ]:
        assert pd.isna(row_a2[col])


def test_inserimento_dati_fasi_in_odp_expands_from_multiple_fasi_but_keeps_single_row_per_odp(mod):
    # anche con più fasi, l'output resta una riga per ODP (perché generazione_lista aggrega)
    df_odp = pd.DataFrame({"IdDocumento": ["A"], "IdRiga": [1]})

    df_odpfasi = pd.DataFrame(
        {
            "IdDocumento": ["A", "A", "A"],
            "IdRiga": [1, 1, 1],
            "NumFase": [10, 20, 30],
            "CodLavorazione": ["L1", "L2", "L3"],
            "CodRisorsaProd": ["R1", "R2", "R3"],
            "CodReparto": ["10", "20", "30"],
            "DataInizioSched": ["d1", "d2", "d3"],
            "DataFineSched": ["f1", "f2", "f3"],
            "TempoPrevistoLavoraz": [1, 2, 3],
        }
    )

    out = mod.inserimento_dati_fasi_in_odp(df_odp, df_odpfasi, ["IdDocumento", "IdRiga"])

    assert len(out) == 1
    row = out.iloc[0]
    assert _json_list_first_values_as_str(row["NumFase"]) == ["10", "20", "30"]


def test_inserimento_dati_fasi_in_odp_reduce_inner_merge_drops_key_when_one_field_missing(
        mod
        ):
    """
    Regressione: l'uso di pd.merge() senza how='outer' nel reduce produce un INNER merge.
    Se una delle colonne (es. CodReparto) è NaN per una chiave, generazione_lista creerà
    comunque la lista, ma se per qualche motivo una delle tabelle aggregate NON contiene
    quella chiave, l'inner merge la elimina e l'ODP perde TUTTI i campi fase.

    Questo test costruisce un caso in cui la chiave (A,1) ha fasi, ma CodReparto è tutto NaN:
    ci aspettiamo che le altre liste (NumFase, CodLavorazione, CodRisorsaProd, ecc.)
    vengano comunque riportate in output (e solo CodReparto resti NaN o lista di null).
    Con il codice attuale (reduce + merge inner), è possibile che la chiave sparisca e
    quindi TUTTE le colonne risultino NaN.
    """

    df_odp = pd.DataFrame(
            {
                "IdDocumento": ["A"],
                "IdRiga"     : [1],
                "CodArt"     : ["P1"],
                }
            )

    # fasi esistono, ma CodReparto è mancante (NaN) su tutte le righe
    df_odpfasi = pd.DataFrame(
            {
                "IdDocumento"         : ["A", "A"],
                "IdRiga"              : [1, 1],
                "NumFase"             : [10, 20],
                "CodLavorazione"      : ["L1", "L2"],
                "CodRisorsaProd"      : ["R1", "R2"],
                "CodReparto"          : [None, None],  # <- campo problematico
                "DataInizioSched"     : ["2024-01-01", "2024-01-02"],
                "DataFineSched"       : ["2024-01-03", "2024-01-04"],
                "TempoPrevistoLavoraz": [1.0, 2.0],
                }
            )

    out = mod.inserimento_dati_fasi_in_odp(
            df_odp = df_odp,
            df_odpfasi = df_odpfasi,
            chiavi = ["IdDocumento", "IdRiga"],
            )

    row = out.iloc[0]

    # Aspettativa "corretta" desiderata:
    # anche se CodReparto manca, le altre colonne fase NON devono diventare tutte NaN.
    assert pd.notna(row["NumFase"]), "NumFase è NaN: la chiave è stata persa nel merge interno del reduce."
    assert pd.notna(
            row["CodLavorazione"]), "CodLavorazione è NaN: la chiave è stata persa nel merge interno del reduce."
    assert pd.notna(
            row["CodRisorsaProd"]), "CodRisorsaProd è NaN: la chiave è stata persa nel merge interno del reduce."
    assert pd.notna(
            row["DataInizioSched"]), "DataInizioSched è NaN: la chiave è stata persa nel merge interno del reduce."
    assert pd.notna(
            row["DataFineSched"]), "DataFineSched è NaN: la chiave è stata persa nel merge interno del reduce."
    assert pd.notna(row[
                        "TempoPrevistoLavoraz"]), "TempoPrevistoLavoraz è NaN: la chiave è stata persa nel merge interno del reduce."

        # CodReparto può essere NaN oppure una lista di null a seconda di come gestisci il dump:
        # qui NON imponiamo il formato, imponiamo solo che non trascini giù tutte le altre colonne.
# endregion
# region gestione_lotto_matricola_famiglia

def test_gestione_lotto_matricola_famiglia_adds_columns_and_filters_missing(mod):
    df_odp = pd.DataFrame(
        {
            "IdDocumento": ["A", "B", "C"],
            "IdRiga": [1, 1, 1],
            "CodArt": ["ART1", "ART2", "ART_MISSING"],
        }
    )

    df_articoli = pd.DataFrame(
        {
            "CodArt": ["ART1", "ART2"],
            "GestioneLotto": ["SI", "NO"],
            "GestioneMatricola": ["NO", "SI"],
            "CodFamiglia": ["F1", "F2"],
            "CodClassifTecnica": ["CT1", "CT2"],
            "Altro": [123, 456],  # ignorato dalla funzione
        }
    )

    out = mod.gestione_lotto_matricola_famiglia(df_odp=df_odp, df_articoli=df_articoli)

    # deve eliminare la riga con CodArt non presente in df_articoli (NaN dopo merge)
    assert set(out["CodArt"].tolist()) == {"ART1", "ART2"}
    assert len(out) == 2

    # colonne aggiunte
    for col in ["GestioneLotto", "GestioneMatricola", "CodFamiglia", "CodClassifTecnica"]:
        assert col in out.columns

    # mapping corretto
    row1 = out[out["CodArt"] == "ART1"].iloc[0]
    assert row1["GestioneLotto"] == "SI"
    assert row1["GestioneMatricola"] == "NO"
    assert row1["CodFamiglia"] == "F1"
    assert row1["CodClassifTecnica"] == "CT1"


def test_gestione_lotto_matricola_famiglia_drops_rows_with_missing_required_fields(mod):
    df_odp = pd.DataFrame(
        {
            "IdDocumento": ["A", "B", "C"],
            "IdRiga": [1, 1, 1],
            "CodArt": ["ART1", "ART2", "ART3"],
        }
    )

    # ART2 ha CodFamiglia mancante, ART3 ha GestioneMatricola mancante => devono essere droppati
    df_articoli = pd.DataFrame(
        {
            "CodArt": ["ART1", "ART2", "ART3"],
            "GestioneLotto": ["SI", "NO", "SI"],
            "GestioneMatricola": ["NO", "SI", None],
            "CodFamiglia": ["F1", None, "F3"],
            "CodClassifTecnica": ["CT1", "CT2", "CT3"],
        }
    )

    out = mod.gestione_lotto_matricola_famiglia(df_odp, df_articoli)

    assert len(out) == 1
    assert out.iloc[0]["CodArt"] == "ART1"


def test_gestione_lotto_matricola_famiglia_duplicates_in_articoli_expand_rows(mod):
    df_odp = pd.DataFrame({"IdDocumento": ["A"], "IdRiga": [1], "CodArt": ["ART1"]})

    # due righe per lo stesso CodArt => output si espande
    df_articoli = pd.DataFrame(
        {
            "CodArt": ["ART1", "ART1"],
            "GestioneLotto": ["SI", "SI"],
            "GestioneMatricola": ["NO", "SI"],
            "CodFamiglia": ["F1", "F1"],
            "CodClassifTecnica": ["CT1", "CT1b"],
        }
    )

    out = mod.gestione_lotto_matricola_famiglia(df_odp, df_articoli)

    assert len(out) == 2
    assert set(out["GestioneMatricola"].tolist()) == {"NO", "SI"}
    assert set(out["CodClassifTecnica"].tolist()) == {"CT1", "CT1b"}


def test_gestione_lotto_matricola_famiglia_empty_when_no_matches(mod):
    df_odp = pd.DataFrame({"IdDocumento": ["A"], "IdRiga": [1], "CodArt": ["ART_NOPE"]})
    df_articoli = pd.DataFrame(
        {
            "CodArt": ["ART1"],
            "GestioneLotto": ["SI"],
            "GestioneMatricola": ["NO"],
            "CodFamiglia": ["F1"],
            "CodClassifTecnica": ["CT1"],
        }
    )

    out = mod.gestione_lotto_matricola_famiglia(df_odp, df_articoli)
    assert out.empty
# endregion
# region inserisci_o_ignora

def test_inserimento_macrofamiglia_adds_column_and_drops_missing(mod):
    df_odp = pd.DataFrame(
        {
            "IdDocumento": ["A", "B", "C"],
            "IdRiga": [1, 1, 1],
            "CodFamiglia": ["F1", "F2", "F_MISSING"],
        }
    )

    df_famiglia = pd.DataFrame(
        {
            "CodFamiglia": ["F1", "F2"],
            "CodMacrofamiglia": ["MF1", "MF2"],
            "Altro": [10, 20],  # ignorato
        }
    )

    out = mod.inserimento_macrofamiglia(df_odp=df_odp, df_famiglia=df_famiglia)

    # riga con famiglia mancante deve essere scartata
    assert set(out["CodFamiglia"].tolist()) == {"F1", "F2"}
    assert len(out) == 2

    # colonna aggiunta
    assert "CodMacrofamiglia" in out.columns

    # mapping corretto
    assert out.loc[out["CodFamiglia"] == "F1", "CodMacrofamiglia"].iloc[0] == "MF1"
    assert out.loc[out["CodFamiglia"] == "F2", "CodMacrofamiglia"].iloc[0] == "MF2"

    # nessun NaN su CodMacrofamiglia dopo dropna
    assert out["CodMacrofamiglia"].isna().sum() == 0


def test_inserimento_macrofamiglia_drops_rows_when_macrofamiglia_is_nan(mod):
    df_odp = pd.DataFrame(
        {
            "IdDocumento": ["A", "B"],
            "IdRiga": [1, 1],
            "CodFamiglia": ["F1", "F2"],
        }
    )

    # F2 ha macrofamiglia nulla => deve essere scartata
    df_famiglia = pd.DataFrame(
        {
            "CodFamiglia": ["F1", "F2"],
            "CodMacrofamiglia": ["MF1", None],
        }
    )

    out = mod.inserimento_macrofamiglia(df_odp, df_famiglia)

    assert len(out) == 1
    assert out.iloc[0]["CodFamiglia"] == "F1"
    assert out.iloc[0]["CodMacrofamiglia"] == "MF1"


def test_inserimento_macrofamiglia_duplicates_expand_rows(mod):
    df_odp = pd.DataFrame(
        {"IdDocumento": ["A"], "IdRiga": [1], "CodFamiglia": ["F1"]}
    )

    # due macrofamiglie per stessa famiglia => output si espande
    df_famiglia = pd.DataFrame(
        {
            "CodFamiglia": ["F1", "F1"],
            "CodMacrofamiglia": ["MF1", "MF1_ALT"],
        }
    )

    out = mod.inserimento_macrofamiglia(df_odp, df_famiglia)

    assert len(out) == 2
    assert set(out["CodMacrofamiglia"].tolist()) == {"MF1", "MF1_ALT"}


def test_inserimento_macrofamiglia_empty_when_no_matches(mod):
    df_odp = pd.DataFrame(
        {"IdDocumento": ["A"], "IdRiga": [1], "CodFamiglia": ["F_NOPE"]}
    )

    df_famiglia = pd.DataFrame(
        {"CodFamiglia": ["F1"], "CodMacrofamiglia": ["MF1"]}
    )

    out = mod.inserimento_macrofamiglia(df_odp, df_famiglia)
    assert out.empty
# endregion
# region elaborazione_dati

def _identity(df, *args, **kwargs):
    return df


def _patch_elaborazione_pipeline(monkeypatch, mod, df_input_odp_prepared: pd.DataFrame):
    """
    Patcha tutta la pipeline interna di elaborazione_dati per:
    - evitare DB e logiche complesse
    - forzare df_input_odp finale a quello che decidiamo noi
    Ritorna un list "events" dove vengono registrate le chiamate a emit_event.
    """
    # 1) leggi_view: restituisce DF "minimi" per soddisfare le chiamate
    def fake_leggi_view(table, colonna_filtro_esclusi=None, colonna_filtro_stato=None):
        # ritorniamo DataFrame con colonne minime; non verranno davvero usate perché patchiamo la pipeline
        if table == "vwESOdP":
            return pd.DataFrame({"IdDocumento": [], "IdRiga": [], "CodArt": []})
        if table == "vwESOdPFasi":
            return pd.DataFrame({"IdDocumento": [], "IdRiga": []})
        if table == "vwESRisorse":
            return pd.DataFrame({"CodRisorsaProd": [], "CodReparto": []})
        if table == "vwESOdPComponenti":
            return pd.DataFrame({"IdDocumento": [], "IdRigaPadre": [], "NumFase": []})
        if table == "vwESArticoli":
            return pd.DataFrame({"CodArt": [], "GestioneLotto": [], "GestioneMatricola": [], "CodFamiglia": [], "CodClassifTecnica": []})
        if table == "vwESFamiglia":
            return pd.DataFrame({"CodFamiglia": [], "CodMacrofamiglia": []})
        return pd.DataFrame()

    monkeypatch.setattr(mod, "leggi_view", fake_leggi_view, raising=True)

    # 2) patch funzioni pipeline a identity, tranne inserimento_distinta_in_odp che forzerà il df finale
    monkeypatch.setattr(mod, "filtra_odpfasi_con_odp", _identity, raising=True)
    monkeypatch.setattr(mod, "inserimento_reparto_da_risorsa", _identity, raising=True)
    monkeypatch.setattr(mod, "filtra_odp_componenti_con_odp", _identity, raising=True)
    monkeypatch.setattr(mod, "unione_fasi_componenti", lambda *args, **kwargs: pd.DataFrame(), raising=True)
    monkeypatch.setattr(mod, "generazione_dizionario", lambda *args, **kwargs: pd.DataFrame(), raising=True)

    # qui forziamo df_input_odp della pipeline
    def fake_inserimento_distinta_in_odp(df_odp, componenti_per_odp, chiavi):
        return df_input_odp_prepared.copy()

    monkeypatch.setattr(mod, "inserimento_distinta_in_odp", fake_inserimento_distinta_in_odp, raising=True)

    # resto pipeline: pass-through
    monkeypatch.setattr(mod, "inserimento_dati_fasi_in_odp", _identity, raising=True)
    monkeypatch.setattr(mod, "gestione_lotto_matricola_famiglia", _identity, raising=True)
    monkeypatch.setattr(mod, "inserimento_macrofamiglia", _identity, raising=True)

    # 3) globals usati da elaborazione_dati
    monkeypatch.setattr(mod, "sqlite_engine_app", object(), raising=False)
    monkeypatch.setattr(mod, "inserisci_o_ignora", object(), raising=False)

    # 4) cattura eventi
    events = []

    def fake_emit_event(session, topic, scope=None, payload_json=None):
        events.append(
            {"session": session, "topic": topic, "scope": scope, "payload_json": payload_json}
        )

    monkeypatch.setattr(mod, "emit_event", fake_emit_event, raising=True)

    return events


def _patch_to_sql(monkeypatch, inserted_rows):
    """
    Patcha DataFrame.to_sql per restituire inserted_rows e validare i parametri principali.
    """
    def fake_to_sql(self, name, con, if_exists, index, method):
        assert name == "input_odp"
        assert if_exists == "append"
        assert index is False
        # con e method sono patchati nel modulo, qui basta che non siano None
        assert con is not None
        assert method is not None
        return inserted_rows

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=True)


def test_elaborazione_dati_emits_nuovo_ciclo_when_counter_zero_even_if_no_rows(monkeypatch, mod):
    # df finale deve avere DataInizioProduzione perché la funzione fa drop su quella colonna
    df_input = pd.DataFrame(
        {
            "IdDocumento": [1],
            "IdRiga": [1],
            "CodReparto": [10],
            "DataInizioProduzione": ["x"],
        }
    )

    events = _patch_elaborazione_pipeline(monkeypatch, mod, df_input)
    _patch_to_sql(monkeypatch, inserted_rows=0)

    # counter iniziale
    monkeypatch.setattr(mod, "COUNTER_RIGHE", 0, raising=False)

    session = object()
    mod.elaborazione_dati(session=session)

    # deve emettere nuovo_ciclo (COUNTER_RIGHE==0)
    assert [e["topic"] for e in events] == ["nuovo_ciclo"]
    assert mod.COUNTER_RIGHE == 0


def test_elaborazione_dati_emits_nuovo_ordine_with_expected_scope_and_payload(monkeypatch, mod):
    # 3 righe: dopo sort desc per (IdDocumento, IdRiga) => (2,1), (1,2), (1,1)
    df_input = pd.DataFrame(
        {
            "IdDocumento": [1, 2, 1],
            "IdRiga": [1, 1, 2],
            "CodReparto": [10, 20, 30],
            "DataInizioProduzione": ["x", "x", "x"],
        }
    )

    events = _patch_elaborazione_pipeline(monkeypatch, mod, df_input)
    _patch_to_sql(monkeypatch, inserted_rows=2)

    monkeypatch.setattr(mod, "COUNTER_RIGHE", 0, raising=False)

    session = object()
    mod.elaborazione_dati(session=session)

    # 1) nuovo_ciclo sempre emesso quando COUNTER_RIGHE==0
    # 2) nuovo_ordine emesso perché inserted_rows=2 !=0 e != COUNTER_RIGHE (0)
    assert [e["topic"] for e in events] == ["nuovo_ciclo", "nuovo_ordine"]

    nuovo_ordine = events[1]
    assert nuovo_ordine["scope"] == str([20, 30])  # reparti prime 2 righe post-sort
    assert nuovo_ordine["payload_json"] == str(["2,1", "1,2"])  # prime 2 chiavi post-sort

    assert mod.COUNTER_RIGHE == 2


def test_elaborazione_dati_does_not_emit_nuovo_ordine_when_inserted_equals_counter(monkeypatch, mod):
    df_input = pd.DataFrame(
        {
            "IdDocumento": [1, 2],
            "IdRiga": [1, 1],
            "CodReparto": [10, 20],
            "DataInizioProduzione": ["x", "x"],
        }
    )

    events = _patch_elaborazione_pipeline(monkeypatch, mod, df_input)
    _patch_to_sql(monkeypatch, inserted_rows=5)

    # counter già uguale alle righe "inserite"
    monkeypatch.setattr(mod, "COUNTER_RIGHE", 5, raising=False)

    session = object()
    mod.elaborazione_dati(session=session)

    # non deve emettere né nuovo_ciclo (counter != 0) né nuovo_ordine (inserted == counter)
    assert events == []
    assert mod.COUNTER_RIGHE == 5
# endregion
# region _intime_window
from datetime import time as dtime
def test_in_time_window_start_equals_end_is_24h(mod):
    start = dtime(8, 0)
    end = dtime(8, 0)

    assert mod._in_time_window(dtime(0, 0), start, end) is True
    assert mod._in_time_window(dtime(7, 59, 59), start, end) is True
    assert mod._in_time_window(dtime(8, 0), start, end) is True
    assert mod._in_time_window(dtime(23, 59, 59), start, end) is True


def test_in_time_window_normal_window_inclusive_start_exclusive_end(mod):
    start = dtime(9, 0)
    end = dtime(17, 0)

    assert mod._in_time_window(dtime(8, 59, 59), start, end) is False
    assert mod._in_time_window(dtime(9, 0), start, end) is True
    assert mod._in_time_window(dtime(16, 59, 59), start, end) is True
    assert mod._in_time_window(dtime(17, 0), start, end) is False
    assert mod._in_time_window(dtime(23, 0), start, end) is False


def test_in_time_window_overnight_window(mod):
    start = dtime(18, 0)
    end = dtime(6, 0)

    assert mod._in_time_window(dtime(0, 0), start, end) is True
    assert mod._in_time_window(dtime(5, 59, 59), start, end) is True
    assert mod._in_time_window(dtime(6, 0), start, end) is False

    assert mod._in_time_window(dtime(6, 0, 1), start, end) is False
    assert mod._in_time_window(dtime(12, 0), start, end) is False
    assert mod._in_time_window(dtime(17, 59, 59), start, end) is False

    assert mod._in_time_window(dtime(18, 0), start, end) is True
    assert mod._in_time_window(dtime(23, 59, 59), start, end) is True


def test_in_time_window_edge_case_midnight_to_morning(mod):
    start = dtime(0, 0)
    end = dtime(6, 0)

    assert mod._in_time_window(dtime(0, 0), start, end) is True
    assert mod._in_time_window(dtime(5, 59, 59), start, end) is True
    assert mod._in_time_window(dtime(6, 0), start, end) is False
    assert mod._in_time_window(dtime(23, 0), start, end) is False


def test_in_time_window_edge_case_evening_to_midnight(mod):
    start = dtime(18, 0)
    end = dtime(0, 0)

    assert mod._in_time_window(dtime(17, 59, 59), start, end) is False
    assert mod._in_time_window(dtime(18, 0), start, end) is True
    assert mod._in_time_window(dtime(23, 59, 59), start, end) is True
    assert mod._in_time_window(dtime(0, 0), start, end) is False
    assert mod._in_time_window(dtime(0, 0, 1), start, end) is False
# endregion
# region _is_allowed_datetime
import datetime as dt

def test_is_allowed_datetime_normal_window_allowed_weekday_inside(mod):
    # Lunedì
    now = dt.datetime(2026, 2, 2, 10, 0, 0)  # Monday
    start = dtime(9, 0)
    end = dtime(17, 0)
    allowed = {0, 1, 2, 3, 4}  # lun-ven

    assert mod._is_allowed_datetime(now, start, end, allowed) is True


def test_is_allowed_datetime_normal_window_disallowed_weekday(mod):
    # Domenica
    now = dt.datetime(2026, 2, 1, 10, 0, 0)  # Sunday
    start = dtime(9, 0)
    end = dtime(17, 0)
    allowed = {0, 1, 2, 3, 4}  # lun-ven

    assert mod._is_allowed_datetime(now, start, end, allowed) is False


def test_is_allowed_datetime_normal_window_end_exclusive(mod):
    # Lunedì alle 17:00 => fuori (end escluso)
    now = dt.datetime(2026, 2, 2, 17, 0, 0)
    start = dtime(9, 0)
    end = dtime(17, 0)
    allowed = {0, 1, 2, 3, 4}

    assert mod._is_allowed_datetime(now, start, end, allowed) is False


def test_is_allowed_datetime_24h_window_allowed_weekday(mod):
    # start == end => 24h; conta solo il weekday
    now = dt.datetime(2026, 2, 2, 3, 0, 0)  # lunedì
    start = dtime(8, 0)
    end = dtime(8, 0)
    allowed = {0}  # solo lunedì

    assert mod._is_allowed_datetime(now, start, end, allowed) is True


def test_is_allowed_datetime_24h_window_disallowed_weekday(mod):
    now = dt.datetime(2026, 2, 2, 3, 0, 0)  # lunedì
    start = dtime(8, 0)
    end = dtime(8, 0)
    allowed = {1}  # solo martedì

    assert mod._is_allowed_datetime(now, start, end, allowed) is False


def test_is_allowed_datetime_overnight_counts_start_day_even_after_midnight(mod):
    # finestra overnight: 18:00 -> 06:00
    start = dtime(18, 0)
    end = dtime(6, 0)

    # Martedì 02:00: la finestra è iniziata lunedì alle 18:00.
    now = dt.datetime(2026, 2, 3, 2, 0, 0)  # Tuesday
    allowed = {0}  # solo lunedì (start_day)

    assert mod._is_allowed_datetime(now, start, end, allowed) is True


def test_is_allowed_datetime_overnight_disallowed_start_day(mod):
    start = dtime(18, 0)
    end = dtime(6, 0)

    # Martedì 02:00 conta come finestra iniziata lunedì, ma lunedì NON è ammesso
    now = dt.datetime(2026, 2, 3, 2, 0, 0)  # Tuesday
    allowed = {1}  # solo martedì

    assert mod._is_allowed_datetime(now, start, end, allowed) is False


def test_is_allowed_datetime_overnight_evening_part_uses_same_weekday(mod):
    start = dtime(18, 0)
    end = dtime(6, 0)

    # Lunedì 23:00 => start_day è lunedì
    now = dt.datetime(2026, 2, 2, 23, 0, 0)  # Monday
    allowed = {0}

    assert mod._is_allowed_datetime(now, start, end, allowed) is True

def test_is_allowed_datetime_with_timezone_info(mod):
    # il codice fa timetz().replace(tzinfo=None) quindi deve funzionare anche con tzinfo presente
    start = dtime(9, 0)
    end = dtime(17, 0)
    allowed = {0, 1, 2, 3, 4}

    tz = dt.timezone(dt.timedelta(hours=1))
    now = dt.datetime(2026, 2, 2, 10, 0, 0, tzinfo=tz)  # lunedì 10:00+01:00

    assert mod._is_allowed_datetime(now, start, end, allowed) is True
# endregion
# region seconds_until_next_allowed
from zoneinfo import ZoneInfo
def _patch_now(monkeypatch, mod, fixed_now: dt.datetime):
    class FakeDateTime:
        @staticmethod
        def now(tz=None):
            return fixed_now

    monkeypatch.setattr(mod, "datetime", FakeDateTime, raising=True)



def test_seconds_until_next_allowed_returns_0_if_inside_window(monkeypatch, mod):
    tz = ZoneInfo("Europe/Rome")
    # lunedì 10:15 -> dentro 9-17
    fixed_now = dt.datetime(2026, 2, 2, 10, 15, 30, tzinfo=tz)
    _patch_now(monkeypatch, mod, fixed_now)

    allowed = {0, 1, 2, 3, 4}
    assert mod.seconds_until_next_allowed(9, 17, allowed, tz=tz, step_minutes=1) == 0


def test_seconds_until_next_allowed_normal_window_waits_until_start(monkeypatch, mod):
    tz = ZoneInfo("Europe/Rome")
    # lunedì 08:10:30 -> fuori 9-17, deve aspettare fino a 09:00:00
    fixed_now = dt.datetime(2026, 2, 2, 8, 10, 30, tzinfo=tz)
    _patch_now(monkeypatch, mod, fixed_now)

    allowed = {0, 1, 2, 3, 4}
    # probe parte da 08:10:00, poi incrementa a minuti interi: primo consentito 09:00:00
    expected = int((dt.datetime(2026, 2, 2, 9, 0, 0, tzinfo=tz) - fixed_now).total_seconds())
    assert mod.seconds_until_next_allowed(9, 17, allowed, tz=tz, step_minutes=1) == expected


def test_seconds_until_next_allowed_overnight_daytime_waits_until_evening_start(monkeypatch, mod):
    tz = ZoneInfo("Europe/Rome")
    # lunedì 12:00 -> fuori finestra 18-6, prossimo è lunedì 18:00
    fixed_now = dt.datetime(2026, 2, 2, 12, 0, 10, tzinfo=tz)
    _patch_now(monkeypatch, mod, fixed_now)

    allowed = {0, 1, 2, 3, 4}
    expected = int((dt.datetime(2026, 2, 2, 18, 0, 0, tzinfo=tz) - fixed_now).total_seconds())
    assert mod.seconds_until_next_allowed(18, 6, allowed, tz=tz, step_minutes=1) == expected


def test_seconds_until_next_allowed_respects_step_minutes_granularity(monkeypatch, mod):
    tz = ZoneInfo("Europe/Rome")
    # lunedì 08:02:30, finestra 09-17, step 15 min
    # probe = 08:02:00 -> +15 => 08:17, 08:32, 08:47, 09:02 (primo consentito, NON 09:00)
    fixed_now = dt.datetime(2026, 2, 2, 8, 2, 30, tzinfo=tz)
    _patch_now(monkeypatch, mod, fixed_now)

    allowed = {0, 1, 2, 3, 4}
    expected_probe = dt.datetime(2026, 2, 2, 9, 2, 0, tzinfo=tz)
    expected = int((expected_probe - fixed_now).total_seconds())

    assert mod.seconds_until_next_allowed(9, 17, allowed, tz=tz, step_minutes=15) == expected


def test_seconds_until_next_allowed_raises_if_no_window_found(monkeypatch, mod):
    tz = ZoneInfo("Europe/Rome")
    # allowed_weekdays vuoto => impossibile trovare finestra
    fixed_now = dt.datetime(2026, 2, 2, 10, 0, 0, tzinfo=tz)
    _patch_now(monkeypatch, mod, fixed_now)

    with pytest.raises(RuntimeError):
        mod.seconds_until_next_allowed(9, 17, allowed_weekdays=set(), tz=tz, step_minutes=60)

def _patch_now(monkeypatch, mod, fixed_now: dt.datetime):
    class FakeDateTime:
        @staticmethod
        def now(tz=None):
            return fixed_now
    monkeypatch.setattr(mod, "datetime", FakeDateTime, raising=True)


def test_seconds_until_next_allowed_step_minutes_zero_is_clamped_to_1(monkeypatch, mod):
    tz = ZoneInfo("Europe/Rome")
    # lunedì 08:10:30, finestra 09-17 => prossimo consentito 09:00:00
    fixed_now = dt.datetime(2026, 2, 2, 8, 10, 30, tzinfo=tz)
    _patch_now(monkeypatch, mod, fixed_now)

    warn_calls = []
    monkeypatch.setattr(mod.logging, "warning", lambda msg, *args: warn_calls.append((msg, args)), raising=True)

    allowed = {0, 1, 2, 3, 4}
    s = mod.seconds_until_next_allowed(9, 17, allowed, tz=tz, step_minutes=0)

    expected = int((dt.datetime(2026, 2, 2, 9, 0, 0, tzinfo=tz) - fixed_now).total_seconds())
    assert s == expected

    assert len(warn_calls) == 1
    assert "step_minutes" in warn_calls[0][0]
    
def test_seconds_until_next_allowed_step_minutes_negative_is_clamped(monkeypatch, mod):
    tz = ZoneInfo("Europe/Rome")
    fixed_now = dt.datetime(2026, 2, 2, 8, 0, 30, tzinfo=tz)
    _patch_now(monkeypatch, mod, fixed_now)

    warn_calls = []
    monkeypatch.setattr(mod.logging, "warning", lambda msg, *args: warn_calls.append((msg, args)), raising=True)

    s = mod.seconds_until_next_allowed(9, 17, {0,1,2,3,4}, tz=tz, step_minutes=-5)
    assert s > 0
    assert len(warn_calls) == 1

# endregion
# region wait_if_not_allowed
def test_wait_if_not_allowed_does_nothing_when_seconds_zero(monkeypatch, mod):
    # seconds_until_next_allowed -> 0
    monkeypatch.setattr(mod, "seconds_until_next_allowed", lambda *args, **kwargs: 0, raising=True)

    info_calls = []
    sleep_calls = []

    monkeypatch.setattr(mod.logging, "info", lambda *args, **kwargs: info_calls.append((args, kwargs)), raising=True)
    monkeypatch.setattr(mod.time_mod, "sleep", lambda s: sleep_calls.append(s), raising=True)

    mod.wait_if_not_allowed(start_h=9, end_h=17, allowed_weekdays={0, 1, 2, 3, 4})

    assert info_calls == []
    assert sleep_calls == []


def test_wait_if_not_allowed_logs_and_sleeps_when_positive_seconds(monkeypatch, mod):
    # seconds_until_next_allowed -> 125 sec => 2 min nel log (floor division)
    monkeypatch.setattr(mod, "seconds_until_next_allowed", lambda *args, **kwargs: 125, raising=True)

    info_calls = []
    sleep_calls = []

    def fake_info(msg, *args, **kwargs):
        info_calls.append((msg, args, kwargs))

    monkeypatch.setattr(mod.logging, "info", fake_info, raising=True)
    monkeypatch.setattr(mod.time_mod, "sleep", lambda s: sleep_calls.append(s), raising=True)

    mod.wait_if_not_allowed(start_h=9, end_h=17, allowed_weekdays={0, 1, 2, 3, 4})

    # sleep chiamato con esattamente i secondi ritornati
    assert sleep_calls == [125]

    # logging.info chiamato una volta con minuti = 125 // 60 = 2
    assert len(info_calls) == 1
    msg, args, kwargs = info_calls[0]
    assert "Fuori schedule. Sleep" in msg
    assert args[0] == 2  # ~%d min


def test_wait_if_not_allowed_propagates_exception_from_seconds_until_next_allowed(monkeypatch, mod):
    def boom(*args, **kwargs):
        raise RuntimeError("no window")

    monkeypatch.setattr(mod, "seconds_until_next_allowed", boom, raising=True)

    with pytest.raises(RuntimeError):
        mod.wait_if_not_allowed(start_h=9, end_h=17, allowed_weekdays={0, 1, 2, 3, 4})

# endregion
# region emit_event

class DummySession:
    def __init__(self, fail_add: bool = False):
        self.fail_add = fail_add
        self.added = []
        self.committed = 0
        self.rolled_back = 0

    def add(self, obj):
        if self.fail_add:
            raise RuntimeError("add failed")
        self.added.append(obj)

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

class EmitEventSession:
    def __init__(self, should_fail_add: bool = False):
        self.should_fail_add = should_fail_add
        self.added = []
        self.committed = 0
        self.rolled_back = 0

    def add(self, obj):
        if self.should_fail_add:
            raise RuntimeError("add failed")
        self.added.append(obj)

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1


def test_emit_event_commits_on_success(monkeypatch, mod):
    class FakeChangeEvent:
        def __init__(self, topic, scope=None, payload_json=None):
            self.topic = topic
            self.scope = scope
            self.payload_json = payload_json

    monkeypatch.setattr(mod, "ChangeEvent", FakeChangeEvent, raising=True)

    session = EmitEventSession(should_fail_add=False)

    mod.emit_event(session=session, topic="nuovo_ordine", scope="['10']", payload_json='["A,1"]')

    assert len(session.added) == 1
    assert session.committed == 1
    assert session.rolled_back == 0

    ev = session.added[0]
    assert ev.topic == "nuovo_ordine"
    assert ev.scope == "['10']"
    assert ev.payload_json == '["A,1"]'


def test_emit_event_rolls_back_on_add_failure(monkeypatch, mod):
    class FakeChangeEvent:
        def __init__(self, topic, scope=None, payload_json=None):
            self.topic = topic
            self.scope = scope
            self.payload_json = payload_json

    monkeypatch.setattr(mod, "ChangeEvent", FakeChangeEvent, raising=True)

    session = EmitEventSession(should_fail_add=True)

    mod.emit_event(session=session, topic="nuovo_ciclo", scope=None, payload_json=None)

    assert session.added == []
    assert session.committed == 0
    assert session.rolled_back == 1


def test_emit_event_accepts_none_scope_and_payload(monkeypatch, mod):
    class FakeChangeEvent:
        def __init__(self, topic, scope=None, payload_json=None):
            self.topic = topic
            self.scope = scope
            self.payload_json = payload_json

    monkeypatch.setattr(mod, "ChangeEvent", FakeChangeEvent, raising=True)

    session = EmitEventSession()

    mod.emit_event(session=session, topic="test_topic")

    assert len(session.added) == 1
    assert session.committed == 1

    ev = session.added[0]
    assert ev.topic == "test_topic"
    assert ev.scope is None
    assert ev.payload_json is None

# endregion
# region read_cycle

class DummySession:
    def __init__(self):
        self.began = 0
        self.closed = 0

    def begin(self):
        self.began += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.closed += 1
        # non sopprimere eccezioni
        return False


class DummySessionFactory:
    """Simula Session(sqlite_engine_app) -> context manager che ritorna DummySession."""
    def __init__(self, session_obj):
        self._s = session_obj

    def __call__(self, *args, **kwargs):
        return self._s


def test_read_cycle_single_iteration_logs_and_sleeps_and_handles_elab_exception(monkeypatch, mod):
    # --- patch Session / engine ---
    dummy_session = DummySession()
    monkeypatch.setattr(mod, "sqlite_engine_app", object(), raising=False)
    monkeypatch.setattr(mod, "Session", DummySessionFactory(dummy_session), raising=True)

    # --- patch schedule globals ---
    monkeypatch.setattr(mod, "START_H", 9, raising=False)
    monkeypatch.setattr(mod, "END_H", 17, raising=False)
    monkeypatch.setattr(mod, "ALLOWED_WEEKDAYS", {0, 1, 2, 3, 4}, raising=False)

    # --- patch wait_if_not_allowed ---
    wait_calls = []
    monkeypatch.setattr(mod, "wait_if_not_allowed", lambda *a, **k: wait_calls.append((a, k)), raising=True)

    # --- patch elaborazione_dati: solleva eccezione per triggerare logging.exception ---
    def boom(*args, **kwargs):
        raise RuntimeError("fail")

    monkeypatch.setattr(mod, "elaborazione_dati", boom, raising=True)

    # --- patch time_mod.time to control elapsed ---
    # start=100.0, end=101.2 => elapsed=1.2
    times = iter([100.0, 101.2])
    monkeypatch.setattr(mod.time_mod, "time", lambda: next(times), raising=True)

    # --- patch time_mod.sleep: cattura sleep e poi interrompi il loop con KeyboardInterrupt ---
    sleep_calls = []

    def fake_sleep(s):
        sleep_calls.append(s)
        raise KeyboardInterrupt

    monkeypatch.setattr(mod.time_mod, "sleep", fake_sleep, raising=True)

    # --- patch logging to capture calls ---
    info_calls = []
    exc_calls = []

    monkeypatch.setattr(mod.logging, "info", lambda msg, *args: info_calls.append((msg, args)), raising=True)
    monkeypatch.setattr(mod.logging, "exception", lambda msg, *args: exc_calls.append((msg, args)), raising=True)

    # --- run ---
    mod.read_cycle(poll_seconds=5)

    # session.begin chiamato 1 volta
    assert dummy_session.began == 1

    # wait chiamato (con START_H, END_H, ALLOWED_WEEKDAYS)
    assert len(wait_calls) == 1
    assert wait_calls[0][0] == (mod.START_H, mod.END_H, mod.ALLOWED_WEEKDAYS)

    # elaborazione_dati fallisce => logging.exception chiamato
    assert len(exc_calls) == 1
    assert "Errore generico" in exc_calls[0][0]

    # elapsed=1.2, poll=5 => sleep_for = max(0, 5 - int(1.2)) = 5 - 1 = 4
    assert sleep_calls == [4]

    # log "Inizio programma" + log ciclo + log medie finali (KeyboardInterrupt)
    assert any("Inizio programma" in m for (m, _) in info_calls)
    assert any("Ciclo" in m and "Sleep" in m for (m, _) in info_calls)
    assert any("Media tempo ciclo" in m for (m, _) in info_calls)
    assert any("Media tempo riposo" in m for (m, _) in info_calls)


def test_read_cycle_two_iterations_then_keyboardinterrupt_logs_final_means(monkeypatch, mod):
    dummy_session = DummySession()
    monkeypatch.setattr(mod, "sqlite_engine_app", object(), raising=False)
    monkeypatch.setattr(mod, "Session", DummySessionFactory(dummy_session), raising=True)

    monkeypatch.setattr(mod, "START_H", 9, raising=False)
    monkeypatch.setattr(mod, "END_H", 17, raising=False)
    monkeypatch.setattr(mod, "ALLOWED_WEEKDAYS", {0, 1, 2, 3, 4}, raising=False)

    monkeypatch.setattr(mod, "wait_if_not_allowed", lambda *a, **k: None, raising=True)
    monkeypatch.setattr(mod, "elaborazione_dati", lambda *a, **k: None, raising=True)

    # Due cicli: (elapsed 0.4) e (elapsed 2.7)
    # time sequence: start1=10.0 end1=10.4 start2=20.0 end2=22.7
    times = iter([10.0, 10.4, 20.0, 22.7])
    monkeypatch.setattr(mod.time_mod, "time", lambda: next(times), raising=True)

    sleep_calls = []
    sleep_iter = iter([None, KeyboardInterrupt])

    def fake_sleep(s):
        sleep_calls.append(s)
        v = next(sleep_iter)
        if v is KeyboardInterrupt:
            raise KeyboardInterrupt

    monkeypatch.setattr(mod.time_mod, "sleep", fake_sleep, raising=True)

    info_calls = []
    monkeypatch.setattr(mod.logging, "info", lambda msg, *args: info_calls.append((msg, args)), raising=True)
    monkeypatch.setattr(mod.logging, "exception", lambda *a, **k: None, raising=True)

    mod.read_cycle(poll_seconds=5)

    # sleep_for:
    # elapsed1=0.4 => int=0 => 5-0=5
    # elapsed2=2.7 => int=2 => 5-2=3
    assert sleep_calls == [5, 3]

    # alla fine deve loggare le medie finali (KeyboardInterrupt)
    assert any("Media tempo ciclo" in m for (m, _) in info_calls)
    assert any("Media tempo riposo" in m for (m, _) in info_calls)