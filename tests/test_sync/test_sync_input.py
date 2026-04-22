import importlib
import json
import sqlite3 as sq
import sys
from datetime import datetime, time
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
import sqlalchemy as sa

MODULE_PATH = "sync.sync_input"


@pytest.fixture()
def mod():
    sys.modules.pop(MODULE_PATH, None)
    return importlib.import_module(MODULE_PATH)


@pytest.fixture()
def mod_reset(mod):
    mod.CONFIG = None
    mod.config = None
    mod.sqlite_engine_app = None
    mod.sqlserver_engine_app = None
    mod.sqlite_engine_log = None
    mod.ALLOWED_WEEKDAYS = None
    mod.START_H = None
    mod.END_H = None
    mod.TIMEZONE = None
    mod.POLL_SECONDS_DEFAULT = None
    mod.ELEMENTI_ESCLUSI = None
    mod.ELEMENTI_SELEZIONATI = None
    mod._INITIALIZED = False
    mod.nuovo_ciclo = 0
    return mod


# =========================
# CONFIG E INIZIALIZZAZIONE
# =========================


def test_load_config_reads_toml(tmp_path, mod):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[Percorsi]
percorso_db = "db.sqlite"

[sync_config]
giorni_settimanali = [0, 1, 2]
ora_inizio = 8
ora_fine = 17
time_zone = "Europe/Rome"
tempo_polling = 5
sql_params = "Driver=ODBC Driver 18"

[Elementi_esclusi]
CodArt = ["X"]

[Elementi_selezionati]
StatoOrdine = "APERTO"
""".strip(),
        encoding="utf-8",
    )

    cfg = mod.load_config(config_path)

    assert cfg["Percorsi"]["percorso_db"] == "db.sqlite"
    assert cfg["sync_config"]["ora_inizio"] == 8
    assert cfg["Elementi_esclusi"]["CodArt"] == ["X"]


def test_init_populates_globals_and_ensure_init_calls_init(monkeypatch, mod_reset):
    cfg = {
        "Percorsi": {
            "percorso_db": "test.sqlite",
            "percorso_db_log": "log.sqlite",
        },
        "sync_config": {
            "giorni_settimanali": [0, 1, 2, 3, 4],
            "ora_inizio": 8,
            "ora_fine": 17,
            "time_zone": "Europe/Rome",
            "tempo_polling": 7,
            "sql_params": "Driver=ODBC Driver 18;Server=localhost",
        },
        "Elementi_esclusi": {"CodArt": ["A"], "CodRisorsaProd": ["R9"]},
        "Elementi_selezionati": {"StatoOrdine": "A"},
    }
    created = []
    fk_enabled = []

    def fake_create_engine(url):
        created.append(url)
        return SimpleNamespace(url=url)

    monkeypatch.setattr(mod_reset, "load_config", lambda path: cfg)
    monkeypatch.setattr(mod_reset, "create_engine", fake_create_engine)
    monkeypatch.setattr(
        mod_reset,
        "_enable_sqlite_foreign_keys",
        lambda engine: fk_enabled.append(engine),
    )

    mod_reset.init("dummy.toml")

    assert mod_reset._INITIALIZED is True
    assert mod_reset.ALLOWED_WEEKDAYS == {0, 1, 2, 3, 4}
    assert mod_reset.START_H == 8
    assert mod_reset.END_H == 17
    assert mod_reset.TIMEZONE == "Europe/Rome"
    assert mod_reset.POLL_SECONDS_DEFAULT == 7.0
    assert mod_reset.ELEMENTI_ESCLUSI == {"CodArt": ["A"], "CodRisorsaProd": ["R9"]}
    assert mod_reset.ELEMENTI_SELEZIONATI == {"StatoOrdine": "A"}

    assert mod_reset.sqlite_engine_app.url == "sqlite:///test.sqlite"
    assert mod_reset.sqlserver_engine_app.url.startswith(
        "mssql+pyodbc:///?odbc_connect="
    )
    assert mod_reset.sqlite_engine_log.url == "sqlite:///log.sqlite"

    assert [engine.url for engine in fk_enabled] == [
        "sqlite:///test.sqlite",
        "sqlite:///log.sqlite",
    ]
    assert created[1].startswith("mssql+pyodbc:///?odbc_connect=")

    calls = []
    mod_reset._INITIALIZED = False
    monkeypatch.setattr(mod_reset, "init", lambda *a, **k: calls.append((a, k)))
    mod_reset.ensure_init()

    assert len(calls) == 1


def test_init_force_reinitializes_and_reuses_existing_sqlserver(monkeypatch, mod_reset):
    cfg = {
        "Percorsi": {
            "percorso_db": "forced.sqlite",
            "percorso_db_log": "forced_log.sqlite",
        },
        "sync_config": {
            "giorni_settimanali": [0, 1, 2, 3, 4],
            "ora_inizio": 6,
            "ora_fine": 14,
            "time_zone": "Europe/Rome",
            "tempo_polling": 3,
            "sql_params": "Driver=ODBC Driver 18;Server=localhost",
        },
        "Elementi_esclusi": {"CodArt": [], "CodRisorsaProd": []},
        "Elementi_selezionati": {"StatoOrdine": "APERTO"},
    }
    created = []
    fk_enabled = []

    def fake_create_engine(url):
        created.append(url)
        return SimpleNamespace(url=url)

    mod_reset._INITIALIZED = True
    mod_reset.sqlite_engine_app = "OLD_SQLITE_APP"
    mod_reset.sqlserver_engine_app = "OLD_SQLSERVER"
    mod_reset.sqlite_engine_log = "OLD_SQLITE_LOG"

    monkeypatch.setattr(mod_reset, "load_config", lambda path: cfg)
    monkeypatch.setattr(mod_reset, "create_engine", fake_create_engine)
    monkeypatch.setattr(
        mod_reset,
        "_enable_sqlite_foreign_keys",
        lambda engine: fk_enabled.append(engine),
    )

    mod_reset.init("dummy.toml", force=True)

    assert mod_reset._INITIALIZED is True
    assert mod_reset.sqlite_engine_app.url == "sqlite:///forced.sqlite"
    assert mod_reset.sqlserver_engine_app.url.startswith(
        "mssql+pyodbc:///?odbc_connect="
    )
    assert mod_reset.sqlite_engine_log.url == "sqlite:///forced_log.sqlite"

    assert [engine.url for engine in fk_enabled] == [
        "sqlite:///forced.sqlite",
        "sqlite:///forced_log.sqlite",
    ]


# ==================
# LETTURA E FILTRAGGI
# ==================


def test_leggi_view_applies_exclusion_and_state_filters(monkeypatch, mod):
    mod._INITIALIZED = True
    mod.sqlserver_engine_app = object()
    mod.ELEMENTI_ESCLUSI = {"CodArt": ["ESCL"]}
    mod.ELEMENTI_SELEZIONATI = {"StatoOrdine": "APERTO"}

    df_source = pd.DataFrame(
        {
            "CodArt": ["OK1", "ESCL", None, "OK2"],
            "StatoOrdine": ["APERTO", "APERTO", "APERTO", "CHIUSO"],
        }
    )
    monkeypatch.setattr(mod.pd, "read_sql", lambda query, engine: df_source)

    result = mod.leggi_view(
        table="vwESOdP",
        colonna_filtro_esclusi="CodArt",
        colonna_filtro_stato="StatoOrdine",
    )

    assert result.to_dict("records") == [{"CodArt": "OK1", "StatoOrdine": "APERTO"}]


def test_leggi_view_without_filters_returns_reset_index(monkeypatch, mod):
    mod._INITIALIZED = True
    mod.sqlserver_engine_app = object()
    mod.ELEMENTI_ESCLUSI = {}
    mod.ELEMENTI_SELEZIONATI = {}

    df_source = pd.DataFrame({"A": [10, 20]}, index=[7, 9])
    monkeypatch.setattr(mod.pd, "read_sql", lambda query, engine: df_source)

    result = mod.leggi_view(table="vwESOdP")

    assert result.index.tolist() == [0, 1]
    assert result.to_dict("records") == [{"A": 10}, {"A": 20}]


def test_filtra_odpfasi_con_odp_keeps_only_matching_rows(mod):
    df_odpfasi = pd.DataFrame(
        {
            "IdDocumento": [1, 2],
            "IdRiga": [10, 20],
            "NumFase": [1, 2],
        }
    )
    df_odp = pd.DataFrame({"IdDocumento": [1], "IdRiga": [10]})

    result = mod.filtra_odpfasi_con_odp(df_odpfasi, df_odp)

    assert result[["IdDocumento", "IdRiga", "NumFase"]].to_dict("records") == [
        {"IdDocumento": 1, "IdRiga": 10, "NumFase": 1}
    ]


def test_filtra_odp_componenti_con_odp_removes_helper_columns(mod):
    df_comp = pd.DataFrame(
        {
            "IdDocumento": [1, 2],
            "IdRigaPadre": [10, 30],
            "IdRiga": [100, 300],
            "CodArt": ["C1", "C2"],
        }
    )
    df_odp = pd.DataFrame({"IdDocumento": [1], "IdRiga": [10]})

    result = mod.filtra_odp_componenti_con_odp(df_comp, df_odp)

    assert "IdRiga_y" not in result.columns
    assert result[["IdDocumento", "IdRigaPadre", "IdRiga", "CodArt"]].to_dict(
        "records"
    ) == [{"IdDocumento": 1, "IdRigaPadre": 10, "IdRiga": 100, "CodArt": "C1"}]


def test_inserimento_reparto_da_risorsa_drops_rows_without_reparto(mod):
    df_fasi = pd.DataFrame({"CodRisorsaProd": ["R1", "R2"], "NumFase": [1, 2]})
    df_risorse = pd.DataFrame({"CodRisorsaProd": ["R1"], "CodReparto": ["REP-A"]})

    result = mod.inserimento_reparto_da_risorsa(df_fasi, df_risorse)

    assert result.to_dict("records") == [
        {"CodRisorsaProd": "R1", "NumFase": 1, "CodReparto": "REP-A"}
    ]


# ======================
# TRASFORMAZIONE DATASET
# ======================


def test_unione_fasi_componenti_adds_component_description(mod):
    df_fasi = pd.DataFrame(
        {
            "IdDocumento": [1],
            "IdRiga": [10],
            "NumFase": [1],
            "CodRisorsaProd": ["R1"],
        }
    )
    df_componenti = pd.DataFrame(
        {
            "IdDocumento": [1],
            "IdRigaPadre": [10],
            "IdRiga": [99],
            "NumFase": [1],
            "CodArt": ["AA00-111-2222"],
            "Quantita": [3],
            "VarianteArt": ["V1"],
        }
    )
    df_articoli = pd.DataFrame(
        {
            "CodArt": ["AA00-111-2222"],
            "DesArt": ["Componente"],
            "TecniciUm": ["PZ"],
            "GestioneLotto": ["S"],
        }
    )

    result = mod.unione_fasi_componenti(df_fasi, df_componenti, df_articoli)

    row = result.iloc[0].to_dict()
    assert row["IdRigacomponente"] == 99
    assert row["DesArt"] == "Componente"
    assert row["TecniciUm"] == "PZ"
    assert row["GestioneLotto"] == "S"
    assert row["VarianteArt"] == "V1"


def test_generazione_lista_supports_single_and_multi_column(mod):
    df = pd.DataFrame(
        {
            "IdDocumento": [1, 1, 1],
            "IdRiga": [10, 10, 10],
            "NumFase": [1, 1, 2],
            "CodArt": ["A", "A", "B"],
        }
    )

    single = mod.generazione_lista(
        df, ["IdDocumento", "IdRiga"], "NumFase", ["NumFase"]
    )
    multi = mod.generazione_lista(
        df,
        ["IdDocumento", "IdRiga"],
        "Coppie",
        ["NumFase", "CodArt"],
        dumps_json=False,
    )

    assert json.loads(single.loc[0, "NumFase"]) == [1, 2]
    assert multi.loc[0, "Coppie"] == [(1, "A"), (1, "A"), (2, "B")]


def test_sanitize_helpers_convert_missing_values_to_none(mod):
    assert mod._sanitize_json_scalar(pd.NA) is None
    assert mod._sanitize_json_scalar(float("nan")) is None
    assert mod._sanitize_json_scalar("ok") == "ok"

    records = [{"a": pd.NA, "b": 1}, {"a": float("nan"), "b": "x"}]
    assert mod._sanitize_records_for_json(records) == [
        {"a": None, "b": 1},
        {"a": None, "b": "x"},
    ]


def test_generazione_dizionario_filters_fake_distinta_rows_and_sanitizes_nan(mod):
    df = pd.DataFrame(
        {
            "IdDocumento": [1, 1],
            "IdRiga": [10, 10],
            "CodArt": ["AA00-111-2222", None],
            "DesArt": ["Comp", "Da scartare"],
            "Quantita": [2, 3],
            "NumFase": [1, 1],
            "TecniciUm": ["PZ", "PZ"],
            "GestioneLotto": [pd.NA, pd.NA],
            "VarianteArt": ["V1", None],
        }
    )

    result = mod.generazione_dizionario(
        df=df,
        chiavi=["IdDocumento", "IdRiga"],
        rename_col="DistintaMateriale",
        list_columns=[
            "CodArt",
            "DesArt",
            "Quantita",
            "NumFase",
            "TecniciUm",
            "GestioneLotto",
            "VarianteArt",
        ],
    )

    payload = json.loads(result.loc[0, "DistintaMateriale"])
    assert payload == [
        {
            "CodArt": "AA00-111-2222",
            "DesArt": "Comp",
            "Quantita": 2,
            "NumFase": 1,
            "TecniciUm": "PZ",
            "GestioneLotto": None,
            "VarianteArt": "V1",
        }
    ]


def test_generazione_dizionario_formats_dates_when_requested(mod):
    df = pd.DataFrame(
        {
            "IdDocumento": [1],
            "IdRiga": [10],
            "DataInizioSched": pd.to_datetime(["2024-01-02 13:45:00"]),
        }
    )

    result = mod.generazione_dizionario(
        df=df,
        chiavi=["IdDocumento", "IdRiga"],
        rename_col="DataInizioSched",
        list_columns=["DataInizioSched"],
        data_in="data",
    )

    payload = json.loads(result.loc[0, "DataInizioSched"])
    assert isinstance(payload, list)
    assert payload == [{"DataInizioSched": "02/01/2024 13:45:00"}]
    assert payload[0]["DataInizioSched"] == "02/01/2024 13:45:00"


def test_inserimento_distinta_in_odp_drops_unused_columns(mod):
    df_odp = pd.DataFrame(
        {
            "IdDocumento": [1],
            "IdRiga": [10],
            "NumRegistraz": [1],
            "DataRegistrazione": ["2024-01-01"],
            "UnitaMisura": ["PZ"],
            "QtaResidua": [5],
        }
    )
    distinte = pd.DataFrame(
        {
            "IdDocumento": [1],
            "IdRiga": [10],
            "DistintaMateriale": ["[]"],
        }
    )

    result = mod.inserimento_distinta_in_odp(
        df_odp, distinte, ["IdDocumento", "IdRiga"]
    )

    assert list(result.columns) == ["IdDocumento", "IdRiga", "DistintaMateriale"]


def test_inserimento_dati_fasi_in_odp_groups_phase_columns(mod):
    df_odp = pd.DataFrame({"IdDocumento": [1], "IdRiga": [10], "CodArt": ["FG"]})
    df_fasi = pd.DataFrame(
        {
            "IdDocumento": [1, 1],
            "IdRiga": [10, 10],
            "NumFase": [1, 2],
            "CodLavorazione": ["L1", "L2"],
            "CodRisorsaProd": ["R1", "R2"],
            "CodReparto": ["REP", "REP"],
            "DataInizioSched": ["2024-01-01", "2024-01-02"],
            "DataFineSched": ["2024-01-03", "2024-01-04"],
            "TempoPrevistoLavoraz": [10, 20],
            "TempoAttrezzaggio": [5, 6],
        }
    )

    result = mod.inserimento_dati_fasi_in_odp(
        df_odp, df_fasi, ["IdDocumento", "IdRiga"]
    )

    assert json.loads(result.loc[0, "NumFase"]) == [1, 2]
    assert json.loads(result.loc[0, "CodLavorazione"]) == ["L1", "L2"]
    assert json.loads(result.loc[0, "TempoPrevistoLavoraz"]) == [10, 20]
    assert json.loads(result.loc[0, "TempoAttrezzaggio"]) == [5, 6]


def test_gestione_lotto_matricola_famiglia_and_macrofamiglia(mod):
    df_odp = pd.DataFrame({"CodArt": ["FG", "MISS"]})
    df_articoli = pd.DataFrame(
        {
            "CodArt": ["FG"],
            "GestioneLotto": ["S"],
            "GestioneMatricola": ["N"],
            "CodFamiglia": ["F1"],
            "CodClassifTecnica": ["CT1"],
            "DesArt": ["Finito"],
            "IndiceModifica": ["A"],
        }
    )
    df_famiglia = pd.DataFrame({"CodFamiglia": ["F1"], "CodMacrofamiglia": ["MF1"]})

    enriched = mod.gestione_lotto_matricola_famiglia(df_odp, df_articoli)
    final = mod.inserimento_macrofamiglia(enriched, df_famiglia)

    assert final.to_dict("records") == [
        {
            "CodArt": "FG",
            "GestioneLotto": "S",
            "GestioneMatricola": "N",
            "CodFamiglia": "F1",
            "CodClassifTecnica": "CT1",
            "DesArt": "Finito",
            "IndiceModifica": "A",
            "CodMacrofamiglia": "MF1",
        }
    ]


# ======================
# DATABASE E RUNTIME DATA
# ======================


def test_inserisci_o_ignora_inserts_only_new_rows(mod):
    engine = sa.create_engine("sqlite:///:memory:")
    meta = sa.MetaData()
    table = sa.Table(
        "input_odp",
        meta,
        sa.Column("IdDocumento", sa.Integer, primary_key=True),
        sa.Column("IdRiga", sa.Integer, primary_key=True),
        sa.Column("CodArt", sa.String),
    )
    meta.create_all(engine)

    class SqlTable:
        def __init__(self, table):
            self.table = table

    with engine.begin() as conn:
        inserted = mod.inserisci_o_ignora(
            SqlTable(table),
            conn,
            ["IdDocumento", "IdRiga", "CodArt"],
            [(1, 10, "A"), (1, 10, "A"), (1, 20, "B")],
        )
        rows = conn.execute(sa.text("SELECT COUNT(*) FROM input_odp")).scalar_one()

    assert inserted == 2
    assert rows == 2


def test_inserisci_o_ignora_returns_len_for_non_sqlalchemy_connection(mod):
    meta = sa.MetaData()
    table = sa.Table(
        "input_odp",
        meta,
        sa.Column("IdDocumento", sa.Integer, primary_key=True),
        sa.Column("IdRiga", sa.Integer, primary_key=True),
        sa.Column("CodArt", sa.String),
    )

    class SqlTable:
        def __init__(self, table):
            self.table = table

    class FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, stmt):
            self.calls.append(stmt)
            return object()

    conn = FakeConn()
    inserted = mod.inserisci_o_ignora(
        SqlTable(table),
        conn,
        ["IdDocumento", "IdRiga", "CodArt"],
        [(1, 10, "A"), (1, 20, "B")],
    )

    assert inserted == 2
    assert len(conn.calls) == 1


def test_fetch_existing_pks_and_update_rows_by_pk(mod):
    engine = sa.create_engine("sqlite:///:memory:")
    df = pd.DataFrame(
        [
            {"IdDocumento": "1", "IdRiga": "10", "CodArt": "A", "DesArt": "Old"},
            {"IdDocumento": "2", "IdRiga": "20", "CodArt": "B", "DesArt": "Old2"},
        ]
    )
    df.to_sql("input_odp", engine, index=False, if_exists="replace")

    existing = mod._fetch_existing_pks(engine, [("1", "10"), ("9", "90")])
    assert existing == {("1", "10")}

    df_update = pd.DataFrame(
        [{"IdDocumento": "1", "IdRiga": "10", "CodArt": "AX", "DesArt": "New"}]
    )
    updated = mod._update_rows_by_pk(
        engine,
        df_update,
        table_name="input_odp",
        update_cols=["CodArt", "DesArt"],
    )

    after = pd.read_sql(
        "SELECT * FROM input_odp WHERE IdDocumento = '1' AND IdRiga = '10'", engine
    )
    assert updated == 1
    assert after.iloc[0]["CodArt"] == "AX"
    assert after.iloc[0]["DesArt"] == "New"


def test_fetch_existing_pks_empty_and_update_guards(mod):
    assert mod._fetch_existing_pks(engine=None, pk_tuples=[]) == set()

    empty_df = pd.DataFrame(columns=["IdDocumento", "IdRiga", "CodArt"])
    assert (
        mod._update_rows_by_pk(
            engine=None,
            df=empty_df,
            table_name="input_odp",
            update_cols=["CodArt"],
        )
        == 0
    )
    assert (
        mod._update_rows_by_pk(
            engine=None,
            df=pd.DataFrame([{"IdDocumento": "1", "IdRiga": "10"}]),
            table_name="input_odp",
            update_cols=[],
        )
        == 0
    )


def test_build_runtime_seed_and_helpers(mod):
    df_input = pd.DataFrame(
        {
            "IdDocumento": [1],
            "IdRiga": [10],
            "RifRegistraz": ["RIF1"],
            "Quantita": [7],
            "CodLavorazione": ['["L1", "L2"]'],
            "CodRisorsaProd": ['["R1", "R2"]'],
            "TempoAttrezzaggio": ["[15, 20]"],
            "VarianteArt": ["V1"],
        }
    )

    result = mod._build_runtime_seed(df_input)

    assert result.to_dict("records") == [
        {
            "IdDocumento": 1,
            "IdRiga": 10,
            "RifRegistraz": "RIF1",
            "Stato_odp": "Pianificata",
            "Utente_operazione": "sync_input",
            "FaseAttiva": "1",
            "Note": None,
            "QtyDaLavorare": 7,
            "RisorsaAttiva": "R1",
            "LavorazioneAttiva": "L1",
            "AttrezzaggioAttivo": 15,
            "VarianteArt": "V1",
        }
    ]

    assert list(mod._chunked([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]
    assert mod.int_format("8") == 8
    assert mod.int_format("abc") == 0
    assert mod.estrai_lavorazione_attiva('["J1", "J2"]') == "J1"
    assert mod.estrai_lavorazione_attiva("[15, 20]") == 15
    assert mod.estrai_lavorazione_attiva("[]") is None
    assert mod.estrai_lavorazione_attiva(3) is None
    assert mod.estrai_lavorazione_attiva(None) is None


def test_filtri_giacenza_lotti_keeps_positive_valid_rows(mod):
    df = pd.DataFrame(
        {
            "Giacenza": [5, 0, "abc", 2],
            "RifLottoAlfa": ["12345678", "12345678", "87654321", "ABCDE"],
            "CodArt": [
                "BE00-037-0000",
                "BE00-037-0000",
                "BE00-037-0000",
                "BE00-037-0000",
            ],
        }
    )

    result = mod.filtri_giacenza_lotti(df)

    assert result.to_dict("records") == [
        {
            "Giacenza": 5,
            "RifLottoAlfa": "12345678",
            "CodArt": "BE00-037-0000",
        }
    ]


def test_elaborazione_dati_inserts_new_erp_runtime_lots_and_writes_logs(
    monkeypatch, mod_reset, tmp_path
):
    mod_reset._INITIALIZED = True
    mod_reset.sqlite_engine_app = _prepare_sync_sqlite_db(mod_reset, tmp_path)
    mod_reset.sqlite_engine_log = object()

    views = _make_base_views(lot_rows=True)
    support_calls = []
    log_calls = []

    monkeypatch.setattr(
        mod_reset,
        "_sync_rbac_support_tables",
        lambda: support_calls.append(True) or {},
    )
    monkeypatch.setattr(
        mod_reset,
        "leggi_view",
        lambda table, colonna_filtro_esclusi="", colonna_filtro_stato="": views[
            table
        ].copy(),
    )
    monkeypatch.setattr(mod_reset, "_fetch_blocked_outbox_pks", lambda engine: set())
    monkeypatch.setattr(
        mod_reset,
        "_write_sync_logs",
        lambda **kwargs: log_calls.append(kwargs),
    )

    mod_reset.elaborazione_dati(session=object())

    count_odp = pd.read_sql(
        "SELECT COUNT(*) AS n FROM input_odp", mod_reset.sqlite_engine_app
    )
    count_runtime = pd.read_sql(
        "SELECT COUNT(*) AS n FROM input_odp_runtime", mod_reset.sqlite_engine_app
    )
    count_lotti = pd.read_sql(
        "SELECT COUNT(*) AS n FROM giacenza_lotti", mod_reset.sqlite_engine_app
    )

    runtime_row = (
        pd.read_sql(
            "SELECT * FROM input_odp_runtime",
            mod_reset.sqlite_engine_app,
        )
        .iloc[0]
        .to_dict()
    )

    assert support_calls == [True]
    assert int(count_odp.iloc[0]["n"]) == 1
    assert int(count_runtime.iloc[0]["n"]) == 1
    assert int(count_lotti.iloc[0]["n"]) == 1

    assert runtime_row["IdDocumento"] == "1"
    assert runtime_row["IdRiga"] == "10"
    assert runtime_row["RifRegistraz"] == "RIF1"
    assert runtime_row["Stato_odp"] == "Pianificata"
    assert runtime_row["FaseAttiva"] == "1"
    assert runtime_row["RisorsaAttiva"] == "RIS1"
    assert runtime_row["LavorazioneAttiva"] == "LAV1"
    assert int(runtime_row["AttrezzaggioAttivo"]) == 15
    assert runtime_row["VarianteArt"] == "VFG"

    assert len(log_calls) == 1
    assert len(log_calls[0]["df_new_erp"]) == 1
    assert len(log_calls[0]["df_new_runtime"]) == 1


def test_elaborazione_dati_updates_existing_erp_inserts_missing_runtime_without_logs(
    monkeypatch, mod_reset, tmp_path
):
    mod_reset._INITIALIZED = True
    mod_reset.sqlite_engine_app = _prepare_sync_sqlite_db(mod_reset, tmp_path)
    mod_reset.sqlite_engine_log = object()

    existing_odp = pd.DataFrame(
        [
            _blank_row(
                mod_reset.INPUT_ODP_ERP_COLS,
                IdDocumento="1",
                IdRiga="10",
                RifRegistraz="OLD",
                CodArt="OLD_ART",
            )
        ]
    )
    existing_odp.to_sql(
        "input_odp",
        mod_reset.sqlite_engine_app,
        index=False,
        if_exists="append",
    )

    views = _make_base_views(lot_rows=True)
    update_calls = []
    log_calls = []

    real_update = mod_reset._update_rows_by_pk

    def spy_update(*args, **kwargs):
        update_calls.append(kwargs)
        return real_update(*args, **kwargs)

    monkeypatch.setattr(mod_reset, "_sync_rbac_support_tables", lambda: {})
    monkeypatch.setattr(
        mod_reset,
        "leggi_view",
        lambda table, colonna_filtro_esclusi="", colonna_filtro_stato="": views[
            table
        ].copy(),
    )
    monkeypatch.setattr(mod_reset, "_fetch_blocked_outbox_pks", lambda engine: set())
    monkeypatch.setattr(
        mod_reset, "_write_sync_logs", lambda **kwargs: log_calls.append(kwargs)
    )
    monkeypatch.setattr(mod_reset, "_update_rows_by_pk", spy_update)

    mod_reset.elaborazione_dati(session=object())

    count_odp = pd.read_sql(
        "SELECT COUNT(*) AS n FROM input_odp", mod_reset.sqlite_engine_app
    )
    count_runtime = pd.read_sql(
        "SELECT COUNT(*) AS n FROM input_odp_runtime", mod_reset.sqlite_engine_app
    )

    odp_row = (
        pd.read_sql(
            "SELECT * FROM input_odp",
            mod_reset.sqlite_engine_app,
        )
        .iloc[0]
        .to_dict()
    )

    runtime_row = (
        pd.read_sql(
            "SELECT * FROM input_odp_runtime",
            mod_reset.sqlite_engine_app,
        )
        .iloc[0]
        .to_dict()
    )

    assert int(count_odp.iloc[0]["n"]) == 1
    assert int(count_runtime.iloc[0]["n"]) == 1

    assert any(call["table_name"] == "input_odp" for call in update_calls)
    assert log_calls == []

    assert odp_row["RifRegistraz"] == "RIF1"
    assert odp_row["CodArt"] == "FG"
    assert runtime_row["RifRegistraz"] == "RIF1"
    assert runtime_row["Stato_odp"] == "Pianificata"


def test_elaborazione_dati_skips_blocked_outbox_pks_but_still_syncs_lots(
    monkeypatch, mod_reset, tmp_path
):
    mod_reset._INITIALIZED = True
    mod_reset.sqlite_engine_app = _prepare_sync_sqlite_db(mod_reset, tmp_path)
    mod_reset.sqlite_engine_log = object()

    views = _make_base_views(lot_rows=True)
    log_calls = []

    monkeypatch.setattr(mod_reset, "_sync_rbac_support_tables", lambda: {})
    monkeypatch.setattr(
        mod_reset,
        "leggi_view",
        lambda table, colonna_filtro_esclusi="", colonna_filtro_stato="": views[
            table
        ].copy(),
    )
    monkeypatch.setattr(
        mod_reset,
        "_fetch_blocked_outbox_pks",
        lambda engine: {("1", "10")},
    )
    monkeypatch.setattr(
        mod_reset, "_write_sync_logs", lambda **kwargs: log_calls.append(kwargs)
    )

    mod_reset.elaborazione_dati(session=object())

    count_odp = pd.read_sql(
        "SELECT COUNT(*) AS n FROM input_odp", mod_reset.sqlite_engine_app
    )
    count_runtime = pd.read_sql(
        "SELECT COUNT(*) AS n FROM input_odp_runtime", mod_reset.sqlite_engine_app
    )
    count_lotti = pd.read_sql(
        "SELECT COUNT(*) AS n FROM giacenza_lotti", mod_reset.sqlite_engine_app
    )

    assert int(count_odp.iloc[0]["n"]) == 0
    assert int(count_runtime.iloc[0]["n"]) == 0
    assert int(count_lotti.iloc[0]["n"]) == 1
    assert log_calls == []


# ==================
# SCHEDULAZIONE/EVENTI
# ==================


def test_time_window_and_schedule_helpers(monkeypatch, mod):
    from datetime import datetime, time
    from zoneinfo import ZoneInfo

    assert mod._in_time_window(time(9, 0), time(8, 0), time(17, 0)) is True
    assert mod._in_time_window(time(2, 0), time(22, 0), time(6, 0)) is True
    assert mod._in_time_window(time(12, 0), time(8, 0), time(8, 0)) is True

    morning = datetime(2026, 3, 18, 9, 0, tzinfo=ZoneInfo("Europe/Rome"))
    night = datetime(2026, 3, 18, 2, 0, tzinfo=ZoneInfo("Europe/Rome"))
    assert (
        mod._is_allowed_datetime(morning, time(8, 0), time(17, 0), {0, 1, 2, 3, 4})
        is True
    )
    assert mod._is_allowed_datetime(night, time(22, 0), time(6, 0), {1}) is True

    class FakeDateTime:
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 3, 18, 7, 58, tzinfo=tz or ZoneInfo("Europe/Rome"))

    monkeypatch.setattr(mod, "datetime", FakeDateTime)
    seconds = mod.seconds_until_next_allowed(8, 17, {0, 1, 2, 3, 4}, step_minutes=1)
    assert 0 < seconds <= 120


def test_seconds_until_next_allowed_raises_when_no_window_found(monkeypatch, mod):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    class FakeDateTime:
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 3, 18, 7, 58, tzinfo=tz or ZoneInfo("Europe/Rome"))

    monkeypatch.setattr(mod, "datetime", FakeDateTime)
    monkeypatch.setattr(mod, "_is_allowed_datetime", lambda *a, **k: False)

    with pytest.raises(RuntimeError):
        mod.seconds_until_next_allowed(8, 17, {0, 1, 2, 3, 4}, step_minutes=1440)


def test_wait_if_not_allowed_sleeps_only_when_needed(monkeypatch, mod):
    slept = []
    monkeypatch.setattr(mod, "seconds_until_next_allowed", lambda *a, **k: 90)
    monkeypatch.setattr(mod.time_mod, "sleep", lambda s: slept.append(s))

    mod.wait_if_not_allowed(8, 17, {0, 1, 2, 3, 4})
    assert slept == [90]

    slept.clear()
    monkeypatch.setattr(mod, "seconds_until_next_allowed", lambda *a, **k: 0)
    mod.wait_if_not_allowed(8, 17, {0, 1, 2, 3, 4})
    assert slept == []


def test_read_cycle_runs_two_iterations(monkeypatch, mod):
    mod._INITIALIZED = True
    mod.sqlite_engine_app = object()
    mod.START_H = 8
    mod.END_H = 17
    mod.ALLOWED_WEEKDAYS = {0, 1, 2, 3, 4}
    mod.POLL_SECONDS_DEFAULT = 5
    events = []

    class FakeSessionCtx:
        def __enter__(self):
            return "SESSION"

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(mod, "Session", lambda engine: FakeSessionCtx())
    monkeypatch.setattr(
        mod, "wait_if_not_allowed", lambda *a, **k: events.append("wait")
    )
    monkeypatch.setattr(
        mod, "elaborazione_dati", lambda session: events.append(("run", session))
    )

    sleep_calls = []
    times = iter([0.0, 1.0, 10.0, 12.0])

    def fake_sleep(seconds):
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 2:
            raise KeyboardInterrupt()

    monkeypatch.setattr(mod.time_mod, "time", lambda: next(times))
    monkeypatch.setattr(mod.time_mod, "sleep", fake_sleep)

    mod.read_cycle()

    assert events == ["wait", ("run", "SESSION"), "wait", ("run", "SESSION")]
    assert sleep_calls == [4.0, 3.0]


def test_read_cycle_continues_when_elaborazione_raises(monkeypatch, mod):
    mod._INITIALIZED = True
    mod.sqlite_engine_app = object()
    mod.START_H = 8
    mod.END_H = 17
    mod.ALLOWED_WEEKDAYS = {0, 1, 2, 3, 4}
    mod.POLL_SECONDS_DEFAULT = 2

    class FakeSessionCtx:
        def __enter__(self):
            return "SESSION"

        def __exit__(self, exc_type, exc, tb):
            return False

    calls = []

    def fake_elaborazione(session):
        calls.append(session)
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "Session", lambda engine: FakeSessionCtx())
    monkeypatch.setattr(mod, "wait_if_not_allowed", lambda *a, **k: None)
    monkeypatch.setattr(mod, "elaborazione_dati", fake_elaborazione)

    sleep_calls = []
    times = iter([0.0, 0.5, 10.0, 10.5])

    def fake_sleep(seconds):
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 2:
            raise KeyboardInterrupt()

    monkeypatch.setattr(mod.time_mod, "time", lambda: next(times))
    monkeypatch.setattr(mod.time_mod, "sleep", fake_sleep)

    mod.read_cycle()

    assert calls == ["SESSION", "SESSION"]
    assert sleep_calls == [1.5, 1.5]


# -----------------------------------------------------------------------------
# Helper condivisi
# -----------------------------------------------------------------------------


def _make_sync_views(*, lot_rows: bool = True) -> dict[str, pd.DataFrame]:
    lots = pd.DataFrame(
        {
            "CodArt": ["BE00-037-0000"],
            "RifLottoAlfa": ["12345678"],
            "CodMag": ["MAG1"],
            "Giacenza": [4],
        }
    )
    if not lot_rows:
        lots = pd.DataFrame(
            {
                "CodArt": [],
                "RifLottoAlfa": [],
                "CodMag": [],
                "Giacenza": [],
            }
        )

    return {
        "vwESOdP": pd.DataFrame(
            {
                "IdDocumento": ["1"],
                "IdRiga": ["10"],
                "RifRegistraz": ["RIF1"],
                "NumProgrRiga": [1],
                "CodArt": ["FG"],
                "DesArt": ["Prodotto finito"],
                "Quantita": [5],
                "NumRegistraz": [1],
                "DataRegistrazione": ["2024-01-01"],
                "UnitaMisura": ["PZ"],
                "QtaResidua": [5],
                "CodMatricola": [None],
                "StatoRiga": ["A"],
                "CodMagPrincipale": ["MAG1"],
                "StatoOrdine": ["APERTO"],
                "CodTipoDoc": ["ODP"],
                "DataInizioProduzione": ["2024-01-10"],
                "VarianteArt": ["VFG"],
            }
        ),
        "vwESOdPFasi": pd.DataFrame(
            {
                "IdDocumento": ["1"],
                "IdRiga": ["10"],
                "NumFase": [1],
                "CodLavorazione": ["LAV1"],
                "CodRisorsaProd": ["RIS1"],
                "DataInizioSched": ["2024-01-01 08:00:00"],
                "DataFineSched": ["2024-01-01 09:00:00"],
                "TempoPrevistoLavoraz": [60],
                "TempoAttrezzaggio": [15],
            }
        ),
        "vwESRisorse": pd.DataFrame(
            {
                "CodRisorsaProd": ["RIS1"],
                "CodReparto": ["REP1"],
                "DesRisorsaProd": ["Risorsa 1"],
            }
        ),
        "vwESOdPComponenti": pd.DataFrame(
            {
                "IdDocumento": ["1"],
                "IdRigaPadre": ["10"],
                "IdRiga": ["99"],
                "NumFase": [1],
                "CodArt": ["BE00-037-0000"],
                "Quantita": [2],
                "VarianteArt": ["VC1"],
            }
        ),
        "vwESArticoli": pd.DataFrame(
            {
                "CodArt": ["FG", "BE00-037-0000"],
                "GestioneLotto": ["S", "N"],
                "GestioneMatricola": ["N", "N"],
                "CodFamiglia": ["F1", "F2"],
                "CodClassifTecnica": ["CT1", "CT2"],
                "DesArt": ["Prodotto finito", "Componente"],
                "TecniciUm": ["PZ", "PZ"],
                "IndiceModifica": ["A", "B"],
            }
        ),
        "vwESFamiglia": pd.DataFrame(
            {
                "CodFamiglia": ["F1"],
                "CodMacrofamiglia": ["MF1"],
                "Des": ["Famiglia 1"],
            }
        ),
        "vwESCausaliAttivita": pd.DataFrame(
            {
                "CausaleAttivita": ["CA1"],
                "DesCausaleAttivita": ["Causale 1"],
                "CodCategoriaAttivita": ["CAT1"],
                "TipoCausale": ["STD"],
            }
        ),
        "vwESLavorazioni": pd.DataFrame(
            {
                "CodLavorazione": ["LAV1"],
                "DesLavorazione": ["Lavorazione 1"],
            }
        ),
        "vwESMacroFamiglia": pd.DataFrame(
            {
                "CodMacrofamiglia": ["MF1"],
                "Des": ["Macrofamiglia 1"],
            }
        ),
        "vwESMagazzini": pd.DataFrame(
            {
                "CodMag": ["MAG1"],
                "DesMagazzino": ["Magazzino 1"],
            }
        ),
        "vwESReparti": pd.DataFrame(
            {
                "CodReparto": ["REP1"],
                "Des": ["Reparto 1"],
            }
        ),
        "vwESGiacenzaLotti": lots,
    }


def _make_base_views(*, lot_rows: bool = True) -> dict[str, pd.DataFrame]:
    return _make_sync_views(lot_rows=lot_rows)


def _prepare_sync_sqlite_db(mod, tmp_path):
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'sync_input_test.sqlite'}")

    pd.DataFrame(columns=mod.INPUT_ODP_ERP_COLS).to_sql(
        "input_odp",
        engine,
        index=False,
        if_exists="replace",
    )
    pd.DataFrame(columns=mod.INPUT_ODP_RUNTIME_COLS).to_sql(
        "input_odp_runtime",
        engine,
        index=False,
        if_exists="replace",
    )
    pd.DataFrame(columns=[*mod.LOTTI_PK_COLS, "Giacenza"]).to_sql(
        "giacenza_lotti",
        engine,
        index=False,
        if_exists="replace",
    )

    return engine


def _blank_row(columns, **values):
    row = {col: None for col in columns}
    row.update(values)
    return row


# -----------------------------------------------------------------------------
# Test aggiuntivi
# -----------------------------------------------------------------------------


def test_estrai_lavorazione_attiva_raises_on_malformed_json(mod):
    with pytest.raises(json.JSONDecodeError):
        mod.estrai_lavorazione_attiva("[")


def test_build_runtime_seed_handles_null_phase_values(mod):
    df_input = pd.DataFrame(
        {
            "IdDocumento": [1],
            "IdRiga": [10],
            "RifRegistraz": ["RIF1"],
            "Quantita": [7],
            "CodLavorazione": [None],
            "CodRisorsaProd": [None],
            "TempoAttrezzaggio": [None],
            "VarianteArt": [None],
        }
    )

    result = mod._build_runtime_seed(df_input)

    assert result.to_dict("records") == [
        {
            "IdDocumento": 1,
            "IdRiga": 10,
            "RifRegistraz": "RIF1",
            "Stato_odp": "Pianificata",
            "Utente_operazione": "sync_input",
            "FaseAttiva": "1",
            "Note": None,
            "QtyDaLavorare": 7,
            "RisorsaAttiva": None,
            "LavorazioneAttiva": None,
            "AttrezzaggioAttivo": None,
            "VarianteArt": None,
        }
    ]


def test_build_runtime_seed_raises_on_non_json_phase_strings(mod):
    df_input = pd.DataFrame(
        {
            "IdDocumento": [1],
            "IdRiga": [10],
            "RifRegistraz": ["RIF1"],
            "Quantita": [7],
            "CodLavorazione": ["LAV1"],
            "CodRisorsaProd": ['["RIS1"]'],
            "TempoAttrezzaggio": ["[10]"],
            "VarianteArt": ["V1"],
        }
    )

    with pytest.raises(json.JSONDecodeError):
        mod._build_runtime_seed(df_input)


def test_seconds_until_next_allowed_normalizes_non_positive_step(monkeypatch, mod):
    class FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2024, 1, 2, 7, 59, 0, tzinfo=tz)

    warnings = []

    monkeypatch.setattr(mod, "datetime", FakeDateTime)
    monkeypatch.setattr(
        mod.logging, "warning", lambda msg, *args: warnings.append((msg, args))
    )

    seconds = mod.seconds_until_next_allowed(
        start_h=8,
        end_h=17,
        allowed_weekdays={1},
        tz=ZoneInfo("Europe/Rome"),
        step_minutes=0,
    )

    assert seconds == 60
    assert warnings
    assert "step_minutes=%s non valido" in warnings[0][0]


def test_in_time_window_respects_exact_boundaries(mod):
    assert mod._in_time_window(time(8, 0), time(8, 0), time(17, 0)) is True
    assert mod._in_time_window(time(16, 59, 59), time(8, 0), time(17, 0)) is True
    assert mod._in_time_window(time(17, 0), time(8, 0), time(17, 0)) is False

    assert mod._in_time_window(time(22, 0), time(22, 0), time(6, 0)) is True
    assert mod._in_time_window(time(5, 59, 59), time(22, 0), time(6, 0)) is True
    assert mod._in_time_window(time(6, 0), time(22, 0), time(6, 0)) is False


def test_read_cycle_keyboard_interrupt_logs_mean_elapsed(monkeypatch, mod):
    mod._INITIALIZED = True
    mod.sqlite_engine_app = object()
    mod.START_H = 8
    mod.END_H = 17
    mod.ALLOWED_WEEKDAYS = {0, 1, 2, 3, 4}
    mod.POLL_SECONDS_DEFAULT = 0.2

    class FakeSession:
        def __init__(self, engine):
            self.engine = engine

        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    times = iter([10.0, 10.1])
    info_calls = []

    monkeypatch.setattr(mod, "Session", FakeSession)
    monkeypatch.setattr(mod, "wait_if_not_allowed", lambda *a, **k: None)
    monkeypatch.setattr(mod, "elaborazione_dati", lambda session: None)
    monkeypatch.setattr(mod.time_mod, "time", lambda: next(times))
    monkeypatch.setattr(
        mod.time_mod, "sleep", lambda s: (_ for _ in ()).throw(KeyboardInterrupt())
    )
    monkeypatch.setattr(
        mod.logging, "info", lambda msg, *args: info_calls.append((msg, args))
    )

    mod.read_cycle()

    assert any(msg == "Media tempo ciclo %.5f" for msg, _ in info_calls)


def test_generazione_lista_single_column_drops_nan_and_preserves_first_seen_order(mod):
    df = pd.DataFrame(
        {
            "IdDocumento": [1, 1, 1, 1],
            "IdRiga": [10, 10, 10, 10],
            "CodRisorsaProd": ["RIS-B", None, "RIS-A", "RIS-B"],
        }
    )

    result = mod.generazione_lista(
        df=df,
        chiavi=["IdDocumento", "IdRiga"],
        rename_col="CodRisorsaProd",
        list_columns=["CodRisorsaProd"],
    )

    assert json.loads(result.loc[0, "CodRisorsaProd"]) == ["RIS-B", "RIS-A"]


def test_inserimento_dati_fasi_in_odp_leaves_nan_for_orders_without_phases(mod):
    df_odp = pd.DataFrame(
        {
            "IdDocumento": [1, 2],
            "IdRiga": [10, 20],
            "CodArt": ["ART1", "ART2"],
        }
    )
    df_odpfasi = pd.DataFrame(
        {
            "IdDocumento": [1],
            "IdRiga": [10],
            "NumFase": [1],
            "CodLavorazione": ["LAV1"],
            "CodRisorsaProd": ["RIS1"],
            "CodReparto": ["REP1"],
            "DataInizioSched": ["2024-01-01 08:00:00"],
            "DataFineSched": ["2024-01-01 09:00:00"],
            "TempoPrevistoLavoraz": [60],
            "TempoAttrezzaggio": [15],
        }
    )

    result = mod.inserimento_dati_fasi_in_odp(
        df_odp=df_odp,
        df_odpfasi=df_odpfasi,
        chiavi=["IdDocumento", "IdRiga"],
    )

    row_with_phase = result.loc[result["IdDocumento"] == 1].iloc[0]
    row_without_phase = result.loc[result["IdDocumento"] == 2].iloc[0]

    assert json.loads(row_with_phase["NumFase"]) == [1]
    assert json.loads(row_with_phase["CodLavorazione"]) == ["LAV1"]
    assert json.loads(row_with_phase["TempoAttrezzaggio"]) == [15]
    assert pd.isna(row_without_phase["NumFase"])
    assert pd.isna(row_without_phase["CodLavorazione"])
    assert pd.isna(row_without_phase["CodReparto"])
    assert pd.isna(row_without_phase["TempoAttrezzaggio"])


def test_elaborazione_dati_with_empty_lots_inserts_erp_runtime_but_not_giacenza(
    monkeypatch, mod_reset, tmp_path
):
    mod_reset._INITIALIZED = True
    mod_reset.sqlite_engine_app = _prepare_sync_sqlite_db(mod_reset, tmp_path)
    mod_reset.sqlite_engine_log = object()

    views = _make_base_views(lot_rows=False)
    support_calls = []
    log_calls = []

    monkeypatch.setattr(
        mod_reset,
        "_sync_rbac_support_tables",
        lambda: support_calls.append(True) or {},
    )
    monkeypatch.setattr(
        mod_reset,
        "leggi_view",
        lambda table, colonna_filtro_esclusi="", colonna_filtro_stato="": views[
            table
        ].copy(),
    )
    monkeypatch.setattr(mod_reset, "_fetch_blocked_outbox_pks", lambda engine: set())
    monkeypatch.setattr(
        mod_reset,
        "_write_sync_logs",
        lambda **kwargs: log_calls.append(kwargs),
    )

    mod_reset.elaborazione_dati(session=object())

    count_odp = pd.read_sql(
        "SELECT COUNT(*) AS n FROM input_odp",
        mod_reset.sqlite_engine_app,
    )
    count_runtime = pd.read_sql(
        "SELECT COUNT(*) AS n FROM input_odp_runtime",
        mod_reset.sqlite_engine_app,
    )
    count_lotti = pd.read_sql(
        "SELECT COUNT(*) AS n FROM giacenza_lotti",
        mod_reset.sqlite_engine_app,
    )

    runtime_row = (
        pd.read_sql(
            "SELECT * FROM input_odp_runtime",
            mod_reset.sqlite_engine_app,
        )
        .iloc[0]
        .to_dict()
    )

    assert support_calls == [True]
    assert int(count_odp.iloc[0]["n"]) == 1
    assert int(count_runtime.iloc[0]["n"]) == 1
    assert int(count_lotti.iloc[0]["n"]) == 0

    assert runtime_row["IdDocumento"] == "1"
    assert runtime_row["IdRiga"] == "10"
    assert runtime_row["RifRegistraz"] == "RIF1"
    assert runtime_row["Stato_odp"] == "Pianificata"
    assert int(runtime_row["QtyDaLavorare"]) == 5

    assert len(log_calls) == 1
    assert len(log_calls[0]["df_new_erp"]) == 1
    assert len(log_calls[0]["df_new_runtime"]) == 1
