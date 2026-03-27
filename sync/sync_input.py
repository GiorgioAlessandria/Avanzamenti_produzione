# region LIBRERIE
"""
Programma per l'acquisizione dei dati dalle view di produzione verso il db
"""

import logging
import statistics
from typing import Optional, Literal
import json
import re
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import sqlite3 as sq
import tomllib
from pathlib import Path
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import functools as ft
from zoneinfo import ZoneInfo
from datetime import datetime, time, timedelta
import time as time_mod
import urllib.parse
import pathlib
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection as SAConnection

try:
    from icecream import ic
except:
    pass
# endregion
# region COSTANTI
CONFIG = None
config = None
sqlite_engine_app = None
sqlserver_engine_app = None
sqlite_engine_log = None
ALLOWED_WEEKDAYS = None
START_H = None
END_H = None
TIMEZONE = None
POLL_SECONDS_DEFAULT = None
ELEMENTI_ESCLUSI = None
ELEMENTI_SELEZIONATI = None
_INITIALIZED = False
# endregion
# region DB E CONFIG
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

nuovo_ciclo = 0


def init(config_path: str | pathlib.Path = None, *, force: bool = False):
    global CONFIG, config, sqlite_engine_app, sqlserver_engine_app, sqlite_engine_log
    global ALLOWED_WEEKDAYS, START_H, END_H, TIMEZONE, POLL_SECONDS_DEFAULT
    global ELEMENTI_ESCLUSI, ELEMENTI_SELEZIONATI
    global _INITIALIZED

    if _INITIALIZED and not force:
        return

    if config_path is None:
        config_path = Path("app_odp//static//config.toml")

    CONFIG = load_config(config_path)

    percorsi = CONFIG["Percorsi"]
    sync_cfg = CONFIG["sync_config"]
    ELEMENTI_ESCLUSI = CONFIG["Elementi_esclusi"]
    ELEMENTI_SELEZIONATI = CONFIG["Elementi_selezionati"]
    ALLOWED_WEEKDAYS = set(sync_cfg["giorni_settimanali"])
    START_H = int(sync_cfg["ora_inizio"])
    END_H = int(sync_cfg["ora_fine"])
    TIMEZONE = sync_cfg.get("time_zone", "Europe/Rome")
    POLL_SECONDS_DEFAULT = float(sync_cfg.get("tempo_polling", 5))

    percorso_db = percorsi.get("percorso_db")
    if not percorso_db:
        raise KeyError("Percorsi.percorso_db mancante in config.toml")

    percorso_db_log = percorsi.get("percorso_db_log")
    if not percorso_db_log:
        raise KeyError("Percorsi.percorso_db_log mancante in config.toml")

    sql_params = sync_cfg.get("sql_params")
    if not sql_params:
        raise KeyError("sync_config.sql_params mancante in config.toml")

    params = urllib.parse.quote_plus(sql_params)

    sqlite_engine_app = create_engine(f"sqlite:///{percorso_db}")
    sqlserver_engine_app = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    sqlite_engine_log = create_engine(f"sqlite:///{percorso_db_log}")

    logging.info("sqlite_engine_app=%s", sqlite_engine_app.url)
    logging.info("sqlserver_engine_app=%s", sqlserver_engine_app.url)
    logging.info("sqlite_engine_log=%s", sqlite_engine_log.url)

    _INITIALIZED = True


def ensure_init():
    """Helper: garantisce che init() sia stata chiamata."""
    if not _INITIALIZED:
        init()


def load_config(config: Path) -> dict:
    """
    Caricamento e lettura file configurazioni

    :return: Ritorna un dizionario con le configurazioni
    :rtype: dict[Any, Any]
    """
    with config.open("rb") as f:
        return tomllib.load(f)


# endregion
# region ACQUISIZIONE DATI


def leggi_view(
    table: Literal[
        "vwESRisorse",
        "vwESOdP",
        "vwESOdPFasi",
        "vwESLavorazioni",
        "vwESOdPComponenti",
        "vwESRisorse",
        "vwESReparti",
        "vwESCausaliAttivita",
        "vwESGiacenza",
        "vwESGiacenzaLotti",
        "vwESArticoli",
        "vwESMagazzini",
        "vwESFamiglia",
        "vwESMacroFamiglia",
    ],
    colonna_filtro_esclusi: Optional[str] = "",
    colonna_filtro_stato: Optional[str] = "",
) -> pd.DataFrame:
    """
    Lettura della view

    Legge ed esegue due filtri in base ai parametri di input

    :param table: Nome della tabella
    :type table: Literal["vwESRisorse", "vwESOdP", "vwESOdPFasi", "vwESLavorazioni", "vwESOdPComponenti", "vwESRisorse", "vwESReparti", "vwESCausaliAttivita", "vwESGiacenza", "vwESGiacenzaLotti", "vwESArticoli", "vwESMagazzini", "vwESFamiglia", "vwESMacroFamiglia"]
    :param colonna_filtro_esclusi:  Nome del filtro da richiamare (filtri di esclusione) Opzionale
    :type colonna_filtro_esclusi: Optional[str]
    :param colonna_filtro_stato: Nome del filtro da richiamare (filtri di inclusione)
    :type colonna_filtro_stato: Optional[str]
    :return: Dataframe filtrato della view selezionata
    :rtype: DataFrame
    """
    ensure_init()
    query = f"SELECT * FROM BernardiProd.dbo.{table}"
    df = pd.read_sql(query, sqlserver_engine_app)
    if colonna_filtro_esclusi != "":
        df = df[
            ~df[colonna_filtro_esclusi].isin(ELEMENTI_ESCLUSI[colonna_filtro_esclusi])
        ]
        df = df.dropna(subset=[colonna_filtro_esclusi], how="any")
    if colonna_filtro_stato != "":
        df = df[df[colonna_filtro_stato] == ELEMENTI_SELEZIONATI[colonna_filtro_stato]]
        df = df.dropna(subset=[colonna_filtro_stato], how="any")
    df = df.reset_index(drop=True)
    return df


def filtra_odpfasi_con_odp(
    df_odpfasi: pd.DataFrame, df_odp: pd.DataFrame
) -> pd.DataFrame:
    """
    Incrocio dei dati per mantenere le linee di df_odpfasi che corrispondono a [IdDocumento, IdRiga] di df_odp

    :param df_odpfasi: dataframe con le fasi degli ordini di produzione
    :type df_odpfasi: pd.DataFrame
    :param df_odp: dataframe con gli ordini di produzione
    :type df_odp: pd.DataFrame
    :return: DataFrame filtrato per IdDocumento e IdRiga di df_odp
    :rtype: pd.DataFrame
    """
    df_odpfasi_filtered = df_odpfasi.merge(
        df_odp[["IdDocumento", "IdRiga"]], on=["IdDocumento", "IdRiga"], how="right"
    )
    return df_odpfasi_filtered


def filtra_odp_componenti_con_odp(
    df_odp_componenti: pd.DataFrame, df_odp: pd.DataFrame
) -> pd.DataFrame:
    """
    Incrocio dei dati per mantenere le linee di df_odp_componenti che si trovano in IdDocumento e IdRiga di df_odp.
    Il filtro su df_odp_componenti è ["IdDocumento", "IdRigaPadre"]

    :rtype: pd.DataFrame
    :param df_odp_componenti: DataFrame con i componenti in base agli ordini di produzione
    :type df_odp_componenti: pd.DataFrame
    :param df_odp: DataFrame con gli ordini di produzione
    :type df_odp: pd.DataFrame
    :return: DataFrame filtrato IdDocumento e IdRiga di df_odp
    :rtype: pd.DataFrame
    """
    df_odp_componenti_filtered = df_odp_componenti.merge(
        df_odp[["IdDocumento", "IdRiga"]],
        left_on=["IdDocumento", "IdRigaPadre"],
        how="right",
        right_on=["IdDocumento", "IdRiga"],
        suffixes=["", "_y"],
    )
    colonne_con_y = df_odp_componenti_filtered.columns.tolist()
    colonne_con_y = [colonna for colonna in colonne_con_y if colonna.endswith("_y")]

    df_odp_componenti_filtered = df_odp_componenti_filtered.drop(columns=colonne_con_y)
    return df_odp_componenti_filtered


def inserimento_reparto_da_risorsa(
    df_odp_fasi: pd.DataFrame, df_risorse: pd.DataFrame
) -> pd.DataFrame:
    """
    Inserimento del reparto in base alla risorsa richiamata

    :param df_odp_fasi: dataframe con le fasi degli ordini di produzione
    :type df_odp_fasi: pd.DataFrame
    :param df_risorse: dataframe con risorse e reparti associati
    :type df_risorse: pd.DataFrame
    :return: Dataframe con i reparti in CodReparto associati al codice risorsa
    :rtype: pd.DataFrame
    """
    df_odp_fasi_reparti = df_odp_fasi.merge(
        df_risorse[["CodRisorsaProd", "CodReparto"]], on=["CodRisorsaProd"], how="left"
    )
    df_odp_fasi_reparti = df_odp_fasi_reparti.dropna(subset="CodReparto", how="any")
    return df_odp_fasi_reparti


def unione_fasi_componenti(
    df_fasi: pd.DataFrame, df_componenti: pd.DataFrame, df_articoli: pd.DataFrame
) -> pd.DataFrame:
    """
    Join tra il df delle fasi e quello dei componenti per fase. Al df_componenti vengono rinominate le righe IdRigaPadre e IdRiga rispettivamente in IdRiga e IdRigacomponente
    Viene inoltre aggiunta la descrizione del componente per codice articolo
    :param df_fasi: dataframe fasi
    :type df_fasi: pd.DataFrame
    :param df_componenti: dataframe componenti per fase
    :type df_componenti: pd.DataFrame
    :return: Dataframe composto dai codici articolo (CodArt) divisi per fase con le quantità di materiale necessario a codice per fase
    :rtype: pd.DataFrame
    """
    df_componenti = df_componenti.rename(
        columns={"IdRiga": "IdRigacomponente", "IdRigaPadre": "IdRiga"}
    )
    fasi_indexed = df_fasi.set_index(["IdDocumento", "IdRiga", "NumFase"], drop=True)
    comp_indexed = df_componenti.set_index(
        ["IdDocumento", "IdRiga", "NumFase"], drop=True
    )
    df_fasi_componenti = fasi_indexed.join(
        comp_indexed,
        on=["IdDocumento", "IdRiga", "NumFase"],
        validate="1:m",
        how="left",
    )
    df_fasi_componenti = df_fasi_componenti.reset_index(drop=False)
    df_fasi_componenti = df_fasi_componenti.merge(
        df_articoli[["CodArt", "DesArt", "TecniciUm", "GestioneLotto"]],
        on="CodArt",
        how="left",
    )
    return df_fasi_componenti


def generazione_lista(
    df: pd.DataFrame,
    chiavi: list[str],
    rename_col: str,
    list_columns: list[str],
    dumps_json: bool = True,
) -> pd.DataFrame:
    tmp = df.copy()

    if len(list_columns) == 1:
        col = list_columns[0]
        componenti_per_odp = (
            tmp.groupby(chiavi, dropna=False)[col]
            .apply(lambda s: pd.unique(s.dropna()).tolist())
            .rename(rename_col)
            .reset_index()
        )
    else:
        componenti_per_odp = (
            tmp.groupby(chiavi, dropna=False)[list_columns]
            .apply(lambda g: [tuple(r) for r in g.to_numpy()])
            .rename(rename_col)
            .reset_index()
        )

    if dumps_json:
        componenti_per_odp[rename_col] = componenti_per_odp[rename_col].apply(
            lambda x: (
                json.dumps(x, ensure_ascii=False, default=str)
                if isinstance(x, list)
                else None
            )
        )

    return componenti_per_odp


def _sanitize_json_scalar(value):
    """
    Converte i valori pandas/numpy non JSON-validi in None.
    Evita che json.dumps serializzi NaN come token non valido per il frontend.
    """
    if pd.isna(value):
        return None
    return value


def _sanitize_records_for_json(records: list[dict]) -> list[dict]:
    return [
        {key: _sanitize_json_scalar(value) for key, value in record.items()}
        for record in records
    ]


def generazione_dizionario(
    df: pd.DataFrame,
    chiavi: list[str],
    rename_col: str,
    list_columns: list[str],
    data_in: Optional[str] = "normale",
) -> pd.DataFrame:
    """
    Genera un dizionario raggruppando le chiavi

    Raggruppa in base alla costante CHIAVI (impostata nella funzione madre) e crea una lista di elementi che verrà inserita con il nome della colonna rename_col

    :param data_in: Specifica se la colonna contiene dati di tipo data o normale
    :type data_in: Optional[str]
    :param df: Dataframe di input
    :type df: pd.DataFrame
    :param chiavi: elenco delle colonne da raggruppare
    :type chiavi: list[str]
    :param rename_col: Nome della colonna finale
    :type rename_col: str
    :param list_columns: Lista con le colonne da raggruppare
    :type list_columns: list[str]
    :return: Dataframe raggruppati per Codice, descrizione e quantità
    :rtype: pd.DataFrame
    """

    df = df.copy()

    # DistintaMateriale: elimina le pseudo-righe create dalle fasi senza componenti
    # (tipicamente CodArt/DesArt/Quantita/GestioneLotto tutti NaN).
    if rename_col == "DistintaMateriale" and "CodArt" in list_columns:
        codici = df["CodArt"].astype("string").str.strip()
        df = df[codici.notna() & codici.ne("")]

    df = df.set_index(chiavi)
    if data_in == "data":
        df[rename_col] = df[rename_col].dt.strftime("%d/%m/%Y %H:%M:%S")

    componenti_per_odp = (
        df.groupby(chiavi)
        .apply(lambda g: _sanitize_records_for_json(g[list_columns].to_dict("records")))
        .rename(rename_col)
        .reset_index()
    )
    componenti_per_odp[rename_col] = componenti_per_odp[rename_col].apply(
        lambda x: (
            json.dumps(x, ensure_ascii=False, allow_nan=False)
            if isinstance(x, (list, tuple))
            else None
        )
    )
    return componenti_per_odp


def inserimento_distinta_in_odp(
    df_odp: pd.DataFrame, componenti_per_odp: pd.DataFrame, chiavi: list[str]
) -> pd.DataFrame:
    """
    Inserimento della distinta nella linea d'ordine

    Inserisce nella linea d'ordine la distinta per l'ordine di produzione. rimuove le colonne non necessarie al database di ingresso

    :param df_odp: Dataframe degli ordini di produzione
    :type df_odp: pd.DataFrame
    :param componenti_per_odp: Dataframe con la distinta base
    :type componenti_per_odp: pd.DataFrame
    :param chiavi: elenco delle colonne su cui unire i due df
    :type chiavi: list[str]
    :return: dataframe con gli ordini e la distinta
    :rtype: pd.DataFrame
    """
    df_odp = df_odp.merge(componenti_per_odp, on=chiavi, how="left")
    df_odp = df_odp.drop(
        columns=["NumRegistraz", "DataRegistrazione", "UnitaMisura", "QtaResidua"],
        errors="ignore",
    )
    return df_odp


def inserimento_dati_fasi_in_odp(
    df_odp: pd.DataFrame, df_odpfasi: pd.DataFrame, chiavi: list[str]
) -> pd.DataFrame:
    """
    Inserimento dei dati divisi per fase

    Inserisce i dati delle fasi divisi in dizionario, in questo modo posso avere l'elenco delle fasi e la divisione dei vari componenti nelle fasi

    :param df_odp: Dataframe degli ordini di produzione
    :type df_odp: pd.DataFrame
    :param df_odpfasi: Dataframe con gli ordini di produzione con le fasi
    :type df_odpfasi: pd.DataFrame
    :param chiavi: Chiavi su cui vengono filtrati i dataframe
    :type chiavi: list[str]
    :return: dataframe con l'elenco delle fasi, i codici lavorazione, i codici risorsa, i codici reparto,
    data inizio e fine schedulazione ed il tempo previsto di lavorazione divisi per fasi
    :rtype: DataFrame
    """

    num_fase_per_odp = generazione_lista(
        df=df_odpfasi, chiavi=chiavi, rename_col="NumFase", list_columns=["NumFase"]
    ).set_index(["IdDocumento", "IdRiga"])

    cod_lavorazione_per_odp = generazione_lista(
        df=df_odpfasi,
        chiavi=chiavi,
        rename_col="CodLavorazione",
        list_columns=["CodLavorazione"],
    ).set_index(["IdDocumento", "IdRiga"])

    cod_risorsa_prod_per_odp = generazione_lista(
        df=df_odpfasi,
        chiavi=chiavi,
        rename_col="CodRisorsaProd",
        list_columns=["CodRisorsaProd"],
    ).set_index(["IdDocumento", "IdRiga"])

    cod_reparto_per_odp = generazione_lista(
        df=df_odpfasi,
        chiavi=chiavi,
        rename_col="CodReparto",
        list_columns=["CodReparto"],
    ).set_index(["IdDocumento", "IdRiga"])

    data_inizio_sched_per_odp = generazione_lista(
        df=df_odpfasi,
        chiavi=chiavi,
        rename_col="DataInizioSched",
        list_columns=["DataInizioSched"],
    ).set_index(["IdDocumento", "IdRiga"])

    data_fine_sched_per_odp = generazione_lista(
        df=df_odpfasi,
        chiavi=chiavi,
        rename_col="DataFineSched",
        list_columns=["DataFineSched"],
    ).set_index(["IdDocumento", "IdRiga"])

    tempo_previsto_lavoraz_per_odp = generazione_lista(
        df=df_odpfasi,
        chiavi=chiavi,
        rename_col="TempoPrevistoLavoraz",
        list_columns=["TempoPrevistoLavoraz"],
    ).set_index(["IdDocumento", "IdRiga"])

    tempo_attrezzaggio_odp = generazione_lista(
        df=df_odpfasi,
        chiavi=chiavi,
        rename_col="TempoAttrezzaggio",
        list_columns=["TempoAttrezzaggio"],
    ).set_index(["IdDocumento", "IdRiga"])

    df_dizionari = [
        num_fase_per_odp,
        cod_lavorazione_per_odp,
        cod_risorsa_prod_per_odp,
        cod_reparto_per_odp,
        data_inizio_sched_per_odp,
        data_fine_sched_per_odp,
        tempo_previsto_lavoraz_per_odp,
        tempo_attrezzaggio_odp,
    ]
    df_fasi_raggruppate = ft.reduce(
        lambda left, right: pd.merge(left, right, on=chiavi), df_dizionari
    )

    df_odp = df_odp.set_index(["IdDocumento", "IdRiga"])
    df_odp = df_odp.join(df_fasi_raggruppate, how="left")
    df_odp = df_odp.reset_index(drop=False)
    return df_odp


def gestione_lotto_matricola_famiglia(
    df_odp: pd.DataFrame, df_articoli: pd.DataFrame
) -> pd.DataFrame:
    """
    Inserimento nel dataframe la gestione per lotto, matricola e la famiglia

    Inserisce i dati per identificare se il codice deve essere gestito per lotto e/o con una matricola, inoltre inserisce anche la famiglia di appartenenza

    :param df_odp: Dataframe con gli ordini di produzione
    :type df_odp: pd.DataFrame
    :param df_articoli: Dataframe con l'anagrafica degli articoli
    :type df_articoli: pd.DataFrame
    :return: Dataframe degli ordini arricchito con la gestione lotto, matricola e la famiglia di appartenenza
    :rtype: DataFrame
    """
    df_odp = df_odp.merge(
        df_articoli[
            [
                "CodArt",
                "GestioneLotto",
                "GestioneMatricola",
                "CodFamiglia",
                "CodClassifTecnica",
                "DesArt",
                "IndiceModifica",
            ]
        ],
        on="CodArt",
        how="left",
    )
    df_odp = df_odp.dropna(subset=["GestioneLotto", "GestioneMatricola", "CodFamiglia"])
    return df_odp


def inserimento_macrofamiglia(
    df_odp: pd.DataFrame, df_famiglia: pd.DataFrame
) -> pd.DataFrame:
    """
    Inserimento nel dataframe la macrofamiglia di appartenenza

    Inserisce la macrofamiglia di appartenenza all'ordine di produzione

    :param df_odp: Dataframe con gli ordini di produzione
    :type df_odp: pd.DataFrame
    :param df_famiglia: Dataframe con l'elenco delle macrofamiglie e famiglie associate
    :type df_famiglia: pd.DataFrame
    :return: Dataframe con l'elenco degli ordini arricchito con la macrofamiglia
    :rtype: DataFrame
    """
    df_odp = df_odp.merge(
        df_famiglia[["CodFamiglia", "CodMacrofamiglia"]], on=["CodFamiglia"], how="left"
    )
    df_odp = df_odp.dropna(subset=["CodMacrofamiglia"])
    return df_odp


def inserisci_o_ignora(sqltable, conn, keys, data_iter) -> int:
    """
    Inserimento delle righe a db se non già presenti altrimenti ignora.
    Ritorna il numero di righe realmente inserite quando possibile.
    """
    table = sqltable.table
    rows = list(data_iter)
    if not rows:
        return 0
    data = [dict(zip(keys, row)) for row in rows]
    stmt = sqlite_insert(table).values(data).prefix_with("OR IGNORE")
    conn.execute(stmt)
    if isinstance(conn, SAConnection):
        try:
            res = conn.execute(sa.text("SELECT changes()"))
            # SQLAlchemy 1.4/2.x: scalar() o scalar_one()
            if hasattr(res, "scalar_one"):
                n = res.scalar_one()
            else:
                n = res.scalar()
            return int(n or 0)
        except Exception:
            # fallback prudente
            return len(data)
    return len(data)


# endregion
# region ELABORAZIONE

PK_COLS = ("IdDocumento", "IdRiga")

INPUT_ODP_ERP_COLS = [
    "IdDocumento",
    "IdRiga",
    "RifRegistraz",
    "CodArt",
    "DesArt",
    "Quantita",
    "NumFase",
    "CodLavorazione",
    "CodRisorsaProd",
    "DataInizioSched",
    "DataFineSched",
    "GestioneLotto",
    "GestioneMatricola",
    "DistintaMateriale",
    "CodMatricola",
    "StatoRiga",
    "CodFamiglia",
    "CodMacrofamiglia",
    "CodMagPrincipale",
    "CodReparto",
    "TempoPrevistoLavoraz",
    "StatoOrdine",
    "CodClassifTecnica",
    "CodTipoDoc",
    "IndiceModifica",
    "TempoAttrezzaggio",
]

INPUT_ODP_RUNTIME_COLS = [
    "IdDocumento",
    "IdRiga",
    "RifRegistraz",
    "Stato_odp",
    "Utente_operazione",
    "FaseAttiva",
    "Note",
    "QtyDaLavorare",
    "RisorsaAttiva",
    "LavorazioneAttiva",
    "AttrezzaggioAttivo",
]

INPUT_ODP_ERP_UPDATE_COLS = [c for c in INPUT_ODP_ERP_COLS if c not in PK_COLS]


def _fetch_existing_pks(
    engine,
    pk_tuples,
    pk_cols=("IdDocumento", "IdRiga"),
    table_name="input_odp",
) -> set[tuple]:
    """
    Ritorna un set di PK già presenti nella tabella indicata.
    Chunking per evitare limiti di parametri SQLite.
    """
    if not pk_tuples:
        return set()

    md = sa.MetaData()
    t = sa.Table(table_name, md, autoload_with=engine)
    tpl = sa.tuple_(t.c[pk_cols[0]], t.c[pk_cols[1]])

    existing = set()
    for chunk in _chunked(pk_tuples, 400):
        q = sa.select(t.c[pk_cols[0]], t.c[pk_cols[1]]).where(tpl.in_(chunk))
        with engine.connect() as conn:
            rows = conn.execute(q).fetchall()
            existing.update(tuple(r) for r in rows)

    return existing


def _update_rows_by_pk(
    engine,
    df: pd.DataFrame,
    *,
    table_name: str,
    pk_cols: tuple[str, str] = ("IdDocumento", "IdRiga"),
    update_cols: list[str],
    chunk_size: int = 500,
) -> int:
    """
    UPDATE batch (executemany) su SQLite.
    Aggiorna solo le colonne indicate in update_cols.
    """
    if df.empty or not update_cols:
        return 0

    md = sa.MetaData()
    t = sa.Table(table_name, md, autoload_with=engine)

    pk_bind_names = {
        pk_cols[0]: f"b_pk_{pk_cols[0]}",
        pk_cols[1]: f"b_pk_{pk_cols[1]}",
    }

    where_clause = sa.and_(
        t.c[pk_cols[0]] == sa.bindparam(pk_bind_names[pk_cols[0]]),
        t.c[pk_cols[1]] == sa.bindparam(pk_bind_names[pk_cols[1]]),
    )

    stmt = (
        sa.update(t)
        .where(where_clause)
        .values(**{c: sa.bindparam(c) for c in update_cols})
    )

    needed_cols = [*pk_cols, *update_cols]
    df_exec = df[needed_cols].copy()
    df_exec = df_exec.where(pd.notna(df_exec), None)

    records = []
    for rec in df_exec.to_dict("records"):
        row = dict(rec)
        row[pk_bind_names[pk_cols[0]]] = row.pop(pk_cols[0])
        row[pk_bind_names[pk_cols[1]]] = row.pop(pk_cols[1])
        records.append(row)

    updated = 0
    with engine.begin() as conn:
        for chunk in _chunked(records, chunk_size):
            res = conn.execute(stmt, chunk)
            if getattr(res, "rowcount", None) is not None and res.rowcount >= 0:
                updated += int(res.rowcount)
            else:
                updated += len(chunk)

    return updated


def _build_runtime_seed(df_input_odp: pd.DataFrame) -> pd.DataFrame:
    """
    Costruisce il seed iniziale per input_odp_runtime a partire dallo snapshot ERP.
    Va usato solo per le PK mancanti nella tabella runtime.
    """
    df_runtime = df_input_odp[
        [
            "IdDocumento",
            "IdRiga",
            "RifRegistraz",
            "Quantita",
            "CodLavorazione",
            "CodRisorsaProd",
            "TempoAttrezzaggio",
        ]
    ].copy()

    df_runtime["Stato_odp"] = "Pianificata"
    df_runtime["Utente_operazione"] = "sync_input"
    df_runtime["FaseAttiva"] = "1"
    df_runtime["Note"] = None
    df_runtime["QtyDaLavorare"] = df_runtime["Quantita"]
    df_runtime["RisorsaAttiva"] = df_runtime["CodRisorsaProd"].apply(
        estrai_lavorazione_attiva
    )
    df_runtime["LavorazioneAttiva"] = df_runtime["CodLavorazione"].apply(
        estrai_lavorazione_attiva
    )
    df_runtime["AttrezzaggioAttivo"] = df_runtime["TempoAttrezzaggio"].apply(
        estrai_lavorazione_attiva
    )

    return df_runtime[INPUT_ODP_RUNTIME_COLS].copy()


def _norm_text(value) -> str:
    return str(value or "").strip()


def _now_sync_iso() -> str:
    tz_name = TIMEZONE or "Europe/Rome"
    return datetime.now(ZoneInfo(tz_name)).isoformat(timespec="seconds")


def _safe_token(value, default="x") -> str:
    raw = _norm_text(value)
    if not raw:
        return default
    raw = re.sub(r"[^A-Za-z0-9_.-]+", "-", raw)
    return raw.strip("-") or default


def _build_sync_operation_group_id(
    *,
    id_documento: str,
    id_riga: str,
    action: str,
    when_iso: str,
) -> str:
    stamp = re.sub(r"\D+", "", _norm_text(when_iso))[:14]
    if not stamp:
        stamp = datetime.now(ZoneInfo(TIMEZONE or "Europe/Rome")).strftime(
            "%Y%m%d%H%M%S"
        )

    return (
        f"{stamp}_"
        f"{_safe_token(id_documento, 'doc')}_"
        f"{_safe_token(id_riga, 'riga')}_"
        f"{_safe_token(action, 'sync')}"
    )


def _pk_key(id_documento, id_riga) -> tuple[str, str]:
    return (_norm_text(id_documento), _norm_text(id_riga))


def _build_sync_operation_group_map(
    df_new_erp: pd.DataFrame,
    when_iso: str,
) -> dict[tuple[str, str], str]:
    if df_new_erp.empty:
        return {}

    keys = (
        df_new_erp[["IdDocumento", "IdRiga"]]
        .astype(str)
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )

    return {
        _pk_key(id_documento, id_riga): _build_sync_operation_group_id(
            id_documento=id_documento,
            id_riga=id_riga,
            action="sync_nuovo_ordine",
            when_iso=when_iso,
        )
        for id_documento, id_riga in keys
    }


def _get_sync_operation_group_id(
    operation_groups: dict[tuple[str, str], str],
    *,
    id_documento,
    id_riga,
    when_iso: str,
) -> str:
    key = _pk_key(id_documento, id_riga)
    return operation_groups.get(key) or _build_sync_operation_group_id(
        id_documento=key[0],
        id_riga=key[1],
        action="sync_nuovo_ordine",
        when_iso=when_iso,
    )


def _build_input_odp_log_rows(
    df_new_erp: pd.DataFrame,
    df_new_runtime: pd.DataFrame,
    when_iso: str,
    operation_groups: dict[tuple[str, str], str],
) -> pd.DataFrame:
    if df_new_erp.empty:
        return pd.DataFrame()

    runtime_subset = (
        df_new_runtime[
            [
                "IdDocumento",
                "IdRiga",
                "FaseAttiva",
                "QtyDaLavorare",
                "RisorsaAttiva",
                "LavorazioneAttiva",
                "AttrezzaggioAttivo",
                "Note",
            ]
        ].copy()
        if not df_new_runtime.empty
        else pd.DataFrame(
            columns=[
                "IdDocumento",
                "IdRiga",
                "FaseAttiva",
                "QtyDaLavorare",
                "RisorsaAttiva",
                "LavorazioneAttiva",
                "AttrezzaggioAttivo",
                "Note",
            ]
        )
    )

    df_log = df_new_erp.merge(
        runtime_subset,
        on=["IdDocumento", "IdRiga"],
        how="left",
    ).copy()

    df_log["logged_at"] = when_iso
    df_log["OperationGroupId"] = df_log.apply(
        lambda r: _get_sync_operation_group_id(
            operation_groups,
            id_documento=r["IdDocumento"],
            id_riga=r["IdRiga"],
            when_iso=when_iso,
        ),
        axis=1,
    )

    df_log["QtyDaLavorare"] = df_log["QtyDaLavorare"].where(
        df_log["QtyDaLavorare"].notna(), df_log["Quantita"]
    )
    df_log["FaseAttiva"] = df_log["FaseAttiva"].where(df_log["FaseAttiva"].notna(), "1")

    df_log["FaseConsuntivata"] = None
    df_log["QuantitaConforme"] = None
    df_log["QuantitaNonConforme"] = None
    df_log["TempoFunzionamentoFinale"] = None
    df_log["TempoNonFunzionamentoMinuti"] = None
    df_log["TempoNonFunzionamentoSecondi"] = None
    df_log["ChiusuraParziale"] = None
    df_log["NoteChiusura"] = "Inserimento nuova riga da sync_input"
    df_log["StatoOrdinePre"] = ""
    df_log["StatoOrdinePost"] = "Pianificata"
    df_log["QtyDaLavorarePre"] = ""
    df_log["QtyDaLavorarePost"] = df_log["QtyDaLavorare"]
    df_log["ClosedBy"] = "sync_input"
    df_log["ClosedAt"] = when_iso
    df_log["RifOrdinePrinc"] = None

    cols = [
        "logged_at",
        "OperationGroupId",
        "IdDocumento",
        "IdRiga",
        "RifRegistraz",
        "CodArt",
        "DesArt",
        "Quantita",
        "NumFase",
        "CodLavorazione",
        "CodRisorsaProd",
        "DataInizioSched",
        "DataFineSched",
        "GestioneLotto",
        "GestioneMatricola",
        "DistintaMateriale",
        "CodMatricola",
        "StatoRiga",
        "CodFamiglia",
        "CodMacrofamiglia",
        "CodMagPrincipale",
        "CodReparto",
        "TempoPrevistoLavoraz",
        "CodClassifTecnica",
        "CodTipoDoc",
        "FaseAttiva",
        "QtyDaLavorare",
        "RisorsaAttiva",
        "LavorazioneAttiva",
        "AttrezzaggioAttivo",
        "RifOrdinePrinc",
        "Note",
        "FaseConsuntivata",
        "QuantitaConforme",
        "QuantitaNonConforme",
        "TempoFunzionamentoFinale",
        "TempoNonFunzionamentoMinuti",
        "TempoNonFunzionamentoSecondi",
        "ChiusuraParziale",
        "NoteChiusura",
        "StatoOrdinePre",
        "StatoOrdinePost",
        "QtyDaLavorarePre",
        "QtyDaLavorarePost",
        "ClosedBy",
        "ClosedAt",
    ]

    return df_log[cols].where(pd.notna(df_log), None)


def _build_runtime_log_rows(
    df_new_erp: pd.DataFrame,
    df_new_runtime: pd.DataFrame,
    when_iso: str,
    operation_groups: dict[tuple[str, str], str],
) -> pd.DataFrame:
    if df_new_erp.empty or df_new_runtime.empty:
        return pd.DataFrame()

    df_runtime_for_new = df_new_runtime.merge(
        df_new_erp[["IdDocumento", "IdRiga", "RifRegistraz", "CodArt", "CodReparto"]],
        on=["IdDocumento", "IdRiga", "RifRegistraz"],
        how="inner",
    ).copy()

    if df_runtime_for_new.empty:
        return pd.DataFrame()

    df_runtime_for_new["logged_at"] = when_iso
    df_runtime_for_new["OperationGroupId"] = df_runtime_for_new.apply(
        lambda r: _get_sync_operation_group_id(
            operation_groups,
            id_documento=r["IdDocumento"],
            id_riga=r["IdRiga"],
            when_iso=when_iso,
        ),
        axis=1,
    )
    df_runtime_for_new["EventSequence"] = 1
    df_runtime_for_new["Topic"] = "nuovo_ordine_sync"
    df_runtime_for_new["Scope"] = df_runtime_for_new["CodReparto"]
    df_runtime_for_new["PayloadJson"] = df_runtime_for_new.apply(
        lambda r: json.dumps(
            {
                "azione": "sync_seed_runtime",
                "utente": "sync_input",
                "fase": _norm_text(r["FaseAttiva"]),
                "qty_da_lavorare": _norm_text(r["QtyDaLavorare"]),
                "risorsa_attiva": _norm_text(r["RisorsaAttiva"]),
                "lavorazione_attiva": _norm_text(r["LavorazioneAttiva"]),
                "attrezzaggio_attivo": _norm_text(r["AttrezzaggioAttivo"]),
            },
            ensure_ascii=False,
        ),
        axis=1,
    )
    df_runtime_for_new["Azione"] = "sync_seed_runtime"
    df_runtime_for_new["Motivo"] = "Creazione riga input_odp_runtime da sync_input"
    df_runtime_for_new["UtenteOperazione"] = "sync_input"
    df_runtime_for_new["EventAt"] = when_iso
    df_runtime_for_new["StatoOdpPre"] = ""
    df_runtime_for_new["StatoOdpPost"] = df_runtime_for_new["Stato_odp"]
    df_runtime_for_new["StatoOrdinePre"] = ""
    df_runtime_for_new["StatoOrdinePost"] = "Pianificata"
    df_runtime_for_new["FasePre"] = ""
    df_runtime_for_new["FasePost"] = df_runtime_for_new["FaseAttiva"]
    df_runtime_for_new["DataInCaricoPre"] = ""
    df_runtime_for_new["DataInCaricoPost"] = ""
    df_runtime_for_new["DataUltimaAttivazionePre"] = ""
    df_runtime_for_new["DataUltimaAttivazionePost"] = ""
    df_runtime_for_new["TempoFunzionamentoPre"] = ""
    df_runtime_for_new["TempoFunzionamentoPost"] = ""
    df_runtime_for_new["ElapsedSeconds"] = None
    df_runtime_for_new["TempoNonFunzionamentoMinuti"] = None
    df_runtime_for_new["TempoNonFunzionamentoSecondi"] = None
    df_runtime_for_new["QtyDaLavorarePre"] = ""
    df_runtime_for_new["QtyDaLavorarePost"] = df_runtime_for_new["QtyDaLavorare"]
    df_runtime_for_new["QuantitaConforme"] = None
    df_runtime_for_new["QuantitaNonConforme"] = None
    df_runtime_for_new["Causale"] = None
    df_runtime_for_new["Note"] = "Inserimento riga runtime da sync_input"
    df_runtime_for_new["RifOrdinePrinc"] = None

    cols = [
        "logged_at",
        "OperationGroupId",
        "EventSequence",
        "Topic",
        "Scope",
        "CodArt",
        "CodReparto",
        "PayloadJson",
        "IdDocumento",
        "IdRiga",
        "RifRegistraz",
        "Azione",
        "Motivo",
        "UtenteOperazione",
        "EventAt",
        "StatoOdpPre",
        "StatoOdpPost",
        "StatoOrdinePre",
        "StatoOrdinePost",
        "FasePre",
        "FasePost",
        "DataInCaricoPre",
        "DataInCaricoPost",
        "DataUltimaAttivazionePre",
        "DataUltimaAttivazionePost",
        "TempoFunzionamentoPre",
        "TempoFunzionamentoPost",
        "ElapsedSeconds",
        "TempoNonFunzionamentoMinuti",
        "TempoNonFunzionamentoSecondi",
        "QtyDaLavorarePre",
        "QtyDaLavorarePost",
        "QuantitaConforme",
        "QuantitaNonConforme",
        "Causale",
        "Note",
        "RifOrdinePrinc",
    ]

    return df_runtime_for_new[cols].where(pd.notna(df_runtime_for_new), None)


def _write_sync_logs(
    *,
    df_new_erp: pd.DataFrame,
    df_new_runtime: pd.DataFrame,
) -> None:
    if sqlite_engine_log is None:
        raise RuntimeError("sqlite_engine_log non inizializzato")

    if df_new_erp.empty:
        return

    erp_keys = {
        _pk_key(r["IdDocumento"], r["IdRiga"])
        for _, r in df_new_erp[["IdDocumento", "IdRiga"]].iterrows()
    }
    runtime_keys = {
        _pk_key(r["IdDocumento"], r["IdRiga"])
        for _, r in df_new_runtime[["IdDocumento", "IdRiga"]].iterrows()
    }

    missing_runtime = sorted(erp_keys - runtime_keys)
    if missing_runtime:
        raise RuntimeError(
            f"Nuovi ordini senza seed runtime coerente: {missing_runtime}"
        )

    when_iso = _now_sync_iso()
    operation_groups = _build_sync_operation_group_map(df_new_erp, when_iso)

    righe_input_log = 0
    df_input_log = _build_input_odp_log_rows(
        df_new_erp=df_new_erp,
        df_new_runtime=df_new_runtime,
        when_iso=when_iso,
        operation_groups=operation_groups,
    )
    if not df_input_log.empty:
        righe_input_log = int(
            df_input_log.to_sql(
                name="input_odp_log",
                con=sqlite_engine_log,
                if_exists="append",
                index=False,
                method=inserisci_o_ignora,
            )
            or 0
        )

    righe_runtime_log = 0
    df_runtime_log = _build_runtime_log_rows(
        df_new_erp=df_new_erp,
        df_new_runtime=df_new_runtime,
        when_iso=when_iso,
        operation_groups=operation_groups,
    )
    if not df_runtime_log.empty:
        righe_runtime_log = int(
            df_runtime_log.to_sql(
                name="odp_runtime_log",
                con=sqlite_engine_log,
                if_exists="append",
                index=False,
                method=inserisci_o_ignora,
            )
            or 0
        )

    logging.info(
        "Log sync nuovi ordini | input_odp_log=%s | odp_runtime_log=%s",
        righe_input_log,
        righe_runtime_log,
    )


def _chunked(seq, size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def int_format(x):
    try:
        return int(x)
    except (ValueError, TypeError):
        return 0


def filtri_giacenza_lotti(df_giacenza_lotti: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra il dataframe della giacenza lotti per mantenere solo le righe con giacenza maggiore di 0

    :param df_giacenza_lotti: Dataframe con la giacenza dei lotti
    :type df_giacenza_lotti: pd.DataFrame
    :return: Dataframe filtrato con solo i lotti con giacenza maggiore di 0
    :rtype: pd.DataFrame
    """
    df_giacenza_lotti["Giacenza"] = df_giacenza_lotti["Giacenza"].apply(int_format)
    df_giacenza_lotti_filtered = df_giacenza_lotti.loc[
        df_giacenza_lotti["Giacenza"] > 0
    ]
    df_giacenza_lotti_filtered_regexLotto = df_giacenza_lotti_filtered[
        df_giacenza_lotti_filtered["RifLottoAlfa"]
        .astype("string")
        .str.fullmatch(r"^\d{8}$")
    ]
    df_giacenza_lotti_filtered_regexLotto_Articolo = (
        df_giacenza_lotti_filtered_regexLotto[
            df_giacenza_lotti_filtered_regexLotto["CodArt"]
            .astype("string")
            .str.fullmatch(r"^[A-Z]{2}\d{2}-\d{3}-\d{4}$")
        ]
    )
    return df_giacenza_lotti_filtered_regexLotto_Articolo


def estrai_lavorazione_attiva(x):
    if pd.isna(x):
        return None

    if isinstance(x, str):
        x = json.loads(x)

    if isinstance(x, (list, tuple)):
        return x[0] if x else None

    return None


def elaborazione_dati(session: Session) -> None:
    """
    Funzione per l'inserimento dei dati nella tabella input_odp da inserire a db

    :param session: sessione Sqlalchemy ORM
    :type session: Session
    """

    ensure_init()
    global nuovo_ciclo
    df_odp = leggi_view(
        table="vwESOdP",
        colonna_filtro_esclusi="CodArt",
        colonna_filtro_stato="StatoOrdine",
    )
    df_odpfasi = (
        pd.DataFrame(
            leggi_view(table="vwESOdPFasi", colonna_filtro_esclusi="CodRisorsaProd")
        )
        .pipe(filtra_odpfasi_con_odp, df_odp=df_odp)
        .pipe(
            inserimento_reparto_da_risorsa,
            df_risorse=leggi_view(
                "vwESRisorse", colonna_filtro_esclusi="CodRisorsaProd"
            ),
        )
    )
    chiavi = ["IdDocumento", "IdRiga"]
    df_odpcomponenti = leggi_view("vwESOdPComponenti").pipe(
        filtra_odp_componenti_con_odp, df_odp=df_odp
    )
    df_articoli = leggi_view("vwESArticoli")
    df_fasi_componenti = unione_fasi_componenti(
        df_odpfasi, df_odpcomponenti, df_articoli
    )
    distinta_componenti = generazione_dizionario(
        df=df_fasi_componenti,
        chiavi=chiavi,
        rename_col="DistintaMateriale",
        list_columns=[
            "CodArt",
            "DesArt",
            "Quantita",
            "NumFase",
            "TecniciUm",
            "GestioneLotto",
        ],
    )

    df_input_odp = (
        inserimento_distinta_in_odp(
            df_odp=df_odp, componenti_per_odp=distinta_componenti, chiavi=chiavi
        )
        .pipe(inserimento_dati_fasi_in_odp, df_odpfasi=df_odpfasi, chiavi=chiavi)
        .pipe(gestione_lotto_matricola_famiglia, df_articoli=df_articoli)
        .pipe(
            inserimento_macrofamiglia,
            df_famiglia=leggi_view(
                "vwESFamiglia", colonna_filtro_esclusi="CodFamiglia"
            ),
        )
        .drop(columns=["DataInizioProduzione"])
    )
    # --- payload ERP puro ---
    for col in INPUT_ODP_ERP_COLS:
        if col not in df_input_odp.columns:
            df_input_odp[col] = None

    df_input_odp = df_input_odp[INPUT_ODP_ERP_COLS].copy()
    df_input_odp = df_input_odp.where(pd.notna(df_input_odp), None)
    df_input_odp = df_input_odp.drop_duplicates(subset=list(PK_COLS))

    # --- seed runtime iniziale ---
    df_runtime_seed = _build_runtime_seed(df_input_odp)

    # PK del batch corrente
    df_pk = df_input_odp[list(PK_COLS)].astype(str).drop_duplicates()
    pk_tuples = list(map(tuple, df_pk.to_numpy()))
    # PK già presenti nelle due tabelle
    existing_erp = _fetch_existing_pks(
        sqlite_engine_app,
        pk_tuples,
        pk_cols=PK_COLS,
        table_name="input_odp",
    )
    existing_runtime = _fetch_existing_pks(
        sqlite_engine_app,
        pk_tuples,
        pk_cols=PK_COLS,
        table_name="input_odp_runtime",
    )

    # split ERP: nuove righe vs righe già esistenti da aggiornare
    mask_new_erp = [
        tuple(x) not in existing_erp
        for x in df_input_odp[list(PK_COLS)].astype(str).to_numpy()
    ]
    mask_existing_erp = [
        tuple(x) in existing_erp
        for x in df_input_odp[list(PK_COLS)].astype(str).to_numpy()
    ]

    df_new_erp = df_input_odp.loc[mask_new_erp].copy()
    df_existing_erp = df_input_odp.loc[mask_existing_erp].copy()

    # runtime: crea solo le PK mancanti
    mask_new_runtime = [
        tuple(x) not in existing_runtime
        for x in df_runtime_seed[list(PK_COLS)].astype(str).to_numpy()
    ]
    df_new_runtime = df_runtime_seed.loc[mask_new_runtime].copy()

    df_giacenza_lotti = leggi_view(table="vwESGiacenzaLotti").pipe(
        filtri_giacenza_lotti
    )

    righe_inserite_odp = 0
    righe_aggiornate_odp = 0
    righe_inserite_runtime = 0
    righe_inserite_lotti = 0

    try:
        if not df_new_erp.empty:
            righe_inserite_odp = int(
                df_new_erp.to_sql(
                    name="input_odp",
                    con=sqlite_engine_app,
                    if_exists="append",
                    index=False,
                    method=inserisci_o_ignora,
                )
                or 0
            )

        if not df_existing_erp.empty:
            righe_aggiornate_odp = _update_rows_by_pk(
                sqlite_engine_app,
                df_existing_erp,
                table_name="input_odp",
                pk_cols=PK_COLS,
                update_cols=INPUT_ODP_ERP_UPDATE_COLS,
            )

        if not df_new_runtime.empty:
            righe_inserite_runtime = int(
                df_new_runtime.to_sql(
                    name="input_odp_runtime",
                    con=sqlite_engine_app,
                    if_exists="append",
                    index=False,
                    method=inserisci_o_ignora,
                )
                or 0
            )

        if not df_giacenza_lotti.empty:
            righe_inserite_lotti = int(
                df_giacenza_lotti.to_sql(
                    name="giacenza_lotti",
                    con=sqlite_engine_app,
                    if_exists="append",
                    index=False,
                    method=inserisci_o_ignora,
                )
                or 0
            )

        # log punto 1: solo nuovi ordini
        if not df_new_erp.empty:
            _write_sync_logs(
                df_new_erp=df_new_erp,
                df_new_runtime=df_new_runtime,
            )

    except (sa.exc.SQLAlchemyError, sq.Error, RuntimeError, ValueError):
        logging.exception(
            "Errore durante sync input_odp/input_odp_runtime e scrittura log"
        )
        righe_inserite_odp = 0
        righe_aggiornate_odp = 0
        righe_inserite_runtime = 0
        righe_inserite_lotti = 0

    logging.info(
        "Sync input_odp completato | nuovi ERP=%s | aggiornati ERP=%s | nuovi runtime=%s | nuovi lotti=%s",
        righe_inserite_odp,
        righe_aggiornate_odp,
        righe_inserite_runtime,
        righe_inserite_lotti,
    )


# endregion
# region SCHEDULAZIONE


def _in_time_window(now_t: time, start: time, end: time) -> bool:
    """
    Calcola se il tempo attuale è in finestra lavorativa

    :param now_t: tempo attuale
    :type now_t: time
    :param start: inizio finestra lavorativa
    :type start: time
    :param end: fine finestra lavorativa
    :type end: time
    :return: tempo attuale in finestra lavorativa
    :rtype: bool
    """
    if start == end:
        return True  # finestra 24h
    if start < end:
        return start <= now_t < end
    # overnight: es. 18:00 -> 06:00
    return (now_t >= start) or (now_t < end)


def _is_allowed_datetime(
    now: datetime, start: time, end: time, allowed_weekdays: set[int]
) -> bool:
    """
    Calcola se il giorno attuale è in finestra lavorativa

    :param now: giorno attuale
    :type now: datetime
    :param start: giorno iniziale
    :type start: time
    :param end: giorno finale
    :type end: time
    :param allowed_weekdays: giorni settimanali lavorativi
    :type allowed_weekdays: set[int]
    :return: giorno attuale in finestra lavorativa
    :rtype: bool
    """
    if start < end or start == end:
        return (now.weekday() in allowed_weekdays) and _in_time_window(
            now.timetz().replace(tzinfo=None), start, end
        )

    now_t = now.timetz().replace(tzinfo=None)
    if now_t >= start:
        start_day = now.weekday()
    else:
        start_day = (now.weekday() - 1) % 7

    return (start_day in allowed_weekdays) and _in_time_window(now_t, start, end)


def seconds_until_next_allowed(
    start_h: int,
    end_h: int,
    allowed_weekdays: set[int],
    tz: ZoneInfo = ZoneInfo("Europe/Rome"),
    step_minutes: int = 1,
) -> int:
    """
    Ritorna 0 se siamo dentro la schedulazione.
    Altrimenti ritorna i secondi fino al prossimo istante consentito.
    Cerca in avanti con granularità step_minutes (default 1 minuto).

    :param start_h: ora inizio turno di lavoro
    :type start_h: int
    :param end_h: ora fine turno di lavoro
    :type end_h: int
    :param allowed_weekdays: giorni lavorativi permessi
    :type allowed_weekdays: set[int]
    :param tz: timezone
    :type tz: ZoneInfo
    :param step_minutes: minuti di step
    :type step_minutes: int
    :return: delta tempo in cui sopire il programma
    :rtype: int
    """
    if step_minutes <= 0:
        logging.warning("step_minutes=%s non valido: imposto a 1", step_minutes)
        step_minutes = 1

    start = time(start_h, 0)
    end = time(end_h, 0)

    now = datetime.now(tz)
    if _is_allowed_datetime(now, start, end, allowed_weekdays):
        return 0

    probe = now.replace(second=0, microsecond=0)
    limit = probe + timedelta(days=8)
    step = timedelta(minutes=step_minutes)

    while probe < limit:
        probe += step
        if _is_allowed_datetime(probe, start, end, allowed_weekdays):
            return int((probe - now).total_seconds())

    raise RuntimeError(
        "Impossibile trovare una prossima finestra: controlla parametri schedule."
    )


def wait_if_not_allowed(start_h: int, end_h: int, allowed_weekdays: set[int]) -> None:
    """
    Logger per l'output

    :param start_h: ora inizio turno
    :type start_h: int
    :param end_h: ora fine turno
    :type end_h: int
    :param allowed_weekdays: giorni lavorativi
    :type allowed_weekdays: set[int]
    """
    s = seconds_until_next_allowed(start_h, end_h, allowed_weekdays)
    if s > 0:
        logging.info(
            "Fuori schedule. Sleep ~%d min (fino a prossima finestra).", s // 60
        )
        time_mod.sleep(s)


def read_cycle() -> None:
    """
    funzione per elaborazione dati e calcolo del tempo di attività dell'intero programma con polling a n secondi
    """
    ensure_init()
    counter = 0
    logging.info("Inizio programma")
    list_elapsed = []
    list_sleep = []
    try:
        with Session(sqlite_engine_app) as session:
            while counter < 2:
                wait_if_not_allowed(START_H, END_H, ALLOWED_WEEKDAYS)
                start = time_mod.time()

                try:
                    elaborazione_dati(session=session)
                except Exception:
                    logging.exception("Errore generico")

                elapsed = time_mod.time() - start
                sleep_for = max(
                    0.0, float(POLL_SECONDS_DEFAULT) - elapsed
                )  # meglio senza int()
                logging.info(
                    "Ciclo %i completato in %.2f s. Sleep %.2fs",
                    counter,
                    elapsed,
                    sleep_for,
                )

                list_elapsed.append(elapsed)
                list_sleep.append(sleep_for)
                time_mod.sleep(sleep_for)
                counter += 1

            if counter % 1000 == 0:
                media_tempo_ciclo = statistics.mean(
                    list_elapsed[-1000 : len(list_elapsed)]
                )
                logging.info(
                    "Media tempo ciclo ultimi 1000 cicli %.3f", media_tempo_ciclo
                )
                media_riposo = statistics.mean(list_sleep[-1000 : len(list_sleep)])
                logging.info("Media tempo riposo 1000 cicli %.3f", media_riposo)
            elif counter % 500 == 0:
                media_tempo_ciclo = statistics.mean(
                    list_elapsed[-500 : len(list_elapsed)]
                )
                logging.info(
                    "Media tempo ciclo ultimi 500 cicli %.3f", media_tempo_ciclo
                )
                media_riposo = statistics.mean(list_sleep[-500 : len(list_sleep)])
                logging.info("Media tempo riposo 500 cicli %.3f", media_riposo)
            elif counter % 100 == 0:
                media_tempo_ciclo = statistics.mean(
                    list_elapsed[-100 : len(list_elapsed)]
                )
                logging.info(
                    "Media tempo ciclo ultimi 100 cicli %.3f", media_tempo_ciclo
                )
                media_riposo = statistics.mean(list_sleep[-100 : len(list_sleep)])
                logging.info("Media tempo riposo 100 cicli %.3f", media_riposo)

    except KeyboardInterrupt:
        if list_elapsed:
            logging.info("Media tempo ciclo %.5f", statistics.mean(list_elapsed))

        if list_sleep:
            logging.info("Media tempo riposo %.5f", statistics.mean(list_sleep))


if __name__ == "__main__":
    start_all = time_mod.time()
    read_cycle()
    end_all = time_mod.time()

    logging.info("Tempo di funzionamento %4.1f m", ((end_all - start_all) / 60))
# endregion
