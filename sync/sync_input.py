# region LIBRERIE
"""
Programma per l'acquisizione dei dati dalle view di produzione verso il db
"""

import logging
import statistics
from typing import Optional, Literal
import json
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
from app_odp.models import ChangeEvent

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

COUNTER_RIGHE = 0
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


def init(config_path: str | pathlib.Path = None, *, force: bool = False):
    """
    Inizializza configurazione + connessioni DB + parametri globali.
    - config_path: percorso del config.toml
    - force: se True reinizializza anche se era già stato fatto
    """
    global CONFIG, config, sqlite_engine_app, sqlserver_engine_app
    global COUNTER_RIGHE, ALLOWED_WEEKDAYS, START_H, END_H, TIMEZONE, POLL_SECONDS_DEFAULT
    global ELEMENTI_ESCLUSI, ELEMENTI_SELEZIONATI
    global _INITIALIZED

    if _INITIALIZED and not force:
        return

    if config_path is None:
        # default vicino al modulo (o dove vuoi)
        config_path = Path("app_odp//static//config.toml")

    CONFIG = load_config(config_path)

    # estrazione campi
    percorsi = CONFIG["Percorsi"]
    sync_cfg = CONFIG["sync_config"]
    ELEMENTI_ESCLUSI = CONFIG["Elementi_esclusi"]
    ELEMENTI_SELEZIONATI = CONFIG["Elementi_selezionati"]

    COUNTER_RIGHE = int(sync_cfg.get("counter_righe", 0))
    ALLOWED_WEEKDAYS = set(sync_cfg["giorni_settimanali"])
    START_H = int(sync_cfg["ora_inizio"])
    END_H = int(sync_cfg["ora_fine"])
    TIMEZONE = sync_cfg.get("time_zone", "Europe/Rome")
    POLL_SECONDS_DEFAULT = float(sync_cfg.get("tempo_polling", 5))
    params = urllib.parse.quote_plus(sync_cfg.get("sql_params"))

    # Engine
    sqlite_engine_app = create_engine(f"sqlite:///{percorsi['percorso_db']}")
    if sqlserver_engine_app is None:
        sqlserver_engine_app = create_engine("mssql+pyodbc:///?odbc_connect=" + params)

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
    df_fasi: pd.DataFrame, df_componenti: pd.DataFrame
) -> pd.DataFrame:
    """
    Join tra il df delle fasi e quello dei componenti per fase. Al df_componenti vengono rinominate le righe IdRigaPadre e IdRiga rispettivamente in IdRiga e IdRigacomponente

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

    df = df.set_index(chiavi)
    if data_in == "data":
        df[rename_col] = df[rename_col].dt.strftime("%d/%m/%Y %H:%M:%S")
    else:
        pass
    componenti_per_odp = (
        df.groupby(chiavi)
        .apply((lambda g: g[list_columns].to_dict("records")))
        .rename(rename_col)
        .reset_index()
    )
    componenti_per_odp[rename_col] = componenti_per_odp[rename_col].apply(
        lambda x: json.dumps(x) if isinstance(x, (list, tuple)) else None
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

    df_dizionari = [
        num_fase_per_odp,
        cod_lavorazione_per_odp,
        cod_risorsa_prod_per_odp,
        cod_reparto_per_odp,
        data_inizio_sched_per_odp,
        data_fine_sched_per_odp,
        tempo_previsto_lavoraz_per_odp,
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


def _chunked(seq, size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _fetch_existing_pks(
    engine, pk_tuples, pk_cols=("IdDocumento", "IdRiga")
) -> set[tuple]:
    """
    Ritorna un set di PK (IdDocumento, IdRiga) già presenti in input_odp.
    Chunking per evitare limiti di parametri SQLite.
    """
    if not pk_tuples:
        return set()

    md = sa.MetaData()
    t = sa.Table("input_odp", md, autoload_with=engine)
    tpl = sa.tuple_(t.c[pk_cols[0]], t.c[pk_cols[1]])

    existing = set()
    # chunk conservativo (SQLite ha limiti, 400-500 tuple è ok in genere)
    for chunk in _chunked(pk_tuples, 400):
        q = sa.select(t.c[pk_cols[0]], t.c[pk_cols[1]]).where(tpl.in_(chunk))
        with engine.connect() as conn:
            existing.update(conn.execute(q).fetchall())
    return existing


def elaborazione_dati(session: Session) -> None:
    """
    Funzione per l'inserimento dei dati nella tabella input_odp da inserire a db

    :param session: sessione Sqlalchemy ORM
    :type session: Session
    """
    ensure_init()
    global COUNTER_RIGHE
    # righe_inserite = int(0)
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
    df_fasi_componenti = unione_fasi_componenti(df_odpfasi, df_odpcomponenti)
    distinta_componenti = generazione_dizionario(
        df=df_fasi_componenti,
        chiavi=chiavi,
        rename_col="DistintaMateriale",
        list_columns=["CodArt", "Quantita", "NumFase"],
    )
    df_articoli = leggi_view("vwESArticoli")
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
    pk_cols = ["IdDocumento", "IdRiga"]

    # 1) dedup PK nel batch (e normalizza tipo)
    df_pk = df_input_odp[pk_cols].astype(str).drop_duplicates()
    pk_tuples = list(map(tuple, df_pk.to_numpy()))

    # 2) chiavi già presenti
    existing = _fetch_existing_pks(sqlite_engine_app, pk_tuples, tuple(pk_cols))

    # 3) filtra nuove righe + dedup finale PK
    mask_new = [
        tuple(x) not in existing for x in df_input_odp[pk_cols].astype(str).to_numpy()
    ]
    df_new = df_input_odp.loc[mask_new].copy().drop_duplicates(subset=pk_cols)
    df_new["FaseAttiva"] = 1

    # 4) nuovo_ciclo deve essere PRIMA (quando counter==0)
    if COUNTER_RIGHE == 0:
        emit_event(session=session, topic="nuovo_ciclo")

    # 5) inserimento (None->0) + catch IntegrityError
    try:
        righe_inserite = int(
            df_new.to_sql(
                name="input_odp",
                con=sqlite_engine_app,
                if_exists="append",
                index=False,
                method=inserisci_o_ignora,
            )
            or 0
        )
    except sq.IntegrityError:
        print("Tutte le celle sono uguali")
        righe_inserite = 0

    # 6) nuovo_ordine SOLO se cambia rispetto al counter
    if (righe_inserite != 0) and (righe_inserite != COUNTER_RIGHE):
        df_ev = df_new.sort_values(["IdDocumento", "IdRiga"], ascending=False).head(
            righe_inserite
        )
        payload = (
            df_ev["IdDocumento"].astype(str) + "," + df_ev["IdRiga"].astype(str)
        ).tolist()
        scope = df_ev["CodReparto"].tolist()
        emit_event(
            session=session,
            topic="nuovo_ordine",
            scope=json.dumps(scope),
            payload_json=json.dumps(payload),
        )

    COUNTER_RIGHE = righe_inserite


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


def emit_event(
    session, topic: str, scope: str | None = None, payload_json: str | None = None
) -> None:
    """
    Inserisce a db una stringa per identificare l'inserimento di un nuovo odp

    :param session: Session database
    :param topic: tipologia evento
    :type topic: str
    :param scope: non utilizzato
    :type scope: str | None
    :param payload_json: descrizione evento
    :type payload_json: str | None
    """
    try:
        session.add(ChangeEvent(topic=topic, scope=scope, payload_json=payload_json))
    except:
        session.rollback()
    else:
        session.commit()



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
            while True:
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


def server_system_event():
    pass
    # endregion
    # region MAIN


if __name__ == "__main__":
    start_all = time_mod.time()
    read_cycle()
    end_all = time_mod.time()

    logging.info("Tempo di funzionamento %4.1f m", ((end_all - start_all) / 60))
# endregion
