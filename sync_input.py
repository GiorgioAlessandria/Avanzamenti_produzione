import logging
from typing import Optional, Literal
import json
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import sqlite3 as sq
import tomllib
import time
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import functools as ft
from zoneinfo import ZoneInfo
from datetime import datetime, time, timedelta

import time as time_mod
import urllib.parse

from sqlalchemy import create_engine
try:
    from icecream import ic
except:
    pass

ALLOWED_WEEKDAYS = {0, 1, 2, 3, 4, 5}
START_H = 7
END_H = 18
CONFIG_PATH = Path("static//filtri_sync.toml")
TZ = ZoneInfo("Europe/Rome")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def load_config() -> dict:
    '''
    Caricamento e lettura file configurazioni

    :return: Ritorna un dizionario con le configurazioni
    :rtype: dict[Any, Any]
    '''
    with CONFIG_PATH.open("rb") as f:
        return tomllib.load(f)


config = load_config()

engine_app = create_engine(
    "sqlite:///\\\\Serverspring02\\PythonDB\\Avanzamenti_produzione\\instance\\RBAC.db")

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=SERVERSPRING02;"
    "DATABASE=BernardiProd;"
    "UID=Produzione;"
    "PWD=Produzione2025.;"
    "Encrypt=no;"
    "TrustServerCertificate=no;"
    "Pooling=no;"
    "MultipleActiveResultSets=False;"
)

engine_sqlserver = create_engine(
    "mssql+pyodbc:///?odbc_connect=" + params
)


def leggi_view(table: Literal["vwESRisorse", "vwESOdP", "vwESOdPFasi", "vwESLavorazioni", "vwESOdPComponenti", "vwESRisorse", "vwESReparti", "vwESCausaliAttivita", "vwESGiacenza", "vwESGiacenzaLotti", "vwESArticoli", "vwESMagazzini", "vwESFamiglia", "vwESMacroFamiglia"], colonna_filtro_esclusi: Optional[str] = "", colonna_filtro_stato: Optional[str] = "") -> pd.DataFrame:
    '''
    Lettura della view

    Legge la view in base ai parametri e da la view filtrata

    :param table: Nome della tabella
    :type table: str
    :param colonna_filtro_esclusi: Nome del filtro da richiamare (filtri di esclusione)
    :type colonna_filtro_esclusi: str
    :param colonna_filtro_stato: Nome del filtro da richiamare (filtri di inclusione)
    :type colonna_filtro_stato: str
    :return: Dataframe filtrato della view selezionata
    :rtype: DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.{table}"""
    df = pd.read_sql(query, engine_sqlserver)
    if colonna_filtro_esclusi != "":
        df = df[~df[colonna_filtro_esclusi].isin(
            config["Elementi_esclusi"][colonna_filtro_esclusi])]
        df = df.dropna(subset=[colonna_filtro_esclusi], how='any')
    if colonna_filtro_stato != "":
        df = df[df[colonna_filtro_stato] ==
                config["Elementi_selezionati"][colonna_filtro_stato]]
        df = df.dropna(subset=[colonna_filtro_esclusi], how='any')

    df = df.reset_index(drop=True)

    return df


def filtra_odpfasi_con_odp(df_odpfasi: pd.DataFrame, df_odp: pd.DataFrame) -> pd.DataFrame:
    '''
    Incrocio dei dati per mantenere le linee di df_odpfasi che corrispondono a [IdDocumento, IdRiga] di df_odp

    :param df_odpfasi: dataframe con le fasi degli ordini di produzione
    :type df_odpfasi: pd.DataFrame
    :param df_odp: dataframe con gli ordini di produzione
    :type df_odp: pd.DataFrame
    :return: DataFrame filtrato per IdDocumento e IdRiga di df_odp
    :rtype: pd.DataFrame
    '''
    df_odpfasi_filtered = df_odpfasi.merge(df_odp[["IdDocumento", "IdRiga"]], on=[
                                           "IdDocumento", "IdRiga"], how='right')
    return df_odpfasi_filtered


def filtra_odpcomponenti_con_odp(df_odpcomponenti: pd.DataFrame, df_odp: pd.DataFrame) -> pd.DataFrame:
    '''
    Incrocio dei dati per mantenere le linee di df_odpcomponenti che si trovano in IdDocumento e IdRiga di df_odp.
    Il filtro su df_odpcomponenti è ["IdDocumento", "IdRigaPadre"]

    :rtype: pd.DataFrame
    :param df_odpcomponenti: dataframe con i componenti in base agli ordini di produzione
    :type df_odpcomponenti: pd.DataFrame
    :param df_odp: dataframe con gli ordini di produzione
    :type df_odp: pd.DataFrame
    :return: DataFrame filtrato IdDocumento e IdRiga di df_odp
    :rtype: pd.DataFrame
    '''
    df_odpcomponenti_filtered = df_odpcomponenti.merge(df_odp[["IdDocumento", "IdRiga"]], left_on=[
        "IdDocumento", "IdRigaPadre"], how='right', right_on=["IdDocumento", "IdRiga"], suffixes=["", "_y"])
    df_odpcomponenti_filtered = df_odpcomponenti_filtered.drop(
        columns=["IdRiga_y"])
    return df_odpcomponenti_filtered


def inserimento_reparto_da_risorsa(df_odpfasi: pd.DataFrame, df_risorse: pd.DataFrame) -> pd.DataFrame:
    '''
    Inserimento del reparto in base alla risorsa richiamata

    :param df_odpfasi: dataframe con le fasi degli ordini di produzione
    :type df_odpfasi: pd.DataFrame
    :param df_risorse: dataframe con risorse e reparti associati
    :type df_risorse: pd.DataFrame
    :return: Dataframe con i reparti in CodReparto associati al codice risorsa
    :rtype: pd.DataFrame
    '''
    df_odpfasi_reparti = df_odpfasi.merge(df_risorse[["CodRisorsaProd", "CodReparto"]], on=[
        "CodRisorsaProd"], how='left')
    df_odpfasi_reparti = df_odpfasi_reparti.dropna(
        subset="CodReparto", how='any')
    return df_odpfasi_reparti


def unione_fasi_componenti(df_fasi: pd.DataFrame, df_componenti: pd.DataFrame) -> pd.DataFrame:
    '''
    Join tra il df delle fasi e quello dei componenti per fase. Al df_componenti vengono rinominate le righe IdRigaPadre e IdRiga rispettivamente in IdRiga e IdRigacomponente

    :param df_fasi: dataframe fasi
    :type df_fasi: pd.DataFrame
    :param df_componenti: dataframe componenti per fase
    :type df_componenti: pd.DataFrame
    :return: Dataframe composto dai codici articolo (CodArt) divisi per fase con le quantità di materiale necessario a codice per fase
    :rtype: pd.DataFrame
    '''
    df_componenti = df_componenti.rename(
        columns={"IdRiga": "IdRigacomponente", "IdRigaPadre": "IdRiga"})
    fasi_indexed = df_fasi.set_index(
        ["IdDocumento", "IdRiga", "NumFase"], drop=True)
    comp_indexed = df_componenti.set_index(
        ["IdDocumento", "IdRiga", "NumFase"], drop=True)
    df_fasi_componenti = fasi_indexed.join(comp_indexed, on=[
        "IdDocumento", "IdRiga", "NumFase"], validate="1:m", how="left", lsuffix="l_")
    df_fasi_componenti = df_fasi_componenti.reset_index(drop=False)
    return df_fasi_componenti


def generazione_dizionario(df: pd.DataFrame, CHIAVI: list[str], rename_col: str, list_columns: list[str], data_in='normale') -> pd.DataFrame:
    '''
    Genera un dizionario raggruppando le chiavi

    Raggruppa in base alla costante CHIAVI (impostata nella funzione madre) e crea una lista di elementi che verrà inserita con il nome della colonna rename_col

    :param df: Dataframe di input
    :type df: pd.DataFrame
    :param CHIAVI: elenco delle colonne da raggruppare
    :type CHIAVI: list[str]
    :param rename_col: Nome della colonna finale
    :type rename_col: str
    :param list_columns: Lista con le colonne da raggruppare
    :type list_columns: list[str]
    :return: Dataframe raggruppati per Codice, descrizione e quantità
    :rtype: pd.DataFrame
    '''

    df = df.set_index(CHIAVI)
    if data_in == 'data':
        df[rename_col] = df[rename_col].dt.strftime(
            '%d/%m/%Y %H:%M:%S')
    else:
        pass
    componenti_per_odp = (
        df
        .groupby(CHIAVI)
        .apply(
            (lambda g: g[list_columns]
             .to_dict("records")))
        .rename(rename_col)
        .reset_index()
    )
    componenti_per_odp[rename_col] = componenti_per_odp[rename_col].apply(lambda x: json.dumps(
        x) if isinstance(x, (list, tuple)) else None)
    return componenti_per_odp


def inserimento_distinta_in_odp(df_odp: pd.DataFrame, componenti_per_odp: pd.DataFrame, CHIAVI: list[str]) -> pd.DataFrame:
    '''
    Inserimento della distinta nella linea d'ordine

    Inserisce nella linea d'ordine la distinta per l'ordine di produzione. rimuove le colonne non necessarie al database di ingresso

    :param df_odp: Dataframe degli ordini di produzione
    :type df_odp: pd.DataFrame
    :param componenti_per_odp: Dataframe con la distinta base
    :type componenti_per_odp: pd.DataFrame
    :param CHIAVI: elenco delle colonne su cui unire i due df
    :type CHIAVI: list[str]
    :return: dataframe con gli ordini e la distinta
    :rtype: pd.DataFrame
    '''
    df_odp = df_odp.merge(componenti_per_odp, on=CHIAVI, how="left")
    df_odp = df_odp.drop(columns=[
                         "CodTipoDoc", "NumRegistraz", "DataRegistrazione", "UnitaMisura", "QtaResidua"])
    return df_odp


def inserimento_dati_fasi_in_odp(df_odp: pd.DataFrame, df_odpfasi: pd.DataFrame, CHIAVI: list[str]) -> pd.DataFrame:
    '''
    Inserimento dei dati divisi per fase

    Inserisce i dati delle fasi divisi in dizionario, in questo modo posso avere l'elenco delle fasi e la divisione dei vari componenti nelle fasi

    :param df_odp: Dataframe degli ordini di produzione
    :type df_odp: pd.DataFrame
    :param df_odpfasi: Dataframe con gli ordini di produzione con le fasi
    :type df_odpfasi: pd.DataFrame
    :param CHIAVI: Chiavi su cui vengono filtrati i dataframe
    :type CHIAVI: list[str]
    :return: dataframe con l'elenco delle fasi, i codici lavorazione, i codici risorsa, i codici reparto,
    data inizio e fine schedulazione ed il tempo previsto di lavorazione divisi per fasi
    :rtype: DataFrame
    '''

    numFase_per_odp = generazione_dizionario(
        df=df_odpfasi, CHIAVI=CHIAVI, rename_col="NumFase", list_columns=["NumFase"]).set_index(["IdDocumento", "IdRiga"])

    codlavorazione_per_odp = generazione_dizionario(
        df=df_odpfasi, CHIAVI=CHIAVI, rename_col="CodLavorazione", list_columns=["CodLavorazione"]).set_index(["IdDocumento", "IdRiga"])

    codRisorsaProd_per_odp = generazione_dizionario(
        df=df_odpfasi, CHIAVI=CHIAVI, rename_col="CodRisorsaProd", list_columns=["CodRisorsaProd"]).set_index(["IdDocumento", "IdRiga"])

    codReparto_per_odp = generazione_dizionario(
        df=df_odpfasi, CHIAVI=CHIAVI, rename_col="CodReparto", list_columns=["CodReparto"]).set_index(["IdDocumento", "IdRiga"])

    dataInizioSched_per_odp = generazione_dizionario(
        df=df_odpfasi, CHIAVI=CHIAVI, rename_col="DataInizioSched", list_columns=["DataInizioSched"], data_in="data").set_index(["IdDocumento", "IdRiga"])

    dataFineSched_per_odp = generazione_dizionario(
        df=df_odpfasi, CHIAVI=CHIAVI, rename_col="DataFineSched", list_columns=["DataFineSched"], data_in="data").set_index(["IdDocumento", "IdRiga"])

    tempoPrevistoLavoraz_per_odp = generazione_dizionario(
        df=df_odpfasi, CHIAVI=CHIAVI, rename_col="TempoPrevistoLavoraz", list_columns=["TempoPrevistoLavoraz"]).set_index(["IdDocumento", "IdRiga"])

    df_dizionari = [numFase_per_odp, codlavorazione_per_odp, codRisorsaProd_per_odp,
                    codReparto_per_odp, dataInizioSched_per_odp, dataFineSched_per_odp, tempoPrevistoLavoraz_per_odp]
    df_fasi_raggruppate = ft.reduce(lambda left, right: pd.merge(
        left, right, on=CHIAVI), df_dizionari)

    df_odp = df_odp.set_index(["IdDocumento", "IdRiga"])
    df_odp = df_odp.join(
        df_fasi_raggruppate, how="left")
    df_odp = df_odp.reset_index(drop=False)
    return df_odp


def gestione_lotto_matricola_famiglia(df_odp: pd.DataFrame, df_articoli: pd.DataFrame) -> pd.DataFrame:
    '''
    Inserimento nel dataframe la gestione per lotto, matricola e la famiglia

    Inserisce i dati per identificare se il codice deve essere gestito per lotto e/o con una matricola, inoltre inserisce anche la famiglia di appartenenza

    :param df_odp: Dataframe con gli ordini di produzione
    :type df_odp: pd.DataFrame
    :param df_articoli: Dataframe con l'anagrafica degli articoli
    :type df_articoli: pd.DataFrame
    :return: Dataframe degli ordini arricchito con la gestione lotto, matricola e la famiglia di appartenenza
    :rtype: DataFrame
    '''
    df_odp = df_odp.merge(
        df_articoli[["CodArt", "GestioneLotto", "GestioneMatricola", "CodFamiglia"]], on="CodArt", how='left')
    df_odp = df_odp.dropna(subset=[
        "GestioneLotto", "GestioneMatricola", "CodFamiglia"])
    return df_odp


def inserimento_macrofamiglia(df_odp: pd.DataFrame, df_famiglia: pd.DataFrame) -> pd.DataFrame:
    '''
    Inserimento nel dataframe la macrofamiglia di appartenenza

    Inserisce la macrofamiglia di appartenenza all'ordine di produzione

    :param df_odp: Dataframe con gli ordini di produzione
    :type df_odp: pd.DataFrame
    :param df_famiglia: Dataframe con l'elenco delle macrofamiglie e famiglie associate
    :type df_macrofamiglia: pd.DataFrame
    :return: Dataframe con l'elenco degli ordini arricchito con la macrofamiglia
    :rtype: DataFrame
    '''
    df_odp = df_odp.merge(df_famiglia[["CodFamiglia", "CodMacrofamiglia"]], on=[
                          "CodFamiglia"], how='left')
    df_odp = df_odp.dropna(subset=["CodMacrofamiglia"])
    return df_odp


def inserisci_o_ignora(sqltable, conn, keys, data_iter):
    """
    Inserimento delle righe a db se non già presenti altrimenti ignora

    :param sqltable: table sql
    :param conn: connessione al db
    :param keys: colonne della table
    :param data_iter:

    """
    # sqltable è un pandas.io.sql.SQLTable, prendo la vera Table SQLAlchemy:
    table = sqltable.table

    # Converto l'iteratore di righe in lista di dict
    rows = list(data_iter)
    if not rows:
        return

    data = [dict(zip(keys, row)) for row in rows]

    # Costruisco l'INSERT per SQLite
    stmt = sqlite_insert(table).values(data)
    # Aggiungo la parte "OR IGNORE"
    stmt = stmt.prefix_with("OR IGNORE")

    conn.execute(stmt)


def elaborazione_dati() -> None:
    '''
    Funzione per la creazione della tabella input_odp da inserire a db

    Manca l'aggiunta del Codice magazzino da Odp a input_odp ma sarà aggiunto automaticamente
    '''
    # pd.DataFrame(leggi_view_odp()).to_excel("excel//odp.xlsx")
    df_odp = leggi_view(
        table="vwESOdP", colonna_filtro_esclusi="CodArt", colonna_filtro_stato="StatoOrdine")
    df_odpfasi = (pd.DataFrame(leggi_view(table="vwESOdPFasi", colonna_filtro_esclusi="CodRisorsaProd"))
                  .pipe(filtra_odpfasi_con_odp, df_odp=df_odp)
                  .pipe(inserimento_reparto_da_risorsa, df_risorse=leggi_view("vwESRisorse", colonna_filtro_esclusi="CodRisorsaProd")))
    CHIAVI = ["IdDocumento", "IdRiga"]
    # df_odpfasi.to_excel("excel//odpfasi.xlsx")
    df_odpcomponenti = (leggi_view("vwESOdPComponenti")
                        .pipe(filtra_odpcomponenti_con_odp, df_odp=df_odp))
    # df_odpcomponenti.to_excel("excel//odpcomponenti.xlsx")
    df_fasi_componenti = unione_fasi_componenti(df_odpfasi, df_odpcomponenti)
    distinta_componenti = generazione_dizionario(
        df=df_fasi_componenti, CHIAVI=CHIAVI, rename_col="DistintaMateriale", list_columns=["CodArt", "Quantita", "NumFase"])
    df_articoli = leggi_view("vwESArticoli")
    input_odp = (inserimento_distinta_in_odp(df_odp=df_odp, componenti_per_odp=distinta_componenti, CHIAVI=CHIAVI)
                 .pipe(inserimento_dati_fasi_in_odp, df_odpfasi=df_odpfasi, CHIAVI=CHIAVI)
                 .pipe(gestione_lotto_matricola_famiglia, df_articoli=df_articoli)
                 .pipe(inserimento_macrofamiglia, df_famiglia=leggi_view("vwESFamiglia", colonna_filtro_esclusi="CodFamiglia"))
                 .drop(columns=["DataInizioProduzione"]))
    try:
        input_odp.to_sql(name="input_odp",
                         con=engine_app,
                         if_exists='append',
                         index=False,
                         method=inserisci_o_ignora)
    except sq.IntegrityError:
        print("Tutte le celle sono uguali")


def _in_time_window(now_t: time, start: time, end: time) -> bool:
    """Gestisce anche finestre overnight (start > end)."""
    if start == end:
        return True  # finestra 24h
    if start < end:
        return start <= now_t < end
    # overnight: es. 18:00 -> 06:00
    return (now_t >= start) or (now_t < end)


def _is_allowed_datetime(now: datetime, start: time, end: time, allowed_weekdays: set[int]) -> bool:
    # Caso non-overnight: basta che oggi sia allowed.
    if start < end or start == end:
        return (now.weekday() in allowed_weekdays) and _in_time_window(now.timetz().replace(tzinfo=None), start, end)

    # Caso overnight: 18->06
    # - Se sono tra 18:00 e 23:59, conta il "giorno di inizio" (oggi)
    # - Se sono tra 00:00 e 05:59, conta il "giorno di inizio" (ieri)
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
    tz: ZoneInfo = TZ,
    step_minutes: int = 1

) -> int:
    """
    Ritorna 0 se siamo dentro la schedulazione.
    Altrimenti ritorna i secondi fino al prossimo istante consentito.
    Cerca in avanti con granularità step_minutes (default 1 minuto).
    """
    start = time(start_h, 0)
    end = time(end_h, 0)

    now = datetime.now(tz)
    if _is_allowed_datetime(now, start, end, allowed_weekdays):
        return 0

    # Scansione in avanti (max ~ 7 giorni) a step di 1 minuto: semplice e robusta.
    probe = now.replace(second=0, microsecond=0)
    limit = probe + timedelta(days=8)
    step = timedelta(minutes=step_minutes)

    while probe < limit:
        probe += step
        if _is_allowed_datetime(probe, start, end, allowed_weekdays):
            return int((probe - now).total_seconds())

    raise RuntimeError(
        "Impossibile trovare una prossima finestra: controlla parametri schedule.")


def wait_if_not_allowed(start_h: int, end_h: int, allowed_weekdays: set[int]) -> None:
    s = seconds_until_next_allowed(start_h, end_h, allowed_weekdays)
    if s > 0:
        logging.info(
            "Fuori schedule. Sleep ~%d min (fino a prossima finestra).", s // 60)
        time_mod.sleep(s)


def read_cycle(poll_seconds: int = 30):
    counter = 1
    while True:
        wait_if_not_allowed(START_H, END_H, ALLOWED_WEEKDAYS)
        start = time_mod.time()

        try:
            elaborazione_dati()
        except Exception as e:
            logging.exception("Errore generico")
        elapsed = time_mod.time() - start
        sleep_for = max(0, poll_seconds - elapsed)
        logging.info("Ciclo %i completato in %.2f s. Sleep %.2fs",
                     counter, elapsed, sleep_for)
        time_mod.sleep(sleep_for)
        counter += 1


if __name__ == "__main__":
    start_all = time_mod.time()
    read_cycle()
    end_all = time_mod.time()
    print(f"Tempo di funzionamento {(end_all - start_all):4.1f} s")
