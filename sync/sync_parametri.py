import logging
from typing import Optional
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import sqlite3 as sq
import tomllib
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from zoneinfo import ZoneInfo
from typing import Literal
import time as time_mod
import urllib.parse

from sqlalchemy import create_engine
try:
    from icecream import ic
except:
    pass

CONFIG_PATH = Path("app_odp//static//config.toml")


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


def leggi_view(
    table: Literal["vwESRisorse", "vwESOdP", "vwESOdPFasi", "vwESLavorazioni", "vwESOdPComponenti",
                   "vwESRisorse", "vwESReparti", "vwESCausaliAttivita", "vwESGiacenza", "vwESGiacenzaLotti",
                   "vwESArticoli", "vwESMagazzini", "vwESFamiglia", "vwESMacroFamiglia"],
    colonna_filtro_esclusi: Optional[str] = "",
    colonna_filtro_stato: Optional[str] = ""
) -> pd.DataFrame:
    '''
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


def inserisci_o_ignora(
    sqltable,
    conn,
    keys,
    data_iter
) -> None:
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
    Funzione per l'inserimento dei dati nella tabella input_odp da inserire a db
    '''
    df_famiglia = leggi_view(table="vwESFamiglia")
    df_famiglia = df_famiglia.rename(
        columns={"CodFamiglia": "Codice", "Des": "Descrizione"})
    df_famiglia = df_famiglia.drop(columns="CodMacrofamiglia")

    df_macrofamiglia = leggi_view(table="vwESMacroFamiglia")
    df_macrofamiglia = df_macrofamiglia.rename(
        columns={"CodMacrofamiglia": "Codice", "Des": "Descrizione"})

    df_risorse = leggi_view(table="vwESRisorse",
                            colonna_filtro_esclusi="CodRisorsaProd")
    df_risorse = df_risorse.rename(
        columns={"CodRisorsaProd": "Codice", "DesRisorsaProd": "Descrizione"})
    df_risorse = df_risorse.drop(columns="CodReparto")

    df_reparti = leggi_view(table="vwESReparti",
                            colonna_filtro_esclusi="CodReparto")
    df_reparti = df_reparti.rename(
        columns={"CodReparto": "Codice", "Des": "Descrizione"})

    df_lavorazioni = leggi_view(table="vwESLavorazioni",
                                colonna_filtro_esclusi="CodLavorazione")
    df_lavorazioni = df_lavorazioni.rename(
        columns={"CodLavorazione": "Codice", "DesLavorazione": "Descrizione"})

    df_magazzino = leggi_view(table="vwESMagazzini",
                              colonna_filtro_esclusi="CodMag")
    df_magazzino = df_magazzino.rename(
        columns={"CodMag": "Codice", "DesMagazzino": "Descrizione"})

    df_causali = leggi_view(table="vwESCausaliAttivita")

    try:
        df_famiglia.to_sql(name="famiglia",
                           con=engine_app,
                           if_exists='append',
                           index=False,
                           method=inserisci_o_ignora)
        df_macrofamiglia.to_sql(name="macrofamiglia",
                                con=engine_app,
                                if_exists='append',
                                index=False,
                                method=inserisci_o_ignora)
        df_risorse.to_sql(name="risorse",
                          con=engine_app,
                          if_exists='append',
                          index=False,
                          method=inserisci_o_ignora)
        df_reparti.to_sql(name="reparti",
                          con=engine_app,
                          if_exists='append',
                          index=False,
                          method=inserisci_o_ignora)
        df_lavorazioni.to_sql(name="lavorazioni",
                              con=engine_app,
                              if_exists='append',
                              index=False,
                              method=inserisci_o_ignora)
        df_magazzino.to_sql(name="magazzini",
                            con=engine_app,
                            if_exists='append',
                            index=False,
                            method=inserisci_o_ignora)
        df_causali.to_sql(name="causaliattivita",
                          con=engine_app,
                          if_exists='append',
                          index=False,
                          method=inserisci_o_ignora)
    except sq.IntegrityError:
        print("Tutte le celle sono uguali")


if __name__ == "__main__":
    start_all = time_mod.time()
    elaborazione_dati()
    end_all = time_mod.time()
    print(f"Tempo di funzionamento {(end_all - start_all):4.1f} s")
