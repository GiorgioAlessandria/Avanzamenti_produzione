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

CONFIG_PATH = Path("static//filtri_sync.toml")


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


def leggi_view(table: Literal["vwESRisorse", "vwESOdP", "vwESOdPFasi", "vwESLavorazioni", "vwESOdPComponenti",
                              "vwESRisorse", "vwESReparti", "vwESCausaliAttivita", "vwESGiacenza", "vwESGiacenzaLotti",
                              "vwESArticoli", "vwESMagazzini", "vwESFamiglia", "vwESMacroFamiglia"],
               colonna_filtro_esclusi: Optional[str] = "",
               colonna_filtro_stato: Optional[str] = ""
               ) -> pd.DataFrame:
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


def inserimento_descrizione_famiglia(df: pd.DataFrame,
                                     df_di_merge: pd.DataFrame,
                                     colonna_merge: str | list[str],
                                     lista_colonne_da_inserire: list[str],
                                     colonna_da_rinominare: Optional[str] = "",
                                     colonna_rinominata: Optional[str] = "") -> pd.DataFrame:
    ''''''
    df_out = df.merge(
        df_di_merge[lista_colonne_da_inserire], on=colonna_merge, how="left")
    if colonna_da_rinominare and colonna_rinominata:
        df_out = df_out.rename(
            columns={colonna_da_rinominare: colonna_rinominata})
    return df_out


def elaborazione_dati() -> None:
    '''
    Funzione per la creazione della tabella input_odp da inserire a db

    Manca l'aggiunta del Codice magazzino da Odp a input_odp ma sarà aggiunto automaticamente
    '''

    df_giacenza = leggi_view(
        "vwESGiacenza")

    df_giacenzalotti = leggi_view(
        table="vwESGiacenzaLotti")

    df_giacenzatotale = (pd.concat(
        [df_giacenza, df_giacenzalotti], ignore_index=True, sort=False).reset_index()
        .pipe(inserimento_descrizione_famiglia, df_di_merge=leggi_view("vwESArticoli"), colonna_merge="CodArt", lista_colonne_da_inserire=["CodArt", "DesArt", "CodFamiglia"])
        .pipe(inserimento_descrizione_famiglia, df_di_merge=leggi_view("vwESFamiglia"), colonna_merge="CodFamiglia", lista_colonne_da_inserire=["CodFamiglia", "CodMacrofamiglia", "Des"], colonna_da_rinominare="Des", colonna_rinominata="DesFamiglia")
        .pipe(inserimento_descrizione_famiglia, df_di_merge=leggi_view("vwESMacroFamiglia"), colonna_merge="CodMacrofamiglia", lista_colonne_da_inserire=["CodMacrofamiglia", "Des"], colonna_da_rinominare="Des", colonna_rinominata="DesMacroFamiglia"))
    df_giacenzatotale.to_excel("excel/giacenzatotale.xlsx")

    df_articoli = (leggi_view("vwESArticoli").pipe(inserimento_descrizione_famiglia, df_di_merge=leggi_view("vwESFamiglia"), colonna_merge="CodFamiglia", lista_colonne_da_inserire=["CodFamiglia", "CodMacrofamiglia", "Des"], colonna_da_rinominare="Des", colonna_rinominata="DesFamiglia")
                   .pipe(inserimento_descrizione_famiglia, df_di_merge=leggi_view("vwESMacroFamiglia"), colonna_merge="CodMacrofamiglia", lista_colonne_da_inserire=["CodMacrofamiglia", "Des"], colonna_da_rinominare="Des", colonna_rinominata="DesMacroFamiglia")).to_excel("excel/articoli.xlsx")
    #
    df_giacenzatotale.to_excel("excel/giacenzatotale.xlsx")

    ic(df_giacenzatotale)

    try:
        pass
    except sq.IntegrityError:
        print("Tutte le celle sono uguali")


def read_cycle():
    elaborazione_dati()


if __name__ == "__main__":
    start_all = time_mod.time()
    read_cycle()
    end_all = time_mod.time()
    print(f"Tempo di funzionamento {(end_all - start_all):4.1f} s")
