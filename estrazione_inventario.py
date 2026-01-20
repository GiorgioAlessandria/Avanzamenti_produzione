from __future__ import annotations
from typing import Sequence
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
                                     colonna_rinominata: Optional[str] = "",
                                     how: Literal['left', 'right', 'outer',
                                                  'inner', 'cross', 'left_anti'] = "left"
                                     ) -> pd.DataFrame:
    ''''''
    ic(how)
    df_out = df.merge(
        df_di_merge[lista_colonne_da_inserire], on=colonna_merge, how=how)
    if colonna_da_rinominare and colonna_rinominata:
        df_out = df_out.rename(
            columns={colonna_da_rinominare: colonna_rinominata})
    return df_out


def _collapse_by_keys(df: pd.DataFrame, keys: Sequence[str], qty_col: str) -> pd.DataFrame:
    """
    Collassa righe duplicate su `keys`.
    - Somma qty_col
    - Per le altre colonne: somma i numerici, prende il primo valore per le non numeriche
    """
    df = df.copy()

    # Costruisci una mappa di aggregazione "sensata" in automatico
    agg: dict[str, str] = {}

    for col in df.columns:
        if col in keys:
            continue
        if col == qty_col:
            agg[col] = "sum"
        else:
            if pd.api.types.is_numeric_dtype(df[col]):
                agg[col] = "sum"
            else:
                agg[col] = "first"

    return df.groupby(list(keys), as_index=False).agg(agg)


def expand_giacenza_with_lotti(
    df_giacenza: pd.DataFrame,
    df_giacenza_lotti: pd.DataFrame,
    *,
    keys: Sequence[str] = ("CodArt", "CodMag"),
    lotto_col: str = "RifLottoAlfa",
    qty_col: str = "Giacenza",
) -> pd.DataFrame:
    """
    1) Collassa df_giacenza per chiavi (così diventa univoco a livello keys)
    2) Collassa df_giacenza_lotti per (keys + lotto) (così non hai doppioni lotto per stesso codice)
    3) Espande df_giacenza duplicando solo dove esistono lotti
    """
    df_g = df_giacenza.copy()
    df_l = df_giacenza_lotti.copy()

    # Pulizia tipica export Excel
    for df in (df_g, df_l):
        if "Unnamed: 0" in df.columns:
            df.drop(columns=["Unnamed: 0"], inplace=True)

    # Normalizza lotto (XXXX è valido: NON filtriamo niente)
    df_l[lotto_col] = df_l[lotto_col].astype(str).str.strip()

    # 1) Collassa Giacenza per chiavi (risolve l'errore del validate)
    df_g_u = _collapse_by_keys(df_g, keys=keys, qty_col=qty_col)

    # 2) Collassa Lotti per (keys + lotto) (evita aa 123 duplicato)
    df_l_u = (
        df_l.groupby([*keys, lotto_col], as_index=False)[qty_col]
        .sum()
    )

    # Identifica chiavi che hanno almeno un lotto
    keys_with_lots = df_l_u[list(keys)].drop_duplicates()

    # Righe senza lotti: mantieni una riga (collassata) e lotto = NA
    df_no_lots = (
        df_g_u.merge(keys_with_lots, on=list(keys), how="left", indicator=True)
        .query("_merge == 'left_only'")
        .drop(columns="_merge")
        .copy()
    )
    df_no_lots[lotto_col] = pd.NA

    # Righe con lotti: espandi (one -> many)
    df_with_lots = df_g_u.merge(
        df_l_u[[*keys, lotto_col, qty_col]],
        on=list(keys),
        how="inner",
        suffixes=("_tot", "_lot"),
        validate="one_to_many",
    )

    # Usa la quantità per-lotto come quantità finale
    df_with_lots[qty_col] = df_with_lots[f"{qty_col}_lot"]
    drop_cols = [c for c in (
        f"{qty_col}_tot", f"{qty_col}_lot") if c in df_with_lots.columns]
    df_with_lots.drop(columns=drop_cols, inplace=True)

    return pd.concat([df_with_lots, df_no_lots], ignore_index=True)


def elaborazione_dati() -> None:
    '''
    Funzione per la creazione della tabella input_odp da inserire a db

    Manca l'aggiunta del Codice magazzino da Odp a input_odp ma sarà aggiunto automaticamente
    '''
    df_giacenza = leggi_view(
        "vwESGiacenza").to_excel("excel/vwESGiacenza.xlsx")

    df_giacenzalotti = leggi_view(
        table="vwESGiacenzaLotti").to_excel("excel/vwESGiacenzaLotti.xlsx")
    df_giacenzatotale = (expand_giacenza_with_lotti(df_giacenza=leggi_view(table="vwESGiacenza"), df_giacenza_lotti=leggi_view(table="vwESGiacenzaLotti"), keys=("CodArt", "CodMag"), lotto_col="RifLottoAlfa")
                         .pipe(inserimento_descrizione_famiglia, df_di_merge=leggi_view("vwESArticoli"), colonna_merge="CodArt", lista_colonne_da_inserire=["CodArt", "DesArt", "CodFamiglia"])
                         .pipe(inserimento_descrizione_famiglia, df_di_merge=leggi_view("vwESFamiglia"), colonna_merge="CodFamiglia", lista_colonne_da_inserire=["CodFamiglia", "CodMacrofamiglia", "Des"], colonna_da_rinominare="Des", colonna_rinominata="DesFamiglia")
                         .pipe(inserimento_descrizione_famiglia, df_di_merge=leggi_view("vwESMacroFamiglia"), colonna_merge="CodMacrofamiglia", lista_colonne_da_inserire=["CodMacrofamiglia", "Des"], colonna_da_rinominare="Des", colonna_rinominata="DesMacroFamiglia"))
    df_giacenzatotale.to_excel("excel/giacenzatotale.xlsx", index=False)
    return
    df_risorse = leggi_view(
        "vwESRisorse").to_excel("excel/risorse.xlsx")
    df_articoli = (leggi_view("vwESArticoli").pipe(inserimento_descrizione_famiglia, df_di_merge=leggi_view("vwESFamiglia"), colonna_merge="CodFamiglia", lista_colonne_da_inserire=["CodFamiglia", "CodMacrofamiglia", "Des"], colonna_da_rinominare="Des", colonna_rinominata="DesFamiglia")
                   .pipe(inserimento_descrizione_famiglia, df_di_merge=leggi_view("vwESMacroFamiglia"), colonna_merge="CodMacrofamiglia", lista_colonne_da_inserire=["CodMacrofamiglia", "Des"], colonna_da_rinominare="Des", colonna_rinominata="DesMacroFamiglia")).to_excel("excel/articoli.xlsx")

    #
    # df_articoli.to_excel("excel/articoli.xlsx")

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
