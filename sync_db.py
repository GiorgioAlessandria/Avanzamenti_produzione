import json
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import sqlite3 as sq
import tomllib
import time
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import functools as ft
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


def leggi_view(table: str, colonna_filtro_esclusi: str, colonna_filtro_stato: str) -> pd.DataFrame:
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


def drop_na(df: pd.DataFrame, colonna: str | list[str]) -> pd.DataFrame:
    '''
    Rimozione NaN dalla colonna o lista di colonne del dataframe in ingresso

    :param df: Dataframe input
    :type df: pd.DataFrame
    :param colonna: Nome o lista di nomi delle colonne da controllare
    :type colonna: str | list[str]
    :return: Dataframe senza Nan
    :rtype: DataFrame
    '''
    df = df.dropna(subset=colonna, how='any')
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
    df_odpfasi_reparti = drop_na(df=df_odpfasi_reparti, colonna="CodReparto")

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
    df_odp = drop_na(df=df_odp, colonna=[
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
    df_odp = drop_na(df=df_odp, colonna=[
                     "CodMacrofamiglia"])
    return df_odp


def inserisci_o_ignora(sqltable, conn, keys, data_iter):
    """
    Usato da pandas.to_sql per eseguire INSERT OR IGNORE.
    Richiede che la tabella abbia una PRIMARY KEY o UNIQUE constraint
    sulle colonne che definiscono l'univocità (es. IdDocumento, IdRiga).
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


def elaborazione_dati():
    '''
    Funzione per la creazione della tabella input_odp da inserire a db

    Manca l'aggiunta del Codice magazzino da Odp a input_odp ma sarà aggiunto automaticamente
    '''
    # pd.DataFrame(leggi_view_odp()).to_excel("excel//odp.xlsx")
    df_odp = leggi_view(
        table="vwESOdP", colonna_filtro_esclusi="CodArt", colonna_filtro_stato="StatoOrdine")
    df_odpfasi = (pd.DataFrame(leggi_view(table="vwESOdPFasi", colonna_filtro_esclusi="CodRisorsaProd", colonna_filtro_stato=""))
                  .pipe(filtra_odpfasi_con_odp, df_odp=df_odp)
                  .pipe(inserimento_reparto_da_risorsa, df_risorse=leggi_view("vwESRisorse", colonna_filtro_esclusi="CodRisorsaProd", colonna_filtro_stato="")))
    CHIAVI = ["IdDocumento", "IdRiga"]
    # df_odpfasi.to_excel("excel//odpfasi.xlsx")
    df_odpcomponenti = (leggi_view("vwESOdPComponenti", colonna_filtro_esclusi="", colonna_filtro_stato="")
                        .pipe(filtra_odpcomponenti_con_odp, df_odp=df_odp))
    # df_odpcomponenti.to_excel("excel//odpcomponenti.xlsx")
    df_fasi_componenti = unione_fasi_componenti(df_odpfasi, df_odpcomponenti)
    distinta_componenti = generazione_dizionario(
        df=df_fasi_componenti, CHIAVI=CHIAVI, rename_col="DistintaMateriale", list_columns=["CodArt", "Quantita", "NumFase"])
    df_articoli = leggi_view(
        "vwESArticoli", colonna_filtro_esclusi="", colonna_filtro_stato="")
    input_odp = (inserimento_distinta_in_odp(df_odp=df_odp, componenti_per_odp=distinta_componenti, CHIAVI=CHIAVI)
                 .pipe(inserimento_dati_fasi_in_odp, df_odpfasi=df_odpfasi, CHIAVI=CHIAVI)
                 .pipe(gestione_lotto_matricola_famiglia, df_articoli=df_articoli)
                 .pipe(inserimento_macrofamiglia, df_famiglia=leggi_view("vwESFamiglia", colonna_filtro_esclusi="CodFamiglia", colonna_filtro_stato=""))
                 .drop(columns=["DataInizioProduzione"]))
    try:
        input_odp.to_sql(name="staging_input_odp",
                         con=engine_app,
                         if_exists='append',
                         index=False,
                         method=inserisci_o_ignora)
    except sq.IntegrityError:
        print("Tutte le celle sono uguali")
    # input_odp.to_excel("excel//df_odp.xlsx")
    return

    leggi_view("vwESOdP", "CodArt", "StatoOrdne")
    leggi_view("vwESOdPFasi", "CodRisorsaProd")
    leggi_view("vwESOdPComponenti")
    leggi_view("vwESLavorazioni", "CodLavorazione")
    leggi_view("vwESRisorse", "CodRisorsaProd")
    leggi_view("vwESReparti", "CodReparto")
    leggi_view("vwESCausaliAttivita")
    leggi_view("vwESGiacenzaLotti")
    leggi_view("vwESGiacenza")
    leggi_view("vwESMagazzini", "CodMag")
    leggi_view("vwESArticoli")
    leggi_view("vwESMacroFamiglia", "CodMacrofamiglia")
    leggi_view("vwESArticoli", "CodMag")
    leggi_view("vwESFamiglia", "CodFamiglia")

    # # Rimetti l'indice come colonne
    # # df_joined = df_joined.reset_index()

    # # Rimuovi eventuali colonne inutili
    # df_joined.to_excel("excel//joined.xlsx")
    # ic(df_joined)
    # df_joined = df_joined.drop(columns=["Unnamed: 0"])
    df_lavorazioni = leggi_view_lavorazioni().to_excel("excel//lavorazioni.xlsx")
    df_macrofamiglia = leggi_view_macrofamiglia().to_excel("excel//macrofamiglia.xlsx")
    df_famiglia = leggi_view_famiglia().to_excel("excel//famiglia.xlsx")
    df_reparti = leggi_view_reparti().to_excel("excel//reparti.xlsx")
    df_magazzino = leggi_view_magazzini().to_excel("excel//magazzino.xlsx")
    df_risorse = leggi_view_risorse().to_excel("excel//risorse.xlsx")
    df_risorse = pd.DataFrame(leggi_view_risorse())
    '''df_odp: filtro per StatoOrdine == Pianificata, e rimuovo i macchinari 4.0'''

    '''df_odpfasi: filtro per ordini di produzione e ottengo le fasi dei vari odp
     inserisco la descrizione delle lavorazioni associate al codice'''
    '''df_odpcomponenti: filtro per ordini di produzione e ottengo i componenti per le varie fasi di lavorazione'''
    df_odpcomponenti = (pd.DataFrame(leggi_view_odpcomponenti()).pipe(
        filtra_odpcomponenti_con_odp, df_odp=df_odp))
    # df_odp


if __name__ == "__main__":

    start = time.time()
    elaborazione_dati()
    end = time.time()
    print(f"Tempo di funzionamento {(end - start):4.1f} s")
    # .to_excel("excel//odp.xlsx")
    # df_magazzino = f_lavorazioni().to_excel("excel//magazzini.xlsx")
    # df_odp.to_excel("excel//odp.xlsx")
    # df_odpfasi.to_excel("excel//odpfasi.xlsx")
    # df_odpcomponenti.to_excel("excel//odpcomponenti.xlsx")
    # df_gicenzalotti = f_gicenzalotti().to_excel('excel//giacenzalotti.xlsx')
    # df_giacenza = f_giacenza().to_excel('excel//giacenza.xlsx')

    # df_odpcomponenti = f_odpcomponenti()
    # df_reparti = f_reparti()
    # df_causali = f_causali()
    # df_magazzino = f_magazzino()
    # df_articoli = f_articoli()
    # df_macrofamiglia = f_macrofamiglia()
    # df_famiglia = f_famiglia()


# def inserimento_descrizione_lavorazioni(df_odpfasi: pd.DataFrame, df_lavorazioni: pd.DataFrame) -> pd.DataFrame:
#     '''
#     NON PIù IN USO
#     Inserimento delle descrizioni associate al codice lavorazione

#     :param df_odpfasi: dataframe con le fasi degli ordini di produzione
#     :type df_odpfasi: pd.DataFrame
#     :param df_lavorazioni: dataframe con il codice lavorazion e la descrizione associata
#     :type df_lavorazioni: pd.DataFrame
#     :return: Dataframe df_odpfasi con aggiunta della descrizione della lavorazione
#     :rtype: DataFrame
#     '''
#     df_odpfasi_merged = df_odpfasi.merge(
#         df_lavorazioni[['CodLavorazione', 'DesLavorazione']], on='CodLavorazione', how='left')

#     return df_odpfasi_merged


# def inserimento_descrizione_articoli(df_input: pd.DataFrame, df_articoli: pd.DataFrame) -> pd.DataFrame:
#     '''
#     NON PIù IN USO
#     Inserimento descrizione articolo (DesArt) acquisito dal df_articoli ed inserito  nel df di input

#     :param df_input: Dataframe di input con la colonna codici articolo (CodArt)
#     :type df_input: pd.DataFrame
#     :param df_articoli: Acquisizione degli articoli dal db iniziale
#     :type df_articoli: pd.DataFrame
#     :return: Ritorno del df df_fasi_articolo con la descrizione del materiale
#     :rtype: pd.DataFrame
#     '''
#     df_output = df_input.merge(
#         df_articoli[["CodArt", "DesArt"]], on=["CodArt"], how="left")
#     return df_output
