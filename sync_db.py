# sync_sqlserver_to_appdb.py
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
import time

import tomllib
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
    with CONFIG_PATH.open("rb") as f:  # tomllib vuole il file in binario
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


def leggi_view_odp() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESOdP

    :return: DataFrame della vista con filtro su StatoOrdine e rimozione degli articoli non necessari
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESOdP"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtro = df[df['StatoOrdine'] ==
                   config["Elementi_selezionati"]["seleziona_stato"]]
    df_filtro_articoli = df_filtro[~df_filtro["CodArt"].isin(
        config["Elementi_esclusi"]["escludi_articoli"])]
    df_filtro_articoli = df_filtro_articoli.reset_index(drop=True)
    return df_filtro_articoli


def leggi_view_odpfasi() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESOdPFasi con filtro su CodRisorsaProd

    :return: DataFrame della vista
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESOdPFasi"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodRisorsaProd"].isin(
        config["Elementi_esclusi"]["escludi_risorse"])]
    return df_filtrato


def leggi_view_odpcomponenti() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESOdPComponenti

    :return: DataFrame della vista
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESOdPComponenti"""

    df = pd.read_sql(query, engine_sqlserver)

    return df


def leggi_view_lavorazioni() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESLavorazioni

    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESLavorazioni"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodLavorazione"].isin(
        config["Elementi_esclusi"]["escludi_lavorazioni"])]
    return df_filtrato


def leggi_view_risorse() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESRisorse

    :return: DataFrame della vista
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESRisorse"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodRisorsaProd"].isin(
        config["Elementi_esclusi"]["escludi_risorse"])]
    return df_filtrato


def leggi_view_reparti() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESReparti

    :return: DataFrame della vista
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESReparti"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodReparto"].isin(
        config["Elementi_esclusi"]["escludi_reparti"])]
    return df_filtrato


def leggi_view_causali() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESCausaliAttivita

    :return: DataFrame della vista
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESCausaliAttivita"""
    df = pd.read_sql(query, engine_sqlserver)
    return df


def leggi_view_gicenzalotti() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESGiacenzaLotti

    :return: DataFrame della vista
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESGiacenzaLotti"""
    df = pd.read_sql(query, engine_sqlserver)
    return df


def leggi_view_giacenza() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESGiacenza

    :return: DataFrame della vista
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESGiacenza"""
    df = pd.read_sql(query, engine_sqlserver)
    return df


def leggi_view_magazzini() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESMagazzino

    :return: DataFrame della vista
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESMagazzini"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodMag"].isin(
        config["Elementi_esclusi"]["escludi_magazzini"])]
    return df_filtrato


def leggi_view_articoli() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESArticoli

    :return: DataFrame della vista
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESArticoli"""
    df = pd.read_sql(query, engine_sqlserver)
    return df


def leggi_view_macrofamiglia() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESMacroFamiglia

    :return: DataFrame della vista
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESMacroFamiglia"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodMacrofamiglia"].isin(
        config["Elementi_esclusi"]["escludi_macrofamiglie"])]
    return df_filtrato


def leggi_view_famiglia() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESFamiglia

    :return: DataFrame della vista
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESFamiglia"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodFamiglia"].isin(
        config["Elementi_esclusi"]["escludi_famiglie"])]
    return df_filtrato


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
    Incrocio dei dati per mantenere le linee di df_odpcomponenti che si trovano in IdDocumento e IdRiga di df_odp.\nIl filtro su df_odpcomponenti è ["IdDocumento", "IdRigaPadre"]

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


def generazione_distinta(df_fasi_componenti: pd.DataFrame, CHIAVI: list[str]) -> pd.DataFrame:
    '''
    Genera la distinta base per ogni ordine di produzione.

    Raggruppa in base alla costante CHIAVI (impostata nella funzione madre) e crea una lsita di elementi in cui ogni Codice della distinta viene raggruppato con la descrizione e la quantità

    :param df_fasi_componenti: Dataframe con i componenti per fase
    :type df_fasi_componenti: pd.DataFrame
    :param CHIAVI: elenco delle colonne da raggruppare
    :type CHIAVI: list[str]
    :return: Dataframe raggruppati per Codice, descrizione e quantità
    :rtype: pd.DataFrame
    '''

    df_fasi_componenti = df_fasi_componenti.set_index(CHIAVI)
    componenti_per_odp = (
        df_fasi_componenti
        .groupby(CHIAVI)
        .apply(
            (lambda g: g[["CodArt", "Quantita", "NumFase"]]
             .to_dict("records")))
        .rename("DistintaMateriale")
        .reset_index()
    )
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
    # df_odpfasi_fasi_raggruppate = df_odpfasi.set_index(
    #     ["IdDocumento", "IdRiga"])
    # ic(df_odpfasi_fasi_raggruppate)
    # df_odp = df_odp.set_index(
    #     ["IdDocumento", "IdRiga"])
    NumFase_per_odp = pd.DataFrame(
        df_odpfasi
        .groupby(CHIAVI)
        .apply(
            (lambda g: g[["NumFase"]]
             .to_dict("records")))
        .rename("NumFase")
        .reset_index()
    ).set_index(["IdDocumento", "IdRiga"])

    codlavorazione_per_odp = pd.DataFrame(
        df_odpfasi
        .groupby(CHIAVI)
        .apply(
            (lambda g: g[["CodLavorazione"]]
             .to_dict("records")))
        .rename("CodLavorazione")
        .reset_index()
    ).set_index(["IdDocumento", "IdRiga"])

    CodRisorsaProd_per_odp = pd.DataFrame(
        df_odpfasi
        .groupby(CHIAVI)
        .apply(
            (lambda g: g[["CodRisorsaProd"]]
             .to_dict("records")))
        .rename("CodRisorsaProd")
        .reset_index()
    ).set_index(["IdDocumento", "IdRiga"])

    CodReparto_per_odp = pd.DataFrame(
        df_odpfasi
        .groupby(CHIAVI)
        .apply(
            (lambda g: g[["CodReparto"]]
             .to_dict("records")))
        .rename("CodReparto")
        .reset_index()
    ).set_index(["IdDocumento", "IdRiga"])

    DataInizioSched_per_odp = pd.DataFrame(
        df_odpfasi
        .groupby(CHIAVI)
        .apply(
            (lambda g: g[["DataInizioSched"]]
             .to_dict("records")))
        .rename("DataInizioSched")
        .reset_index()
    ).set_index(["IdDocumento", "IdRiga"])

    DataFineSched_per_odp = pd.DataFrame(
        df_odpfasi
        .groupby(CHIAVI)
        .apply(
            (lambda g: g[["DataFineSched"]]
             .to_dict("records")))
        .rename("DataFineSched")
        .reset_index()
    ).set_index(["IdDocumento", "IdRiga"])

    TempoPrevistoLavoraz_per_odp = pd.DataFrame(
        df_odpfasi
        .groupby(CHIAVI)
        .apply(
            (lambda g: g[["TempoPrevistoLavoraz"]]
             .to_dict("records")))
        .rename("TempoPrevistoLavoraz")
        .reset_index()
    ).set_index(["IdDocumento", "IdRiga"])

    df_dizionari = [NumFase_per_odp, codlavorazione_per_odp, CodRisorsaProd_per_odp,
                    CodReparto_per_odp, DataInizioSched_per_odp, DataFineSched_per_odp, TempoPrevistoLavoraz_per_odp]
    import functools as ft
    df_fasi_raggruppate = ft.reduce(lambda left, right: pd.merge(
        left, right, on=CHIAVI), df_dizionari)

    df_odp = df_odp.set_index(["IdDocumento", "IdRiga"])
    df_odp = df_odp.join(
        df_fasi_raggruppate, how="left")
    df_odp = df_odp.reset_index(drop=False)
    return df_odp


def elaborazione_dati():
    '''
    Funzione per l'elaborazione dei dati
    '''
    # pd.DataFrame(leggi_view_odp()).to_excel("excel//odp.xlsx")
    df_odp = leggi_view_odp()
    df_articoli = leggi_view_articoli()
    df_odpfasi = (pd.DataFrame(leggi_view_odpfasi())
                  .pipe(filtra_odpfasi_con_odp, df_odp=leggi_view_odp())
                  .pipe(inserimento_reparto_da_risorsa, df_risorse=leggi_view_risorse()))
    CHIAVI = ["IdDocumento", "IdRiga"]
    # df_odpfasi.to_excel("excel//odpfasi.xlsx")
    df_odpcomponenti = (leggi_view_odpcomponenti()
                        .pipe(filtra_odpcomponenti_con_odp, df_odp=df_odp))
    # df_odpcomponenti.to_excel("excel//odpcomponenti.xlsx")

    df_fasi_componenti = unione_fasi_componenti(df_odpfasi, df_odpcomponenti)
    distinta_componenti = generazione_distinta(
        df_fasi_componenti=df_fasi_componenti, CHIAVI=CHIAVI)
    df_odp = (inserimento_distinta_in_odp(df_odp=df_odp, componenti_per_odp=distinta_componenti, CHIAVI=CHIAVI)
              .pipe(inserimento_dati_fasi_in_odp, df_odpfasi=df_odpfasi, CHIAVI=CHIAVI))

    df_odp.to_excel("excel//df_odp.xlsx")
    return
    #   .pipe(inserimento_descrizione_lavorazioni, df_lavorazioni=leggi_view_lavorazioni())
    #   .pipe(inserimento_descrizione_articoli, df_articoli=df_articoli)
    #   .pipe(inserimento_descrizione_articoli, df_articoli=df_articoli))
    # df_joined = comp_indexed.join(
    #     fasi_indexed,
    #     how="left",
    #     rsuffix="_fase",
    # )

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


def inserimento_descrizione_lavorazioni(df_odpfasi: pd.DataFrame, df_lavorazioni: pd.DataFrame) -> pd.DataFrame:
    '''
    NON PIù IN USO
    Inserimento delle descrizioni associate al codice lavorazione

    :param df_odpfasi: dataframe con le fasi degli ordini di produzione
    :type df_odpfasi: pd.DataFrame
    :param df_lavorazioni: dataframe con il codice lavorazion e la descrizione associata
    :type df_lavorazioni: pd.DataFrame
    :return: Dataframe df_odpfasi con aggiunta della descrizione della lavorazione
    :rtype: DataFrame
    '''
    df_odpfasi_merged = df_odpfasi.merge(
        df_lavorazioni[['CodLavorazione', 'DesLavorazione']], on='CodLavorazione', how='left')
    return df_odpfasi_merged


def inserimento_descrizione_articoli(df_input: pd.DataFrame, df_articoli: pd.DataFrame) -> pd.DataFrame:
    '''
    NON PIù IN USO
    Inserimento descrizione articolo (DesArt) acquisito dal df_articoli ed inserito  nel df di input

    :param df_input: Dataframe di input con la colonna codici articolo (CodArt)
    :type df_input: pd.DataFrame
    :param df_articoli: Acquisizione degli articoli dal db iniziale
    :type df_articoli: pd.DataFrame
    :return: Ritorno del df df_fasi_articolo con la descrizione del materiale
    :rtype: pd.DataFrame
    '''
    df_output = df_input.merge(
        df_articoli[["CodArt", "DesArt"]], on=["CodArt"], how="left")
    return df_output
