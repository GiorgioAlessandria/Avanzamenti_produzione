# sync_sqlserver_to_appdb.py
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
import tomllib
try:
    from icecream import ic
except:
    pass

CONFIG_PATH = Path("static//filtri_sync.toml")


def load_config() -> dict:
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


def f_odp() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESOdP\n
    :return: DataFrame della vista con filtro su StatoOrdine e rimozione degli articoli non necessari\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESOdP"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtro = df[df['StatoOrdine'] ==
                   config["Elementi_selezionati"]["seleziona_stato"]]
    df_filtro_articoli = df_filtro[~df_filtro["CodArt"].isin(
        config["Elementi_esclusi"]["escludi_articoli"])]
    df_filtro_articoli = df_filtro_articoli.reset_index()
    return df_filtro_articoli


def f_odpfasi() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESOdPFasi con filtro su CodRisorsaProd\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESOdPFasi"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodRisorsaProd"].isin(
        config["Elementi_esclusi"]["escludi_risorse"])]
    return df_filtrato


def filtro_odp_odpfasi(df_odpfasi: pd.DataFrame, df_odp: pd.DataFrame) -> pd.DataFrame:
    '''
    Incrocio dei dati per mantenere le linee di df_odpfasi che corrispondono a [IdDocumento, IdRiga] di df_odp\n

    :param df_odpfasi: dataframe con le fasi degli ordini di produzione
    :type df_odpfasi: pd.DataFrame
    :param df_odp: dataframe con gli ordini di produzione
    :type df_odp: pd.DataFrame
    :return: DataFrame filtrato per IdDocumento e IdRiga di df_odp\n
    :rtype: pd.DataFrame
    '''
    df_odpfasi_filtered = df_odpfasi.merge(df_odp[["IdDocumento", "IdRiga"]], on=[
                                           "IdDocumento", "IdRiga"], how='right')
    return df_odpfasi_filtered


def f_odpcomponenti() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESOdPComponenti\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESOdPComponenti"""

    df = pd.read_sql(query, engine_sqlserver)

    return df


def odpcomponenti_filtro(df_odpcomponenti: pd.DataFrame, df_odp: pd.DataFrame) -> pd.DataFrame:
    '''
    Incrocio dei dati per mantenere le linee di df_odpcomponenti che si trovano in IdDocumento e IdRiga di df_odp.\nIl filtro su df_odpcomponenti è ["IdDocumento", "IdRigaPadre"]\n

    :rtype: pd.DataFrame
    :param df_odpcomponenti: dataframe con i componenti in base agli ordini di produzione
    :type df_odpcomponenti: pd.DataFrame
    :param df_odp: dataframe con gli ordini di produzione
    :type df_odp: pd.DataFrame
    :return: DataFrame filtrato IdDocumento e IdRiga di df_odp\n
    :rtype: pd.DataFrame
    '''
    df_odpcomponenti_filtered = df_odpcomponenti.merge(df_odp[["IdDocumento", "IdRiga"]], left_on=[
        "IdDocumento", "IdRigaPadre"], how='right', right_on=["IdDocumento", "IdRiga"], suffixes=["", "_y"])
    df_odpcomponenti_filtered = df_odpcomponenti_filtered.drop(
        columns=["IdRiga_y"])
    return df_odpcomponenti_filtered


def inserimento_CodReparto(df_odpfasi: pd.DataFrame, df_risorse: pd.DataFrame) -> pd.DataFrame:
    '''
    Inserimento del reparto in base alla risorsa richiamata\n

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


def f_lavorazioni() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESLavorazioni\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESLavorazioni"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodLavorazione"].isin(
        config["Elementi_esclusi"]["escludi_lavorazioni"])]
    return df_filtrato


def descrizione_lavorazioni(df_odpfasi: pd.DataFrame, df_lavorazioni: pd.DataFrame) -> pd.DataFrame:
    '''
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


def f_risorse() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESRisorse\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESRisorse"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodRisorsaProd"].isin(
        config["Elementi_esclusi"]["escludi_risorse"])]
    return df_filtrato


def f_reparti() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESReparti\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESReparti"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodReparto"].isin(
        config["Elementi_esclusi"]["escludi_reparti"])]
    return df_filtrato


def f_causali() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESCausaliAttivita\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESCausaliAttivita"""
    df = pd.read_sql(query, engine_sqlserver)
    return df


def f_gicenzalotti() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESGiacenzaLotti\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESGiacenzaLotti"""
    df = pd.read_sql(query, engine_sqlserver)
    return df


def f_giacenza() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESGiacenza\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESGiacenza"""
    df = pd.read_sql(query, engine_sqlserver)
    return df


def f_magazzini() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESMagazzino\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESMagazzini"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodMag"].isin(
        config["Elementi_esclusi"]["escludi_magazzini"])]
    return df_filtrato


def f_articoli() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESArticoli\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESArticoli"""
    df = pd.read_sql(query, engine_sqlserver)
    return df


def f_macrofamiglia() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESMacroFamiglia\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESMacroFamiglia"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodMacrofamiglia"].isin(
        config["Elementi_esclusi"]["escludi_macrofamiglie"])]
    return df_filtrato


def f_famiglia() -> pd.DataFrame:
    '''
    Acquisizione dei dati sulla vista vwESFamiglia\n
    :return: DataFrame della vista\n
    :rtype: pd.DataFrame
    '''
    query = f"""SELECT * FROM BernardiProd.dbo.vwESFamiglia"""
    df = pd.read_sql(query, engine_sqlserver)
    df_filtrato = df[~df["CodFamiglia"].isin(
        config["Elementi_esclusi"]["escludi_famiglie"])]
    return df_filtrato


def sync_ordini():
    '''
    Funzione per l'elaborazione dei dati in ingresso
    '''
    pd.DataFrame(f_odp()).to_excel("excel//odp.xlsx")
    df_odp = pd.DataFrame(f_odp())
    df_odpfasi = pd.DataFrame(f_odpfasi()).pipe(filtro_odp_odpfasi, df_odp=pd.DataFrame(f_odp())).pipe(
        descrizione_lavorazioni, df_lavorazioni=pd.DataFrame(f_lavorazioni())).pipe(inserimento_CodReparto, df_risorse=pd.DataFrame(f_risorse())).to_excel("excel//odpfasi.xlsx")
    df_odpcomponenti = (pd.DataFrame(f_odpcomponenti()).pipe(
        odpcomponenti_filtro, df_odp=df_odp)).to_excel("excel//odpcomponenti.xlsx")

    return
    df_lavorazioni = f_lavorazioni().to_excel("excel//lavorazioni.xlsx")
    df_macrofamiglia = f_macrofamiglia().to_excel("excel//macrofamiglia.xlsx")
    df_famiglia = f_famiglia().to_excel("excel//famiglia.xlsx")
    df_reparti = f_reparti().to_excel("excel//reparti.xlsx")
    df_magazzino = f_magazzini().to_excel("excel//magazzino.xlsx")
    df_risorse = f_risorse().to_excel("excel//risorse.xlsx")
    df_risorse = pd.DataFrame(f_risorse())
    '''df_odp: filtro per StatoOrdine == Pianificata, e rimuovo i macchinari 4.0'''

    '''df_odpfasi: filtro per ordini di produzione e ottengo le fasi dei vari odp
     inserisco la descrizione delle lavorazioni associate al codice'''
    '''df_odpcomponenti: filtro per ordini di produzione e ottengo i componenti per le varie fasi di lavorazione'''
    df_odpcomponenti = (pd.DataFrame(f_odpcomponenti()).pipe(
        odpcomponenti_filtro, df_odp=df_odp))
    # df_odp


if __name__ == "__main__":
    sync_ordini()
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
