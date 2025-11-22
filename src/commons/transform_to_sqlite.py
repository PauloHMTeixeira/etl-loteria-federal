"Functions for the third step of the ETL Pipeline, loading the data to a SQLite db"

import json

import pandas as pd
import sqlite3


def listas_para_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte automaticamente colunas com listas para strings JSON,
    já que SQLite não suporta listas nativamente.

    Args:
        df (pd.DataFrame): DataFrame de um concurso.

    Returns:
        pd.DataFrame: DataFrame com colunas contendo listas convertidas para JSON.
    """
    df = df.copy()

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(json.dumps)

    return df

def salvar_sqlite(df: pd.DataFrame, caminho_db: str = "loterias.db") -> None:
    """
    Salva o DataFrame no banco SQLite usando o nome da loteria como nome da tabela.

    Args:
        df (pd.DataFrame): DataFrame já preparado para SQLite.
        caminho_db (str, optional): Caminho do arquivo SQLite. Padrão: 'loterias.db'.

    Returns:
        None
    """
    nome_loteria = str(df["loteria"].iloc[0]).lower()

    conn = sqlite3.connect(caminho_db)
    try:
        df.to_sql(
            name=nome_loteria,
            con=conn,
            if_exists="replace",
            index=False
        )
    finally:
        conn.close()

def salvar_no_sqlite_etapa3(df: pd.DataFrame, caminho_db: str | None) -> None:
    """
    Executa a Etapa 3 da pipeline:
    - converte colunas de listas para JSON;
    - salva o DataFrame no SQLite com nome da tabela igual ao da loteria.

    Args:
        df (pd.DataFrame): DataFrame já processado pelas etapas anteriores.
        caminho_db (str): Caminho do arquivo SQLite.

    Returns:
        None
    """
    caminho_db = "loterias.db" if caminho_db is None else caminho_db
    df_json = listas_para_json(df)
    salvar_sqlite(df_json, caminho_db)