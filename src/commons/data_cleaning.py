"Functions for the first step of the ETL Pipeline, cleaning the data"

import ast
from typing import Any

import pandas as pd


def drop_colunas_por_loteria(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas específicas dependendo do tipo de loteria.

    Args:
        df (pd.DataFrame): DataFrame do concurso.

    Returns:
        pd.DataFrame: DataFrame sem as colunas irrelevantes.
    """
    loteria = str(df["loteria"].iloc[0]).lower()

    if loteria in ["megasena", "lotofacil"]:
        colunas_drop = ["timeCoracao", "mesSorte", "trevos"]

    elif loteria == "maismilionaria":
        colunas_drop = ["timeCoracao", "mesSorte"]

    elif loteria == "timemania":
        colunas_drop = ["mesSorte", "trevos"]

    elif loteria == "diadesorte":
        colunas_drop = ["timeCoracao", "trevos"]

    else:
        colunas_drop = []

    colunas_existentes = [c for c in colunas_drop if c in df.columns]

    return df.drop(columns=colunas_existentes)

def tratar_tipos_e_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica conversões de tipos: datas, inteiros, monetários, booleanos.

    Args:
        df (pd.DataFrame): DataFrame do concurso.

    Returns:
        pd.DataFrame: DataFrame com tipagem correta.
    """
    # Remover duplicatas de concurso
    if df.duplicated(subset=["concurso"]).sum():
        df = df.drop_duplicates(subset=["concurso"], keep="last")

    # Concurso como inteiro
    df["concurso"] = pd.to_numeric(df["concurso"], errors="coerce").astype("Int64")

    # Datas
    df["data"] = pd.to_datetime(df["data"], dayfirst=True, format="mixed")
    df["proximoConcurso"] = pd.to_datetime(df["proximoConcurso"], dayfirst=True, format="mixed")

    # Colunas monetárias
    cols_monetarias = [
        "valorArrecadado",
        "valorAcumuladoConcurso_0_5",
        "valorAcumuladoConcursoEspecial",
        "valorAcumuladoProximoConcurso",
        "valorEstimadoProximoConcurso",
    ]

    for c in cols_monetarias:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Booleanos
    if "acumulou" in df.columns:
        df["acumulou"] = df["acumulou"].astype("boolean")

    return df

def normalizar_lista(valor: Any) -> Any:
    """
    Converte strings de lista/dict para objetos Python.

    Args:
        valor (Any): String para ser convertida para lista/dict.

    Returns:
        Any: Lista ou dicionário convertido.
    """
    if isinstance(valor, list):
        return valor
    if isinstance(valor, str):
        try:
            return ast.literal_eval(valor)
        except Exception:
            return None
    return None

def aplicar_normalizacao_listas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica normalização de listas às colunas que podem conter listas/dicts.

    Args:
        df (pd.DataFrame): DataFrame que vai ter suas colunas normalizadas.
    
    Returns:
        pd.DataFrame: DataFrame com colunas normalizadas.
    """
    colunas = ["dezenas", "dezenasOrdemSorteio", "premiacoes", "localGanhadores"]

    for col in colunas:
        if col in df.columns:
            df[col] = df[col].apply(normalizar_lista)

    return df

def dezenas_validas(lista: Any, loteria: str) -> bool:
    """
    Valida se todas as dezenas estão dentro do intervalo correto da loteria.

    Args:
        lista (Any): Lista com as dezenas que precisam ser validadas.
        loteria (str): Nome da loteria a ser validada.

    Returns:
        bool: True se estiver dentro do intervalo, False se estiver fora.
    """
    intervalos = {
        "megasena": (1, 60),
        "lotofacil": (1, 25),
        "timemania": (1, 80),
        "diadesorte": (1, 31),
        "maismilionaria": (1, 50),
    }

    if loteria not in intervalos:
        return True  # fallback seguro

    faixa = intervalos[loteria]
    if not isinstance(lista, list):
        return False

    try:
        return all(faixa[0] <= int(n) <= faixa[1] for n in lista)
    except Exception:
        return False
    
def validar_dezenas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica validação de expected_count e intervalo de dezenas.

    Args:
        df (pd.DataFrame): DataFrame a ser validado.

    Returns:
        pd.DataFrame: DataFrame validado.
    """
    # expected_count: baseado na 1ª linha
    expected_count = len(df.loc[0, "dezenas"])

    # Filtrar listas com quantidade errada
    df = df[df["dezenas"].apply(lambda x: isinstance(x, list) and len(x) == expected_count)]

    # Validar intervalo correto
    df = df[df.apply(lambda row: 
                     dezenas_validas(row["dezenas"], row["loteria"]), axis=1)]

    return df.reset_index(drop=True)

def data_cleaning_etapa1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Executa a etapa 1 da pipeline:
    - drop colunas específicas por tipo de loteria
    - tratar tipos (datas, numéricos, booleanos)
    - normalizar listas
    - validar dezenas (quantidade e intervalo)

    Args:
        df (pd.DataFrame): DataFrame do concurso.

    Returns:
        pd.DataFrame: DataFrame limpo e validado.
    """
    df = df.copy()

    df = drop_colunas_por_loteria(df)
    df = tratar_tipos_e_colunas(df)
    df = aplicar_normalizacao_listas(df)
    df = validar_dezenas(df)

    return df
