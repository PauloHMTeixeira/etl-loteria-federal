"Functions for the second step of the ETL Pipeline, feature engineering"

import ast
from typing import Any

import pandas as pd


def criar_features_datas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria variáveis derivadas das datas: dia, mês, ano, semana ISO e dia da semana.

    Args:
        df (pd.DataFrame): DataFrame contendo as colunas 'data' e 'proximoConcurso'.

    Returns:
        pd.DataFrame: DataFrame com colunas adicionais de datas.
    """
    df["data_dia"] = df["data"].dt.day
    df["data_mes"] = df["data"].dt.month
    df["data_ano"] = df["data"].dt.year
    df["semana_ano_concurso"] = df["data"].dt.isocalendar().week
    df["dia_semana_concurso"] = df["data"].dt.weekday

    df["proximoConcurso_dia"] = df["proximoConcurso"].dt.day
    df["proximoConcurso_mes"] = df["proximoConcurso"].dt.month
    df["proximoConcurso_ano"] = df["proximoConcurso"].dt.year

    return df

def expandir_dezenas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria colunas dezena_1, dezena_2 ... dezena_N baseadas no tamanho da lista 'dezenas'.

    Args:
        df (pd.DataFrame): DataFrame contendo a coluna 'dezenas'.

    Returns:
        pd.DataFrame: DataFrame expandido com dezenas individuais.
    """
    df["qtd_dezenas"] = df["dezenas"].apply(lambda x: len(x) if isinstance(x, list) else 0)
    max_dezenas = df["qtd_dezenas"].max()

    for i in range(max_dezenas):
        df[f"dezena_{i+1}"] = df["dezenas"].apply(
            lambda x: x[i] if isinstance(x, list) and len(x) > i else None
        )

    df.drop(columns=["qtd_dezenas"], inplace=True)
    return df

def separar_local(df: pd.DataFrame) -> pd.DataFrame:
    """
    Separa a coluna 'local' em 'nome_local', 'cidade' e 'estado'.

    Args:
        df (pd.DataFrame): DataFrame com a coluna 'local'.

    Returns:
        pd.DataFrame: DataFrame com colunas separadas.
    """
    if "local" not in df.columns:
        return df

    df[["nome_local", "resto"]] = df["local"].str.split(" em ", n=1, expand=True)
    df[["cidade", "estado"]] = df["resto"].str.rsplit(", ", n=1, expand=True)
    df.drop(columns=["resto"], inplace=True)

    return df

def expandir_premiacoes_loteria(prem_list: Any) -> dict:
    """
    Expande a lista de premiações em dicionário com colunas ganhadores/valor por faixa.

    Args:
        prem_list (Any): Lista de premiações.

    Returns:
        dict: Colunas dinâmicas por faixa.
    """
    if not isinstance(prem_list, list):
        return {}

    resultado = {}
    for item in prem_list:
        faixa = item.get("faixa")
        if faixa is None:
            continue

        resultado[f"ganhadores_faixa_{faixa}"] = item.get("ganhadores")
        resultado[f"valor_faixa_{faixa}"] = item.get("valorPremio")

    return resultado


def aplicar_expansao_premiacoes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica expansão de premiações ao DataFrame.

    Args:
        df (pd.DataFrame): DataFrame com coluna 'premiacoes'.

    Returns:
        pd.DataFrame: DF com colunas de premiações expandidas.
    """
    df["premiacoes"] = df["premiacoes"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str)
        else (x if isinstance(x, list) else None)
    )

    premios_expandido = df["premiacoes"].apply(expandir_premiacoes_loteria)
    premios_df = pd.DataFrame(premios_expandido.tolist())
    df = pd.concat([df, premios_df], axis=1)

    cols_ganhadores = [c for c in df.columns if c.startswith("ganhadores_faixa_")]
    cols_valores = [c for c in df.columns if c.startswith("valor_faixa_")]

    df["total_ganhadores"] = df[cols_ganhadores].sum(axis=1)
    df["total_pago_premios"] = df[cols_valores].sum(axis=1)
    df["media_premio_real"] = df["total_pago_premios"] / df["total_ganhadores"]

    return df

def processar_local_ganhadores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrai informações de local de ganhadores e cria flag de ticket online.

    Args:
        df (pd.DataFrame): DataFrame contendo localGanhadores.

    Returns:
        pd.DataFrame: DataFrame enriquecido.
    """
    def normalizar_local(lista):
        return lista if isinstance(lista, list) else []

    df["localGanhadores"] = df["localGanhadores"].apply(normalizar_local)

    df["municipioGanhador"] = df["localGanhadores"].apply(
        lambda x: x[0].get("municipio") if len(x) > 0 else None
    )

    df["ufGanhador"] = df["localGanhadores"].apply(
        lambda x: x[0].get("uf") if len(x) > 0 else None
    )

    def is_ticket_online(x):
        if len(x) == 0:
            return False
        registro = x[0]
        municipio = registro.get("municipio", "").strip().upper()
        uf = registro.get("uf", "").strip().upper()
        return municipio == "CANAL ELETRONICO" or uf == "BR"

    df["ticketGanhadorOnline"] = df["localGanhadores"].apply(is_ticket_online)

    return df

def normalizar_dezenas(lista: list) -> list:
    """
    Normaliza a lista de dezenas garantindo que todos os valores sejam inteiros.
    Retorna None caso algo esteja errado.

    Args:
        lista (list): Lista com as dezenas que precisam ser transformados em inteiros.
    
    Returns:
        list: Lista com as dezenas no formato correto.
    """
    if not isinstance(lista, list):
        return None

    nova = []
    for item in lista:
        try:
            n = int(item)
            nova.append(n)
        except Exception:
            return None

    return nova

def calcular_razao(row):
    acumulado = row["valorAcumuladoProximoConcurso"]
    estimado = row["valorEstimadoProximoConcurso"]
    if acumulado in [None, 0] or pd.isna(acumulado):
        return None
    if estimado is None or pd.isna(estimado):
        return None
    return estimado / acumulado

def criar_features_numericas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria features como pares, ímpares, range e razão entre valores acumulados/estimados.

    Args:
        df (pd.DataFrame): DataFrame já processado.

    Returns:
        pd.DataFrame: DataFrame enriquecido com features numéricas.
    """
    df["valorArrecadado"] = df["valorArrecadado"].replace(0, None)

    df["razaoEstimadoAcumulado"] = df.apply(calcular_razao, axis=1)

    df["dezenas"] = df["dezenas"].apply(normalizar_dezenas)

    df["qtd_pares"] = df["dezenas"].apply(
        lambda x: sum(1 for n in x if n % 2 == 0) if isinstance(x, list) else None
    )

    df["qtd_impares"] = df["dezenas"].apply(
        lambda x: sum(1 for n in x if n % 2 != 0) if isinstance(x, list) else None
    )

    df["range_dezenas"] = df["dezenas"].apply(
        lambda x: max(x) - min(x) if isinstance(x, list) and len(x) >= 2 else None
    )

    return df

def feature_engineering_etapa2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Executa todas as transformações de feature engineering da Etapa 2.

    Args:
        df (pd.DataFrame): DataFrame limpo e validado.

    Returns:
        pd.DataFrame: DataFrame enriquecido com features.
    """
    df = criar_features_datas(df)
    df = expandir_dezenas(df)
    df = separar_local(df)
    df = aplicar_expansao_premiacoes(df)
    df = processar_local_ganhadores(df)
    df = criar_features_numericas(df)
    return df
