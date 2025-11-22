"Utils functions for the ETL Pipeline"

import pandas as pd


def separar_concursos(
    fonte: str | pd.DataFrame,
    output_dir: str
) -> dict[str, pd.DataFrame]:
    """
    Separa um dataset de loterias em CSVs individuais por concurso.

    Os nomes das tabelas são gerados com base no valor padronizado
    da coluna "loteria". A função aceita tanto o caminho de um arquivo 
    JSON quanto um DataFrame já carregado.

    Args:
        fonte (str | pd.DataFrame): Caminho para um arquivo JSON com o 
            dataset completo ou um DataFrame contendo os dados.
        output_dir (str, optional): Diretório onde os arquivos CSV serão 
            salvos. Padrão é "raw".

    Returns:
        dict[str, pd.DataFrame]: Um dicionário onde as chaves são os nomes 
        das loterias e os valores são os DataFrames separados.

    Raises:
        FileNotFoundError: Caso o caminho informado em `fonte` não exista.
        ValueError: Caso o DataFrame não contenha a coluna "loteria".
    """
    output_dir = "raw" if output_dir is None else output_dir

    # Carregar dados
    if isinstance(fonte, str):
        try:
            df = pd.read_json(fonte)
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo não encontrado: {fonte}")
    else:
        df = fonte.copy()

    # Remover linhas totalmente nulas
    df = df.dropna(how="all").reset_index(drop=True)

    # Verificar se a coluna loteria existe
    if "loteria" not in df.columns:
        raise ValueError("A coluna 'loteria' não está presente no dataset.")

    # Padronizar nomes de loteria
    df["loteria"] = (
        df["loteria"]
        .astype(str)
        .str.lower()
        .str.replace("-", "", regex=False)
        .str.replace("_", "", regex=False)
        .str.strip()
    )

    # Obter lista de concursos disponíveis
    concursos = df["loteria"].unique()

    # Dicionário para retorno
    tabelas = {}

    # Separar e salvar
    for concurso in concursos:
        df_c = df[df["loteria"] == concurso].reset_index(drop=True)
        caminho_saida = f"{output_dir}/{concurso}.csv"

        df_c.to_csv(caminho_saida, index=False)
        tabelas[concurso] = df_c

    return tabelas
