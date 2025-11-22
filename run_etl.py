"""
Script principal de execução do ETL das loterias.

Rodar com:
    python run_etl.py

Este script:
- Carrega o dataset bruto (dataset.json)
- Separa por concursos
- Executa Etapa 1 → limpeza
- Executa Etapa 2 → feature engineering
- Executa Etapa 3 → salvar em SQLite
"""

import pandas as pd
from pathlib import Path
from tqdm import tqdm

from src.commons.utils import separar_concursos
from src.commons.data_cleaning import data_cleaning_etapa1
from src.commons.feature_engineering import feature_engineering_etapa2
from src.commons.transform_to_sqlite import salvar_no_sqlite_etapa3

DATASET_PATH = "data/raw/dataset.json"
OUTPUT_DIR = "data/raw"
SQLITE_PATH = "loterias.db"

def rodar_pipeline() -> None:
    """
    Executa todo o fluxo de ETL das loterias: separação, limpeza,
    feature engineering e salvamento em SQLite.

    Fluxo:
        1. Carrega o dataset bruto.
        2. Separa cada tipo de loteria em DataFrames independentes.
        3. Aplica a Etapa 1 (limpeza e normalização).
        4. Aplica a Etapa 2 (feature engineering).
        5. Aplica a Etapa 3 (conversão final e escrita no SQLite).
    
    Retorna:
        None. O resultado final é persistido no banco SQLite e nos CSVs.
    """
    print("\n=== INICIANDO ETL DAS LOTERIAS ===\n")

    # ------------------------------------------------------
    # 1) Carregar e separar concursos
    # ------------------------------------------------------
    print("Carregando dataset bruto...")
    tabelas = separar_concursos(DATASET_PATH, OUTPUT_DIR)

    print(f"{len(tabelas)} concursos identificados: {list(tabelas.keys())}\n")

    # ------------------------------------------------------
    # 2) Processar concurso por concurso
    # ------------------------------------------------------
    for nome, df_raw in tqdm(tabelas.items(), desc="Processando concursos"):
        print(f"→ Processando concurso: {nome}")

        # Etapa 1
        df_limpo = data_cleaning_etapa1(df_raw)

        # Etapa 2
        df_feat = feature_engineering_etapa2(df_limpo)

        # Etapa 3
        salvar_no_sqlite_etapa3(df_feat, SQLITE_PATH)

        print(f"{nome} concluído e salvo no SQLite.\n")

    print("\n=== ETL CONCLUÍDO COM SUCESSO ===")


if __name__ == "__main__":
    rodar_pipeline()
