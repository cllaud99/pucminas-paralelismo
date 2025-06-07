import os
import string
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple

import numpy as np
import pandas as pd

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.compare_times import compare_execution_times
from utils.log_decorator import logger

banner_exercise_9 = """
================================================================================
📊  Cálculo de agregações pesadas com multiprocessing
--------------------------------------------------------------------------------
🚀  Gere um grande DataFrame com dados sintéticos e use multiprocessing para
calcular soma, média e desvio padrão por grupo.
================================================================================
"""

logger.info(banner_exercise_9)


def generate_synthetic_data(n_rows: int = 10_000_000) -> pd.DataFrame:
    """
    Gera um DataFrame com dados sintéticos para simular agregações pesadas.

    Args:
        n_rows (int): Número de linhas do DataFrame.

    Returns:
        pd.DataFrame: DataFrame com colunas de grupo e valores numéricos.
    """
    np.random.seed(42)  # Para reprodutibilidade
    group_labels = np.random.choice(list(string.ascii_uppercase[:5]), size=n_rows)
    values = np.random.rand(n_rows) * 100

    df = pd.DataFrame({"group": group_labels, "value": values})
    return df


def aggregate_group(df_part: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza agregações de soma, média e desvio padrão por grupo em uma partição.

    Args:
        df_part (pd.DataFrame): Parte do DataFrame original.

    Returns:
        pd.DataFrame: Resultados agregados da partição.
    """
    return df_part.groupby("group")["value"].agg(["sum", "mean", "std"])


def split_dataframe(df: pd.DataFrame, n_parts: int) -> List[pd.DataFrame]:
    """
    Divide um DataFrame em n partes aproximadamente iguais.

    Args:
        df (pd.DataFrame): DataFrame original.
        n_parts (int): Número de partes.

    Returns:
        List[pd.DataFrame]: Lista de partições do DataFrame.
    """
    total_length = len(df)
    part_size = total_length // n_parts
    return [
        df.iloc[i * part_size : None if i == n_parts - 1 else (i + 1) * part_size]
        for i in range(n_parts)
    ]


def apply_aggregation_parallel(
    df: pd.DataFrame, n_workers: int = 4
) -> Tuple[pd.DataFrame, float]:
    """
    Aplica agregações pesadas em paralelo sobre o DataFrame.

    Args:
        df (pd.DataFrame): DataFrame original.
        n_workers (int): Número de processos paralelos.

    Returns:
        Tuple[pd.DataFrame, float]: DataFrame agregado e tempo de execução.
    """
    start_time = time.time()

    parts = split_dataframe(df, n_workers)
    results = []

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = [executor.submit(aggregate_group, part) for part in parts]
        for future in as_completed(futures):
            results.append(future.result())

    df_result = (
        pd.concat(results)
        .groupby("group")
        .agg(
            {
                "sum": "sum",
                "mean": "mean",
                "std": "mean",  # média dos desvios padrão por grupo
            }
        )
        .reset_index()
    )

    end_time = time.time()
    return df_result, end_time - start_time


def apply_aggregation_sequential(df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
    """
    Aplica agregações pesadas de forma sequencial.

    Args:
        df (pd.DataFrame): DataFrame original.

    Returns:
        Tuple[pd.DataFrame, float]: DataFrame agregado e tempo de execução.
    """
    start_time = time.time()
    df_result = df.groupby("group")["value"].agg(["sum", "mean", "std"]).reset_index()
    end_time = time.time()
    return df_result, end_time - start_time


def main() -> None:
    logger.info("🔧 Gerando dados sintéticos...")
    df = generate_synthetic_data()

    logger.info("⚙️  Agregando com multiprocessing...")
    parallel_result, parallel_time = apply_aggregation_parallel(df)

    logger.info("⚙️  Agregando sequencialmente...")
    sequential_result, sequential_time = apply_aggregation_sequential(df)

    compare_execution_times(sequential_time, parallel_time)


if __name__ == "__main__":
    main()
