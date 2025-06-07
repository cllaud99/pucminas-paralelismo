import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable, List

import pandas as pd

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.compare_times import compare_execution_times
from utils.log_decorator import logger

banner_exercise_7 = """
================================================================================
⚙️  Paralelizar aplicação de funções complexas em DataFrames
--------------------------------------------------------------------------------
🚀  Divida um DataFrame em 4 partes e use ProcessPoolExecutor para aplicar uma
função de transformação em cada parte.
================================================================================
"""

logger.success(banner_exercise_7)


def complex_function(df_part: pd.DataFrame) -> pd.DataFrame:
    """
    Função complexa de transformação que será aplicada em cada partição do DataFrame.
    Simula um processamento pesado, multiplicando colunas e criando novas.

    Args:
        df_part (pd.DataFrame): Partição do DataFrame.

    Returns:
        pd.DataFrame: DataFrame transformado.
    """
    time.sleep(1)  # Simula processamento custoso

    df_part["sum"] = df_part["A"] + df_part["B"]
    df_part["product"] = df_part["A"] * df_part["B"]
    return df_part


def split_dataframe(df: pd.DataFrame, n_parts: int) -> List[pd.DataFrame]:
    """
    Divide um DataFrame em n partes aproximadamente iguais.

    Args:
        df (pd.DataFrame): DataFrame original.
        n_parts (int): Número de partes.

    Returns:
        List[pd.DataFrame]: Lista de DataFrames.
    """
    total_length = len(df)
    part_size = total_length // n_parts
    parts = []
    for i in range(n_parts):
        start = i * part_size
        end = None if i == n_parts - 1 else (i + 1) * part_size
        parts.append(df.iloc[start:end])
    return parts


def apply_in_parallel(
    df: pd.DataFrame,
    func: Callable[[pd.DataFrame], pd.DataFrame],
    n_workers: int = 4,
) -> float:
    """
    Aplica uma função complexa em paralelo dividindo o DataFrame.

    Args:
        df (pd.DataFrame): DataFrame original.
        func (Callable[[pd.DataFrame], pd.DataFrame]): Função a ser aplicada.
        n_workers (int): Número de workers paralelos.

    Returns:
        float: Tempo total de execução em segundos.
    """
    start_time = time.time()

    parts = split_dataframe(df, n_workers)
    results = []

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = [executor.submit(func, part) for part in parts]

        for future in as_completed(futures):
            results.append(future.result())

    df_result = pd.concat(results).sort_index()
    end_time = time.time()
    total_time = end_time - start_time
    return total_time


def apply_sequentially(
    df: pd.DataFrame, func: Callable[[pd.DataFrame], pd.DataFrame]
) -> float:
    """
    Aplica uma função complexa sequencialmente no DataFrame inteiro.

    Args:
        df (pd.DataFrame): DataFrame original.
        func (Callable[[pd.DataFrame], pd.DataFrame]): Função a ser aplicada.

    Returns:
        float: Tempo total de execução em segundos.
    """
    start_time = time.time()
    func(df)
    end_time = time.time()
    total_time = end_time - start_time
    return total_time


def main() -> None:
    df = pd.DataFrame({"A": range(1000), "B": range(1000, 2000)})

    parallel_time = apply_in_parallel(df, complex_function)
    sequential_time = apply_sequentially(df, complex_function)
    compare_execution_times(sequential_time, parallel_time)


if __name__ == "__main__":
    main()
