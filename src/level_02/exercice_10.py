import os
import sys
import time
import pandas as pd
import multiprocessing as mp
import string
import numpy as np

from typing import Tuple

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.compare_times import compare_execution_times
from utils.log_decorator import logger

banner_exercise_10 = """
================================================================================
🔄  Multiprocessamento em pipelines: transformação + persistência
--------------------------------------------------------------------------------
🚀  Transformação em um processo, escrita em disco em outro (multiprocessing.Queue)
================================================================================
"""

logger.info(banner_exercise_10)


def generate_synthetic_data(n_rows: int = 10_000_000) -> pd.DataFrame:
    """
    Gera um DataFrame com dados sintéticos para simular transformações pesadas.

    Args:
        n_rows (int): Número de linhas do DataFrame.

    Returns:
        pd.DataFrame: DataFrame com colunas de grupo e valores numéricos.
    """
    np.random.seed(42)
    group_labels = np.random.choice(list(string.ascii_uppercase[:5]), size=n_rows)
    values = np.random.rand(n_rows) * 100

    return pd.DataFrame({
        "group": group_labels,
        "value": values
    })


def transform_data(df: pd.DataFrame, queue: mp.Queue) -> None:
    """
    Realiza transformação pesada nos dados e envia o resultado para a fila.

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        queue (mp.Queue): Fila de comunicação entre processos.
    """
    logger.info("⚙️  Transformando dados...")
    result = df.groupby("group")["value"].agg(["sum", "mean", "std"]).reset_index()
    queue.put(result)
    logger.info("✅ Transformação concluída")


def persist_data(queue: mp.Queue, output_path: str) -> None:
    """
    Persiste os dados transformados em disco.

    Args:
        queue (mp.Queue): Fila contendo os dados transformados.
        output_path (str): Caminho para salvar o arquivo CSV.
    """
    logger.info("💾 Aguardando dados para persistência...")
    result_df = queue.get()
    result_df.to_csv(output_path, index=False)
    logger.info(f"✅ Dados salvos em: {output_path}")


def pipeline_with_multiprocessing(df: pd.DataFrame, output_path: str) -> float:
    """
    Executa uma pipeline com dois processos: transformação e persistência.

    Args:
        df (pd.DataFrame): DataFrame original.
        output_path (str): Caminho do arquivo de saída.

    Returns:
        float: Tempo total de execução.
    """
    start_time = time.time()
    queue = mp.Queue()

    p1 = mp.Process(target=transform_data, args=(df, queue))
    p2 = mp.Process(target=persist_data, args=(queue, output_path))

    p1.start()
    p2.start()

    p1.join()
    p2.join()

    end_time = time.time()
    return end_time - start_time


def pipeline_sequential(df: pd.DataFrame, output_path: str) -> float:
    """
    Executa a pipeline sequencialmente (transforma e salva).

    Args:
        df (pd.DataFrame): DataFrame original.
        output_path (str): Caminho do arquivo de saída.

    Returns:
        float: Tempo total de execução.
    """
    start_time = time.time()

    logger.info("⚙️  Transformando dados (sequencial)...")
    result = df.groupby("group")["value"].agg(["sum", "mean", "std"]).reset_index()

    logger.info("💾 Salvando dados (sequencial)...")
    result.to_csv(output_path, index=False)

    end_time = time.time()
    return end_time - start_time


def main() -> None:
    logger.info("🔧 Gerando dados sintéticos...")
    df = generate_synthetic_data()
    output_path_parallel = "./data/outputs/exercicio_10/output_parallel.csv"
    output_path_sequential = "./data/outputs/exercicio_10/output_sequential.csv"
    os.makedirs(os.path.dirname(output_path_parallel), exist_ok=True)
    os.makedirs(os.path.dirname(output_path_sequential), exist_ok=True)

    logger.info("🚀 Executando pipeline com multiprocessing...")
    parallel_time = pipeline_with_multiprocessing(df, output_path_parallel)

    logger.info("🚀 Executando pipeline sequencial...")
    sequential_time = pipeline_sequential(df, output_path_sequential)

    compare_execution_times(sequential_time, parallel_time)


if __name__ == "__main__":
    main()
