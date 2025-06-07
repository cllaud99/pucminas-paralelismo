import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List

import pandas as pd

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.compare_times import compare_execution_times
from utils.log_decorator import logger

banner_exercise_8 = """
================================================================================
🔄  Conversão paralela de arquivos Parquet → CSV
--------------------------------------------------------------------------------
🚀  Simule a conversão de 10 arquivos .parquet para .csv, onde cada processo lida
com um arquivo diferente.
================================================================================
"""

logger.success(banner_exercise_8)


def convert_parquet_to_csv(parquet_path: str, csv_path: str) -> str:
    """
    Converte um arquivo Parquet para CSV.

    Args:
        parquet_path (str): Caminho do arquivo Parquet.
        csv_path (str): Caminho onde o arquivo CSV será salvo.

    Returns:
        str: Caminho do arquivo CSV gerado.
    """
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path, index=False)
    return csv_path


def parallel_conversion(
    parquet_files: List[str], output_dir: str, n_workers: int = 4
) -> float:
    """
    Converte múltiplos arquivos Parquet para CSV em paralelo.

    Args:
        parquet_files (List[str]): Lista de caminhos de arquivos Parquet.
        output_dir (str): Diretório onde os CSVs serão salvos.
        n_workers (int): Número de processos paralelos.

    Returns:
        float: Tempo total de execução em segundos.
    """
    os.makedirs(output_dir, exist_ok=True)
    start_time = time.time()

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = []
        for parquet_file in parquet_files:
            filename = os.path.splitext(os.path.basename(parquet_file))[0] + ".csv"
            csv_path = os.path.join(output_dir, filename)
            futures.append(
                executor.submit(convert_parquet_to_csv, parquet_file, csv_path)
            )

        for future in as_completed(futures):
            future.result()

    end_time = time.time()
    return end_time - start_time


def sequential_conversion(parquet_files: List[str], output_dir: str) -> float:
    """
    Converte arquivos Parquet para CSV de forma sequencial.

    Args:
        parquet_files (List[str]): Lista de caminhos de arquivos Parquet.
        output_dir (str): Diretório onde os CSVs serão salvos.

    Returns:
        float: Tempo total de execução em segundos.
    """
    os.makedirs(output_dir, exist_ok=True)
    start_time = time.time()

    for parquet_file in parquet_files:
        filename = os.path.splitext(os.path.basename(parquet_file))[0] + ".csv"
        csv_path = os.path.join(output_dir, filename)
        convert_parquet_to_csv(parquet_file, csv_path)

    end_time = time.time()
    return end_time - start_time


def main() -> None:
    input_parquet_dir = "./data/inputs/simulated_datalake_files_parquet"
    parquet_files = [
        os.path.join(input_parquet_dir, file) for file in os.listdir(input_parquet_dir)
    ]

    output_csv_dir_parallel = "./data/outputs/exercicio_08/output_csv_parallel"
    output_csv_dir_sequential = "./data/outputs/exercicio_08/output_csv_sequential"
    n_workers = 4

    logger.info("🚀 Convertendo arquivos Parquet para CSV em paralelo...")
    parallel_time = parallel_conversion(
        parquet_files, output_csv_dir_parallel, n_workers=n_workers
    )

    logger.info("🔁 Convertendo arquivos Parquet para CSV sequencialmente...")
    sequential_time = sequential_conversion(parquet_files, output_csv_dir_sequential)

    compare_execution_times(sequential_time, parallel_time)


if __name__ == "__main__":
    main()
