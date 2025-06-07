import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import pandas as pd

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.compare_times import compare_execution_times
from utils.log_decorator import log_execution, logger

input_dir = "data/inputs/simulated_datalake_files"
output_dir = "data/outputs/exercicio_11"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]

banner_etl = """
================================================================================
🔄  EXERCÍCIO: ETL PARALELA POR PARTIÇÃO DE DADOS (ARQUIVOS CSV)
--------------------------------------------------------------------------------
📂  Cada arquivo CSV é tratado como uma partição (ex: mês)
🧰  Processamento simulado e saída salva em Parquet
================================================================================
"""

logger.success(banner_etl)


def process_partition(file_path: str) -> str:
    """
    Processa um arquivo CSV: lê, transforma e salva parquet.

    Args:
        file_path (str): Caminho do arquivo CSV de entrada.

    Returns:
        str: Caminho do arquivo parquet gerado.
    """
    try:
        df = pd.read_csv(file_path)

        # Exemplo simples de transformação: criar uma coluna total (quantidade * preço)
        if {"quantity", "price"}.issubset(df.columns):
            df["total"] = df["quantity"] * df["price"]

        # Nome do arquivo parquet correspondente
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        parquet_path = os.path.join(output_dir, f"{base_name}.parquet")

        df.to_parquet(parquet_path, index=False)

        logger.info(f"Arquivo processado e salvo: {parquet_path}")

        return parquet_path

    except Exception as e:
        logger.error(f"Erro no processamento do arquivo {file_path}: {e}")
        raise


@log_execution
def etl_sequential() -> List[str]:
    """
    Processa todos os arquivos CSV sequencialmente.

    Returns:
        List[str]: Lista de arquivos parquet gerados.
    """
    processed_files = []
    for file in files:
        full_path = os.path.join(input_dir, file)
        processed_path = process_partition(full_path)
        processed_files.append(processed_path)

    return processed_files


@log_execution
def etl_parallel() -> List[str]:
    """
    Processa todos os arquivos CSV em paralelo usando ThreadPoolExecutor.

    Returns:
        List[str]: Lista de arquivos parquet gerados.
    """
    processed_files = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(process_partition, os.path.join(input_dir, file)): file
            for file in files
        }

        for future in as_completed(futures):
            file_name = futures[future]
            try:
                parquet_file = future.result()
                processed_files.append(parquet_file)
            except Exception as e:
                logger.error(f"Erro ao processar {file_name}: {e}")

    return processed_files


def main():
    logger.info("Início do ETL sequencial")
    time_seq_start = time.time()
    processed_seq = etl_sequential()
    time_seq_end = time.time()

    logger.info("Início do ETL paralelo")
    time_par_start = time.time()
    processed_par = etl_parallel()
    time_par_end = time.time()

    seq_duration = time_seq_end - time_seq_start
    par_duration = time_par_end - time_par_start

    compare_execution_times(seq_duration, par_duration)

    logger.info(f"Arquivos processados sequencialmente: {processed_seq}")
    logger.info(f"Arquivos processados paralelamente: {processed_par}")


if __name__ == "__main__":
    main()
