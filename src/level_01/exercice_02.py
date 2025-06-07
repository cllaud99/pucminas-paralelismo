import pandas as pd
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.log_decorator import log_execution, logger
from utils.compare_times import compare_execution_times

pasta_csv = "data/inputs/simulated_datalake_files"
files = os.listdir(pasta_csv)


banner_exercicio_2 = """
================================================================================
📂  EXERCÍCIO 2: INGESTÃO DE MÚLTIPLOS ARQUIVOS CSV COM THREADPOOL EXECUTOR
--------------------------------------------------------------------------------
🧮  Leitura de 10 arquivos .csv pequenos com Pandas em paralelo
================================================================================
"""

logger.success(banner_exercicio_2)

@log_execution
def sequential_read_csv():
    """
    Lê os arquivos CSV sequencialmente, imprime os DataFrames, e retorna o tempo gasto.
    """
    start_time = time.time()

    for file in files:
        df = pd.read_csv(os.path.join(pasta_csv, file))
        print(df)

    end_time = time.time()
    final_time = end_time - start_time
    return final_time

@log_execution
def parallel_read_csv():
    """
    Lê os arquivos CSV em paralelo usando ThreadPoolExecutor, imprime resumo dos DataFrames e retorna o tempo gasto.
    """
    start_time = time.time()
    dfs = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(pd.read_csv, os.path.join(pasta_csv, file)): file for file in files}

        for future in as_completed(futures):
            arquivo = futures[future]
            try:
                df = future.result()
                dfs.append(df)
            except Exception as e:
                logger.error(f"Erro ao ler {arquivo}: {e}")

    end_time = time.time()
    final_time = end_time - start_time
    return final_time


def main():
    logger.info("Leitura de arquivos CSV")
    sequential_time = sequential_read_csv()
    parallel_time = parallel_read_csv()

    compare_execution_times(sequential_time, parallel_time)


if __name__ == "__main__":
    main()
