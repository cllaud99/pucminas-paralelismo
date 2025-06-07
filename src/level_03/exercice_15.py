import multiprocessing
import os
import sys
import time

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.compare_times import compare_execution_times
from utils.log_decorator import log_execution, logger

LOGS_DIR = "logs"

pattern = "error"


@log_execution
def analyze_log_file(file_path: str) -> int:
    """
    Lê um arquivo de log e conta quantas vezes o padrão de erro crítico aparece.

    Args:
        file_path (str): Caminho do arquivo de log.

    Returns:
        int: Quantidade de ocorrências do padrão.
    """
    count = 0
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            if pattern in line.lower():
                count += 1
    logger.info(f"{os.path.basename(file_path)} - Encontrados {count} erros críticos.")
    return count


import time


def analyze_logs_sequential(log_files: list[str]) -> float:
    """
    Analisa arquivos de log sequencialmente e retorna o tempo de execução.

    Args:
        log_files (list[str]): Lista de caminhos dos arquivos.

    Returns:
        float: Tempo total de execução em segundos.
    """
    start_time = time.time()
    for file_path in log_files:
        analyze_log_file(file_path)  # mantém o log interno da contagem
    end_time = time.time()

    return end_time - start_time


def analyze_logs_parallel(log_files: list[str]) -> float:
    """
    Analisa arquivos de log em paralelo usando multiprocessing e retorna o tempo de execução.

    Args:
        log_files (list[str]): Lista de caminhos dos arquivos.

    Returns:
        float: Tempo total de execução em segundos.
    """
    start_time = time.time()
    with multiprocessing.Pool() as pool:
        pool.map(analyze_log_file, log_files)  # mantém o log interno da contagem
    end_time = time.time()

    return end_time - start_time


def main():
    log_files = [
        os.path.join(LOGS_DIR, f) for f in os.listdir(LOGS_DIR) if f.endswith(".log")
    ]
    logger.info(f"Arquivos de log para análise: {log_files}")

    time_parallel = analyze_logs_parallel(log_files)
    time_sequential = analyze_logs_sequential(log_files)

    compare_execution_times(time_sequential, time_parallel)


if __name__ == "__main__":
    main()
