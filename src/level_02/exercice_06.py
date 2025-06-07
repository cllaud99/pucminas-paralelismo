import numpy as np
import concurrent.futures
import time
import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.compare_times import compare_execution_times

from utils.log_decorator import logger

# Simulando grandes arrays (listas) de números
arrays = [np.random.randn(20_000_000) for _ in range(4)]  # 4 arrays grandes

def normalize_array(arr: np.ndarray) -> np.ndarray:
    """
    Normaliza um array usando z-score.

    Args:
        arr (np.ndarray): Array de números.

    Returns:
        np.ndarray: Array normalizado.
    """
    mean = np.mean(arr)
    std = np.std(arr)
    return (arr - mean) / std

def sequential_processing(arrays):
    """
    Processa os arrays sequencialmente.

    Args:
        arrays (list[np.ndarray]): Lista de arrays para normalizar.

    Returns:
        list[np.ndarray]: Lista de arrays normalizados.
    """
    results = []
    start_time = time.time()
    for arr in arrays:
        results.append(normalize_array(arr))
    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f"Tempo sequencial: {total_time:.2f} segundos")
    return total_time

def parallel_processing(arrays):
    """
    Processa os arrays em paralelo usando ProcessPoolExecutor.

    Args:
        arrays (list[np.ndarray]): Lista de arrays para normalizar.

    Returns:
        list[np.ndarray]: Lista de arrays normalizados.
    """
    start_time = time.time()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(executor.map(normalize_array, arrays))
    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f"Tempo paralelo: {total_time:.2f} segundos")
    return total_time

def main():
    logger.info("Iniciando processamento sequencial...")
    seq_time = sequential_processing(arrays)

    logger.info("Iniciando processamento paralelo...")
    parallel_time = parallel_processing(arrays)

    compare_execution_times(seq_time, parallel_time)

if __name__ == "__main__":
    main()
