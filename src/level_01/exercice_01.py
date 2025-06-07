import os
import sys
import time
import threading

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from apis import weather_api
from utils.log_decorator import log_execution, logger
from utils.compare_times import compare_execution_times


banner_exercicio_1 = """
================================================================================
🧵  EXERCÍCIO 1: CRAWLER DE APIS CONCORRENTE COM THREADING
--------------------------------------------------------------------------------
🔄  Simulando a coleta de dados de 10 endpoints de uma API pública
================================================================================
"""

logger.success(banner_exercicio_1)


cities = [
    "Cajuru",
    "Belo Horizonte",
    "Sao Paulo",
    "Rio de Janeiro",
    "Curitiba",
    "Brasilia",
    "Porto Alegre",
    "Ribeirão Preto",
    "Joinville",
    "Florianopolis",
]

@log_execution
def requests_in_sequential():
    """
    Função que executa as requisições sequencialmente para obter temperaturas de cidades.
    """
    start_time = time.time()
    for city in cities:
        temperature = weather_api.get_temperature(city)
        logger.info(f"A temperatura atual em {city} é de {temperature} graus Celsius.")
    end_time = time.time()

    final_time = end_time - start_time
    logger.info(f"Tempo de execução: {final_time} segundos")  
    return final_time

@log_execution
def requests_in_parallel():
    """
    Função que executa as requisições em paralelo para obter temperaturas de cidades.
    """
    start_time = time.time()
    threads = []
    for city in cities:
        thread = threading.Thread(target=weather_api.get_temperature, args=(city,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    end_time = time.time()
    final_time = end_time - start_time
    logger.info(f"Tempo de execução: {final_time} segundos")
    return final_time


def main():
    parallel_time = requests_in_parallel()
    sequential_time = requests_in_sequential()
    compare_execution_times(sequential_time, parallel_time)

if __name__ == "__main__":
    main()