import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)
from utils.log_decorator import log_execution, logger
from utils.compare_times import compare_execution_times

banner_exercicio_4 = """
================================================================================
⬇️  EXERCÍCIO 4: DOWNLOAD CONCORRENTE DE ARQUIVOS (SIMULADO)
--------------------------------------------------------------------------------
⏳  Simula o download paralelo de 10 arquivos grandes com time.sleep()
⚙️  Utiliza ThreadPoolExecutor para execução concorrente e ganho de performance
================================================================================
"""

logger.success(banner_exercicio_4)

def simulate_download_time(file_id: int):
    """
    Simula o tempo de download de um arquivo com time.sleep()
    """
    logger.info(f"Iniciando download do arquivo {file_id}")
    time.sleep(1)
    logger.success(f"Download do arquivo {file_id} concluído com sucesso!")
    return f"Arquivo {file_id} baixado"


@log_execution
def sequential_download():
    """
    Simula o download sequencial de 10 arquivos grandes.
    
    Returns:
        list: Lista de mensagens de conclusão de cada download.
    """
    start_time = time.time()
    resultados = []
    for i in range(1, 11):
        resultado = simulate_download_time(i)
        resultados.append(resultado)
    end_time = time.time()
    final_time = end_time - start_time
    return final_time


@log_execution
def parallel_download():
    """
    Simula o download concorrente de 10 arquivos grandes usando ThreadPoolExecutor.
    
    Returns:
        list: Lista de mensagens de conclusão de cada download.
    """
    start_time = time.time()
    resultados = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(simulate_download_time, i): i for i in range(1, 11)}

        for future in as_completed(futures):
            try:
                resultado = future.result()
                resultados.append(resultado)
            except Exception as e:
                logger.error(f"Erro no download do arquivo {futures[future]}: {e}")
    end_time = time.time()
    final_time = end_time - start_time
    return final_time

def main():
    sequential_time = sequential_download()
    parallel_time = parallel_download()
    compare_execution_times(sequential_time, parallel_time)


if __name__ == "__main__":
    main()
