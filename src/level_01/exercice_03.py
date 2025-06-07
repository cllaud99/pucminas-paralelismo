import os
import sys
import time
import threading
import csv

from datetime import datetime
from urllib.request import urlopen
from urllib.error import URLError

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.log_decorator import log_execution, logger
from utils.compare_times import compare_execution_times

banner_exercicio_3 = """
================================================================================
⏱️  EXERCÍCIO 3: MONITORAMENTO DE TEMPO DE RESPOSTA COM MÚLTIPLAS THREADS
--------------------------------------------------------------------------------
🌐  Testa o tempo de resposta de diferentes URLs em paralelo
📄  Salva os resultados em um arquivo CSV
================================================================================
"""

logger.success(banner_exercicio_3)


# Lista de URLs para teste
urls = [
    "https://www.google.com",
    "https://www.github.com",
    "https://www.python.org",
    "https://www.wikipedia.org",
    "https://www.microsoft.com",
    "https://www.apple.com",
    "https://www.stackoverflow.com",
    "https://www.linkedin.com",
    "https://www.facebook.com",
    "https://www.twitter.com",
    "https://www.instagram.com"
]


results = []
lock = threading.Lock()


def check_url_response_time(url: str) -> None:
    """
    Função que mede o tempo de resposta de uma URL específica.

    Args:
        url (str): Endereço da URL a ser testada.
    """
    start = time.time()
    status = None
    try:
        with urlopen(url, timeout=10) as response:
            status = response.status
    except URLError as e:
        status = f"Erro: {e.reason}"
    except Exception as e:
        status = f"Erro inesperado: {str(e)}"
        logger.error(f"Erro ao acessar URL {url}: {e}", exc_info=True)
    finally:
        end = time.time()
        duration = round(end - start, 3)

        with lock:
            results.append({"url": url, "tempo_resposta": duration, "status": status})
        logger.info(f"URL: {url} | Tempo: {duration}s | Status: {status}")


@log_execution
def check_urls_sequential():
    """
    Verifica URLs de forma sequencial.
    """
    start = time.time()
    for url in urls:
        check_url_response_time(url)
    end = time.time()
    return end - start


@log_execution
def check_urls_parallel():
    """
    Verifica URLs utilizando múltiplas threads.
    """
    start = time.time()
    threads = []

    for url in urls:
        thread = threading.Thread(target=check_url_response_time, args=(url,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    end = time.time()
    return end - start


def save_results_to_csv():
    """
    Salva os resultados de tempo de resposta em um arquivo CSV.
    """
    folder_path = os.path.join("data/outputs/exercicio_03")
    os.makedirs(folder_path, exist_ok=True)
    file_name = f"resultados_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_path = os.path.join(folder_path, file_name)


    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["url", "tempo_resposta", "status"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    logger.info(f"Resultados salvos em: {output_path}")


def main():
    """
    Função principal que executa o monitoramento sequencial e paralelo.
    """
    logger.info("Iniciando verificação de URLs.")
    parallel_time = check_urls_parallel()
    sequential_time = check_urls_sequential()
    compare_execution_times(sequential_time, parallel_time)
    save_results_to_csv()


if __name__ == "__main__":
    main()
