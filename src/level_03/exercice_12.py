import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import requests

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.compare_times import compare_execution_times
from utils.log_decorator import log_execution, logger

BANNER = """
================================================================================
🌐  EXERCÍCIO: EXTRAÇÃO CONCORRENTE DE API PAGINADA
--------------------------------------------------------------------------------
🔄  Consulta uma API paginada usando múltiplas threads
📊  Junta todos os dados em uma única estrutura de saída
================================================================================
"""

logger.success(BANNER)


def fetch_page(base_url: str, year: int, month: int, page: int, per_page: int) -> Dict:
    """
    Consulta uma única página da API.

    Args:
        base_url (str): URL base da API (ex: http://localhost:8000/sales/).
        year (int): Ano desejado.
        month (int): Mês desejado.
        page (int): Número da página.
        per_page (int): Quantidade de registros por página.

    Returns:
        Dict: Resposta JSON da API para a página solicitada.
    """
    params = {"year": year, "month": month, "page": page, "per_page": per_page}
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    return response.json()


@log_execution
def fetch_all_sales_sequential(
    base_url: str, year: int, month: int, per_page: int = 10
) -> List[Dict]:
    """
    Consulta todas as páginas da API de forma sequencial.

    Args:
        base_url (str): URL base da API.
        year (int): Ano desejado.
        month (int): Mês desejado.
        per_page (int): Quantidade de registros por página.

    Returns:
        List[Dict]: Lista de todos os dados da API.
    """
    first_page = fetch_page(base_url, year, month, 1, per_page)
    total_pages = first_page["total_pages"]
    all_data = first_page["data"]

    for page in range(2, total_pages + 1):
        result = fetch_page(base_url, year, month, page, per_page)
        all_data.extend(result["data"])

    return all_data


@log_execution
def fetch_all_sales_concurrent(
    base_url: str, year: int, month: int, per_page: int = 10
) -> List[Dict]:
    """
    Consulta todas as páginas da API de forma concorrente usando threads.

    Args:
        base_url (str): URL base da API.
        year (int): Ano desejado.
        month (int): Mês desejado.
        per_page (int): Quantidade de registros por página.

    Returns:
        List[Dict]: Lista de todos os dados da API.
    """
    first_page = fetch_page(base_url, year, month, 1, per_page)
    total_pages = first_page["total_pages"]
    all_data = first_page["data"]

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(fetch_page, base_url, year, month, page, per_page): page
            for page in range(2, total_pages + 1)
        }

        for future in as_completed(futures):
            page = futures[future]
            try:
                result = future.result()
                all_data.extend(result["data"])
                logger.info(f"Página {page} processada com sucesso")
            except Exception as e:
                logger.error(f"Erro ao processar página {page}: {e}")

    return all_data


def main():
    base_url = "http://localhost:8000/sales/"
    year = 2024
    month = 6
    per_page = 100

    logger.info("Início da execução sequencial")
    start_seq = time.time()
    sales_seq = fetch_all_sales_sequential(base_url, year, month, per_page)
    end_seq = time.time()

    logger.info("Início da execução concorrente")
    start_conc = time.time()
    sales_conc = fetch_all_sales_concurrent(base_url, year, month, per_page)
    end_conc = time.time()

    compare_execution_times(end_seq - start_seq, end_conc - start_conc)

    logger.info(f"Total de registros sequencial: {len(sales_seq)}")
    logger.info(f"Total de registros concorrente: {len(sales_conc)}")

    logger.info("Exemplo de registros:")
    for sale in sales_conc[:5]:
        logger.debug(sale)


if __name__ == "__main__":
    main()
