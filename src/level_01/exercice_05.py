import sqlite3
import threading
import pandas as pd
import os
import sys
import time

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.log_decorator import log_execution, logger
from utils.compare_times import compare_execution_times

DB_PATH = "data/inputs/simulated_datalakedb/sales_data.db"


banner_exercicio_5 = """
================================================================================
🗄️  EXERCÍCIO 5: CONSULTA A MÚLTIPLAS BASES DE DADOS (SIMULADA)
--------------------------------------------------------------------------------
🔗  Simula 5 conexões independentes a bancos de dados usando threading
📊  Cada thread lê uma "tabela" simulada e imprime um resumo dos dados
================================================================================
"""

logger.success(banner_exercicio_5)


def get_tables_from_db(db_path: str) -> list[str]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables


def query_table(nome_tabela: str):
    """
    Consulta uma tabela no banco SQLite e loga um resumo com pandas.

    Args:
        nome_tabela (str): Nome da tabela a ser consultada.
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(f"SELECT * FROM {nome_tabela}", conn)
    conn.close()

    logger.info(f"📊 Resumo da tabela {nome_tabela}:\n{df.describe()}")


tabelas = get_tables_from_db(DB_PATH)

@log_execution
def query_sequencial():
    """
    Consulta todas as tabelas sequencialmente.
    """
    start_time = time.time()
    for tabela in tabelas:
        query_table(tabela)
    end_time = time.time()

    return end_time - start_time

@log_execution
def query_parallel():
    """
    Consulta todas as tabelas em paralelo usando threads.
    """
    start_time = time.time()
    threads = []
    for tabela in tabelas:
        thread = threading.Thread(target=query_table, args=(tabela,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    end_time = time.time()
    return end_time - start_time

def main():

    tempo_paralelo = query_parallel()
    tempo_sequencial = query_sequencial()
    compare_execution_times(tempo_sequencial, tempo_paralelo)

if __name__ == "__main__":
    main()
