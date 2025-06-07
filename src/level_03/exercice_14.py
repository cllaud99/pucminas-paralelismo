import os
import sys
import time
import threading
import pandas as pd

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.log_decorator import logger, log_execution
from utils.compare_times import compare_execution_times


banner_exercicio_14 = """
================================================================================
🚀  EXERCÍCIO 14: BENCHMARKING SERIAL VS PARALELO
--------------------------------------------------------------------------------
⏱️  Comparando tempo de execução de uma pipeline de 3 estágios:
    - Ingestão de arquivos
    - Transformação de dados
    - Escrita em parquet
================================================================================
"""

logger.success(banner_exercicio_14)


def ingest_data(folder_path: str) -> pd.DataFrame:
    """
    Função que recebe uma pasta e devolve um dataframe
    Args:
        folder_path (str): Caminho da pasta contendo os arquivos CSV.

    Returns:
        pd.DataFrame: DataFrame contendo os dados dos arquivos CSV.
    """
    df = pd.DataFrame()
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        df = pd.concat([df, pd.read_csv(file_path)], ignore_index=True)

    logger.info("Arquivos lidos com sucesso.")
    return df


def transform_data(
    df: pd.DataFrame,
    discount_threshold: int = 10,
    discount_rate: float = 0.10,
    apply_month_name: bool = True
) -> pd.DataFrame:
    """
    Transforma o DataFrame aplicando regras de negócio:
    - Aplica desconto em registros com quantidade maior ou igual ao limite definido.
    - Adiciona o nome do mês em português, se ativado.

    Args:
        df (pd.DataFrame): DataFrame original.
        discount_threshold (int, opcional): Quantidade mínima para aplicar desconto. Default é 10.
        discount_rate (float, opcional): Percentual de desconto. Default é 0.10 (10%).
        apply_month_name (bool, opcional): Se True, adiciona coluna com nome do mês em português.

    Returns:
        pd.DataFrame: DataFrame transformado.
    """
    df["discount_applied"] = df["quantity"] >= discount_threshold
    df["discounted_total"] = df["total"].where(
        ~df["discount_applied"], round(df["total"] * (1 - discount_rate), 2)
    )
    return df


def save_dataframe_to_parquet(df: pd.DataFrame, output_path: str) -> None:
    """
    Salva o DataFrame em um arquivo parquet.

    Args:
        df (pd.DataFrame): DataFrame a ser salvo.
        output_path (str): Caminho do arquivo de saída.

    Returns:
        None
    """
    df.to_parquet(output_path, index=False)
    logger.info(f"DataFrame salvo em: {output_path}")


@log_execution
def run_pipeline_sequential(input_dir: str, output_path: str) -> float:
    """
    Executa a pipeline sequencialmente: ingestão, transformação, escrita.

    Args:
        input_dir (str): Diretório dos arquivos CSV.
        output_path (str): Caminho de saída do parquet.

    Returns:
        float: Tempo de execução.
    """
    start_time = time.time()
    df = ingest_data(input_dir)
    df_transformed = transform_data(df)
    save_dataframe_to_parquet(df_transformed, output_path)
    return time.time() - start_time


@log_execution
def run_pipeline_parallel(input_dir: str, output_path: str) -> float:
    """
    Executa a pipeline com paralelismo entre as etapas usando threading.

    Args:
        input_dir (str): Diretório dos arquivos CSV.
        output_path (str): Caminho de saída do parquet.

    Returns:
        float: Tempo de execução.
    """
    start_time = time.time()
    shared_data = {}

    def ingest():
        shared_data["df"] = ingest_data(input_dir)

    def transform():
        while "df" not in shared_data:
            time.sleep(0.1)  # espera ativa (não ideal para produção)
        shared_data["df_transformed"] = transform_data(shared_data["df"])

    def save():
        while "df_transformed" not in shared_data:
            time.sleep(0.1)
        save_dataframe_to_parquet(shared_data["df_transformed"], output_path)

    threads = [
        threading.Thread(target=ingest),
        threading.Thread(target=transform),
        threading.Thread(target=save)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    return time.time() - start_time


def main():
    input_dir = "data/inputs/simulated_datalake_files"
    output_dir = "data/outputs/exercicio_14"
    output_path = os.path.join(output_dir, "transformed_data.parquet")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    time_sequential = run_pipeline_sequential(input_dir, output_path)
    time_parallel = run_pipeline_parallel(input_dir, output_path)
    compare_execution_times(time_sequential, time_parallel)


if __name__ == "__main__":
    main()
