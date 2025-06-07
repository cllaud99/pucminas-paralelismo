import os
import pandas as pd
from faker import Faker
import sqlite3
from typing import List
import random

fake = Faker("pt_BR")

def generate_sales_data_for_month(month: int, year: int, num_records: int = 100) -> pd.DataFrame:
    """
    Gera um DataFrame fake com dados de vendas para um mês específico.

    Args:
        month (int): Mês (1 a 12).
        year (int): Ano.
        num_records (int): Número de registros a gerar.

    Returns:
        pd.DataFrame: Dados de vendas falsos.
    """
    records = []
    for _ in range(num_records):
        day = random.randint(1, 28)  # evita problemas com meses menores
        date = f"{year}-{month:02d}-{day:02d}"
        product = fake.word().capitalize()
        quantity = random.randint(1, 20)
        price = round(random.uniform(10.0, 200.0), 2)
        total = round(quantity * price, 2)

        records.append({
            "date": date,
            "product": product,
            "quantity": quantity,
            "price": price,
            "total": total
        })

    return pd.DataFrame(records)


def save_monthly_sales_data(
    output_dir: str,
    year: int = 2024,
    num_records_per_month: int = 100,
    file_format: str = "csv"
) -> List[str]:
    """
    Gera e salva arquivos de dados de vendas para 12 meses no formato CSV ou Parquet.

    Args:
        output_dir (str): Diretório onde os arquivos serão salvos.
        year (int): Ano para os dados.
        num_records_per_month (int): Registros por arquivo mensal.
        file_format (str): Formato do arquivo, pode ser 'csv' ou 'parquet'.

    Returns:
        List[str]: Lista com caminhos dos arquivos gerados.

    Raises:
        ValueError: Se o formato de arquivo não for suportado.
    """
    if file_format not in ("csv", "parquet"):
        raise ValueError("Formato inválido. Use 'csv' ou 'parquet'.")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_paths = []
    for month in range(1, 13):
        df = generate_sales_data_for_month(month, year, num_records_per_month)
        if file_format == "csv":
            file_path = os.path.join(output_dir, f"sales_{year}_{month:02d}.csv")
            df.to_csv(file_path, index=False)
        else:  # parquet
            file_path = os.path.join(output_dir, f"sales_{year}_{month:02d}.parquet")
            df.to_parquet(file_path, index=False)

        file_paths.append(file_path)

    return file_paths


def save_sales_data_to_sqlite(db_path: str, csv_files: List[str]) -> None:
    """
    Salva dados de múltiplos arquivos CSV em um banco SQLite, criando uma tabela
    para cada arquivo CSV, nomeando a tabela com base no nome do arquivo.

    Args:
        db_path (str): Caminho para o arquivo do banco SQLite.
        csv_files (List[str]): Lista de caminhos dos arquivos CSV para importar.

    Raises:
        Exception: Se ocorrer algum erro na conexão ou inserção no banco.
    """
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for file in csv_files:
            # Extrai nome do arquivo sem caminho e extensão para usar como nome da tabela
            table_name = os.path.splitext(os.path.basename(file))[0]
            # Substituir caracteres que não são válidos em nomes de tabelas (opcional)
            table_name = table_name.replace('-', '_').replace(' ', '_')

            # Cria tabela para cada arquivo
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT,
                    product TEXT,
                    quantity INTEGER,
                    price REAL,
                    total REAL
                )
            """)
            conn.commit()

            df = pd.read_csv(file)
            records = df.to_records(index=False)
            cursor.executemany(f"""
                INSERT INTO {table_name} (date, product, quantity, price, total)
                VALUES (?, ?, ?, ?, ?)
            """, records)
            conn.commit()

    except Exception as e:
        print(f"Erro ao salvar dados no SQLite: {e}")
        raise
    finally:
        if conn:
            conn.close()


def main():
    # Gerar CSVs fake
    output_folder_csv = "./data/inputs/simulated_datalake_files"
    generated_files = save_monthly_sales_data(output_folder_csv)
    output_folder_parquet = "./data/inputs/simulated_datalake_files_parquet"
    save_monthly_sales_data(output_folder_parquet, file_format="parquet")
    print(f"Arquivos CSV gerados: {generated_files}")

    # Salvar no SQLite
    sqlite_db_path = "./data/inputs/simulated_datalakedb/sales_data.db"
    save_sales_data_to_sqlite(sqlite_db_path, generated_files)
    print(f"Dados salvos no banco SQLite em: {sqlite_db_path}")
