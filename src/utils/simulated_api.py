from fastapi import FastAPI, Query
from typing import List
from pydantic import BaseModel
from functools import lru_cache
import os
import sys
import pandas as pd

# Ajuste do path para importar seu módulo local (ajuste conforme seu projeto)

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from faker_create_datasets import generate_sales_data_for_month


class SaleRecord(BaseModel):
    date: str
    product: str
    quantity: int
    price: float
    total: float


class PaginatedSalesResponse(BaseModel):
    year: int
    month: int
    page: int
    per_page: int
    total_records: int
    total_pages: int
    data: List[SaleRecord]


app = FastAPI()


@lru_cache(maxsize=24)  # Cache para até 24 meses diferentes (ex: 2 anos)
def cached_sales_data(year: int, month: int) -> pd.DataFrame:
    """Gera ou retorna do cache os dados de vendas para o mês/ano."""
    return generate_sales_data_for_month(month, year, 100)


@app.get("/sales/", response_model=PaginatedSalesResponse)
async def get_sales(
    year: int = Query(2024, ge=2000),
    month: int = Query(1, ge=1, le=12),
    per_page: int = Query(10, ge=1, le=100),
    page: int = Query(1, ge=1)
):
    """
    Retorna dados de vendas paginados para um mês e ano específicos.

    Args:
        year (int): Ano dos dados.
        month (int): Mês dos dados.
        per_page (int): Quantidade de registros por página.
        page (int): Página atual.

    Returns:
        PaginatedSalesResponse: Dados paginados de vendas.
    """
    all_data_df = cached_sales_data(year, month)

    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page

    paged_df = all_data_df.iloc[start_idx:end_idx]

    data = paged_df.to_dict(orient="records")

    total_pages = (len(all_data_df) + per_page - 1) // per_page

    return PaginatedSalesResponse(
        year=year,
        month=month,
        page=page,
        per_page=per_page,
        total_records=len(all_data_df),
        total_pages=total_pages,
        data=data
    )
