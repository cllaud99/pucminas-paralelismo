import os
import sys
import time
import random
import asyncio
from typing import Any, Dict, List
from aiomultiprocess import Pool

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

from utils.log_decorator import log_execution, logger
from utils.compare_times import compare_execution_times

banner = """
================================================================================
🚀  EXERCÍCIO 13: Simulação de múltiplos produtores (leitura) e consumidores (escrita)
--------------------------------------------------------------------------------
🔄  Simulando múltiplos produtores e consumidores com batch async e multi-process
================================================================================
"""

logger.success(banner)

SENTINEL: Dict[str, Any] = {"type": "SENTINEL"}


async def generate_item(producer_id: int) -> Dict[str, Any]:
    """
    Gera um item simulado para produção.

    Args:
        producer_id (int): Identificador do produtor.

    Returns:
        Dict[str, Any]: Item gerado.
    """
    await asyncio.sleep(random.uniform(0.01, 0.05))
    return {
        "producer": producer_id,
        "value": round(random.uniform(10.0, 100.0), 2),
        "timestamp": time.time(),
    }


async def produce_data(
    queue: asyncio.Queue, producer_id: int, num_items: int = 10
) -> None:
    """
    Produtor assíncrono colocando itens na fila.

    Args:
        queue (asyncio.Queue): Fila compartilhada.
        producer_id (int): ID do produtor.
        num_items (int): Quantidade de itens a produzir.
    """
    for _ in range(num_items):
        item = await generate_item(producer_id)
        logger.debug(f"[PRODUCER {producer_id}] Produzindo item: {item}")
        await queue.put(item)
    logger.info(f"[PRODUCER {producer_id}] Finalizado.")


async def batch_insert(batch: List[Dict[str, Any]]) -> None:
    """
    Simula escrita em lote.

    Args:
        batch (List[Dict[str, Any]]): Lista de itens.
    """
    logger.info(f"[BATCH] Persistindo {len(batch)} itens...")
    await asyncio.sleep(0.1)


async def consume_data(
    queue: asyncio.Queue, consumer_id: int, batch_size: int = 5
) -> None:
    """
    Consumidor assíncrono com persistência em lote.

    Args:
        queue (asyncio.Queue): Fila compartilhada.
        consumer_id (int): ID do consumidor.
        batch_size (int): Tamanho do lote.
    """
    batch: List[Dict[str, Any]] = []
    while True:
        item = await queue.get()
        if item is SENTINEL:
            if batch:
                await batch_insert(batch)
                batch.clear()
            queue.task_done()
            break

        batch.append(item)
        if len(batch) >= batch_size:
            await batch_insert(batch)
            batch.clear()

        queue.task_done()

    logger.info(f"[CONSUMER {consumer_id}] Finalizado.")


@log_execution
async def run_asyncio(
    num_producers: int = 5, num_consumers: int = 5, num_items: int = 10
) -> float:
    """
    Orquestra execução assíncrona dos produtores e consumidores.

    Args:
        num_producers (int): Número de produtores.
        num_consumers (int): Número de consumidores.
        num_items (int): Itens por produtor.

    Returns:
        float: Tempo de execução.
    """
    start = time.time()
    queue: asyncio.Queue = asyncio.Queue()

    producers = [
        asyncio.create_task(produce_data(queue, pid, num_items))
        for pid in range(num_producers)
    ]
    consumers = [
        asyncio.create_task(consume_data(queue, cid))
        for cid in range(num_consumers)
    ]

    await asyncio.gather(*producers)

    for _ in range(num_consumers):
        await queue.put(SENTINEL)

    await queue.join()
    await asyncio.gather(*consumers)

    return time.time() - start


def main() -> None:
    logger.info("Executando versão asyncio + aiomultiprocess...")
    async_time = asyncio.run(run_asyncio(num_producers=9, num_consumers=9, num_items=10))
    logger.info(f"Tempo asyncio: {async_time:.3f}s")


if __name__ == "__main__":
    main()
