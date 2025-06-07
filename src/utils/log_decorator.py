import os
import time
from functools import wraps

from loguru import logger

# ==== CONFIGURAÇÃO DO LOGGER ====
os.makedirs("logs", exist_ok=True)
logger.remove()

# Console (opcional)
logger.add(sink=lambda msg: print(msg, end=""), level="INFO", colorize=True)

# Log em arquivo (apenas SUCCESS e acima)
logger.add(
    "logs/log_execucao_{time:YYYY-MM-DD}.log",
    rotation="00:00",
    retention="90 days",
    level="SUCCESS",
    encoding="utf-8",
    enqueue=True,
    backtrace=True,
    diagnose=True,
)


# ==== DECORADOR DE LOGS ====
def log_execution(func):
    """
    Decorador para registrar logs de execução de uma função.

    Args:
        func (callable): Função a ser decorada.

    Returns:
        callable: Função decorada com logs.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Iniciando execução da função: {func.__name__}")
        logger.debug(f"Parâmetros: args={args}, kwargs={kwargs}")
        start_time = time.time()

        try:
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            logger.success(
                f"Função {func.__name__} executada com sucesso em {elapsed_time:.2f} segundos"
            )
            return result
        except Exception as e:
            logger.exception(f"Erro ao executar a função {func.__name__}: {e}")
            raise

    return wrapper


if __name__ == "__main__":

    # ==== EXEMPLO DE USO ====
    @log_execution
    def soma(a, b):
        return a + b

    @log_execution
    def erro():
        raise ValueError("Erro simulado")

    soma(5, 7)
    try:
        erro()
    except:
        pass
