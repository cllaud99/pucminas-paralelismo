from utils.log_decorator import logger


def compare_execution_times(sequential_time: int, parallel_time: int):
    """
    Função para comparar os tempos de execução sequencial e paralelo.
    """
    logger.info(f"Tempo de execução sequencial: {sequential_time:.2f} segundos")
    logger.info(f"Tempo de execução paralelo: {parallel_time:.2f} segundos")

    # Calcula a diferença percentual de tempo entre as execuções
    time_difference_percentage = (
        (sequential_time - parallel_time) / sequential_time * 100
    )
    formatted_diff = f"{abs(time_difference_percentage):.2f}"

    if sequential_time < parallel_time:
        logger.success(
            f"Execução concluída com sucesso! "
            f"O tempo sequencial foi {formatted_diff}% mais rápido que o paralelo."
        )
    elif sequential_time > parallel_time:
        logger.success(
            f"Execução concluída com sucesso! "
            f"O tempo paralelo foi {formatted_diff}% mais rápido que o sequencial."
        )
    else:
        logger.success(
            "Execução concluída com sucesso! "
            "Os tempos sequencial e paralelo foram iguais."
        )
