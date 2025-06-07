import importlib
import os
import sys
import threading
from typing import List

import uvicorn


def run_setup() -> None:
    """
    Roda o setup inicial, executando o main() do faker_create_datasets.py.
    """
    try:
        setup_module = importlib.import_module("utils.faker_create_datasets")
        print("Executando setup: utils.faker_create_datasets.main()")
        setup_module.main()
    except Exception as e:
        print(f"Erro ao executar o setup faker_create_datasets: {e}")
        raise  # se quiser interromper aqui caso o setup falhe


def run_api():
    """
    Roda a API FastAPI em uma thread separada para não bloquear o fluxo.
    """
    api_module = importlib.import_module("utils.simulated_api")
    app = getattr(api_module, "app")  # Assume que a FastAPI está exposta como "app"

    def start_uvicorn():
        uvicorn.run(app, host="127.0.0.1", port=8574, log_level="info")

    thread = threading.Thread(target=start_uvicorn, daemon=True)
    thread.start()
    print("API FastAPI rodando em background na porta 8574")


def run_exercises(base_path: str, levels: List[str]) -> None:
    """
    Executa a função main() de todos os exercícios nas pastas especificadas.

    Args:
        base_path (str): Caminho base da pasta src onde estão os níveis.
        levels (List[str]): Lista com os nomes das pastas de níveis (level_01, level_02, etc).
    """
    for level in levels:
        level_path = os.path.join(base_path, level)
        if not os.path.isdir(level_path):
            print(f"Pasta {level_path} não encontrada, pulando...")
            continue

        # Lista arquivos exercício
        files = [
            f
            for f in os.listdir(level_path)
            if f.startswith("exercice_") and f.endswith(".py")
        ]
        files.sort()  # garante ordem numérica

        for file in files:
            module_name = file[:-3]
            full_module = f"{level}.{module_name}"
            try:
                module = importlib.import_module(full_module)
                print(f"Executando {full_module}.main()")
                module.main()
            except Exception as e:
                print(f"Erro ao executar {full_module}: {e}")


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    run_setup()
    levels = ["level_01", "level_02", "level_03"]
    run_api()
    run_exercises(base_dir, levels)
