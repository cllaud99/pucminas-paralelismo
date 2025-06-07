# Trabalho: Fundamentos de Python — Paralelismo  
**PUC Minas - Engenharia de Dados**  
**Disciplina:** Fundamentos de Python  
**Aluno:** Cláudio Pontes  

---

### 📌 Descrição  
Este projeto foi desenvolvido como parte da disciplina de Fundamentos de Python com foco em técnicas de paralelismo e concorrência. O objetivo é demonstrar o uso de threads e processos para acelerar tarefas típicas de Engenharia de Dados, como ingestão de arquivos, chamadas a APIs, transformação de dados, simulações de ETL e benchmarking de performance.

O projeto está dividido em três níveis de complexidade:

- **Nível 1 — Fundamentos com foco em I/O**  
- **Nível 2 — Processamento CPU-bound com dados**  
- **Nível 3 — Cenários avançados e integrações**

---

### 🧠 Exercícios implementados  
Cada nível contém 5 exercícios numerados conforme a proposta da disciplina:

**level_01/**  
1. Crawler concorrente de APIs usando threading.Thread para coletar dados de múltiplos endpoints.

2. Ingestão paralela de vários arquivos CSV com ThreadPoolExecutor e Pandas.

3. Monitoramento simultâneo de tempo de resposta de diversas URLs, com gravação dos resultados.

4. Simulação de downloads concorrentes usando ThreadPoolExecutor e delays artificiais.

5. Consulta paralela simulada a múltiplas bases de dados com threads, gerando resumos independentes.

**level_02/**

6. Transformação pesada de dados usando ProcessPoolExecutor para operações CPU-bound.
7. Aplicação paralela de funções complexas em partes de DataFrames com processos.
8. Conversão paralela de arquivos Parquet para CSV, processando cada arquivo individualmente.
9. Cálculo paralelo de agregações estatísticas (soma, média, desvio) em grandes DataFrames.
10. Pipeline multiprocessamento: transformação em um processo e persistência em outro, usando pipes ou queues.

**level_03/**

11. ETL paralelo por partição de dados, simulando processamento distribuído em Data Lake.
12. Extração concorrente de dados paginados via API, unindo resultados em paralelo com threads.
13. Simulação de múltiplos produtores e consumidores usando queue.Queue para leitura e escrita concorrentes.
14. Benchmark comparativo entre pipeline serial e paralela em três etapas (ingestão, transformação, escrita).
15. Sistema paralelizado de alertas baseado em monitoramento de logs e detecção de padrões críticos.

---

### 📁 Estrutura de pastas  
```markdown
.
├── README.md
├── data/ # Dados de entrada e saída
├── logs/ # Arquivos de log gerados pelos exercícios
├── poetry.lock
├── pyproject.toml
└── src/
    ├── apis/
    │   └── weather_api.py
    ├── level_01/
    │   └── exercice_01.py ... exercice_05.py
    ├── level_02/
    │   └── exercice_06.py ... exercice_10.py
    ├── level_03/
    │   └── exercice_11.py ... exercice_15.py
    ├── utils/
    │   ├── faker_create_datasets.py
    │   ├── simulated_api.py
    │   ├── compare_times.py
    │   └── log_decorator.py
    └── main.py # Script principal para orquestrar todos os níveis
```

---

### ▶️ Como executar  

Gere uma chave de api em: weatherapi.com (importante para o exercicio 01 somente)

Instale o poetry:   
https://python-poetry.org/docs/#installation


Clone o repositório:

```bash
git clone https://github.com/seu-usuario/pucminas-paralelismo.git
cd pucminas-paralelismo
```

Instale as dependências (via Poetry):

```bash
poetry install
```

Execute o projeto:

```bash
poetry run python src/main.py
```

Este comando:

- Gera datasets simulados com faker_create_datasets.py  
- Inicia uma API local simulada (FastAPI)  
- Executa todos os exercícios dos três níveis em sequência  

---

### 🛠 Principais bibliotecas utilizadas

- **Python 3.12**  
  Versão moderna e atual da linguagem, garantindo suporte a recursos recentes para melhor desempenho e produtividade.

- **requests**  
  Biblioteca para realizar chamadas HTTP, essencial para a comunicação com APIs externas e internas.

- **python-dotenv**  
  Para gerenciamento seguro e prático de variáveis de ambiente, facilitando a configuração do projeto.

- **taskipy**  
  Ferramenta para orquestração e execução de tarefas, ajudando na automação dos comandos do projeto.

- **loguru**  
  Biblioteca poderosa e simples para logging, utilizada junto com decoradores para monitoramento e debug eficientes.

- **pandas**  
  Biblioteca fundamental para manipulação e análise de dados, usada nas transformações e processamento dos datasets.

- **faker**  
  Geração de dados sintéticos realistas para testes e simulações, garantindo flexibilidade nos exercícios.

- **fastapi**  
  Framework moderno e rápido para criação da API simulada, suportando chamadas concorrentes.

- **uvicorn**  
  Servidor ASGI leve para rodar a API FastAPI localmente durante a execução do projeto.

- **aiomultiprocess**  
  Biblioteca para facilitar o uso de multiprocessamento assíncrono, aumentando a eficiência no processamento paralelo.

---

### ✅ Task de formatação  
```bash
poetry run task format
```

Executa o isort e black para manter o código limpo e padronizado.

---

### 🌐 API Simulada  
Durante a execução, uma API FastAPI é iniciada localmente na porta 8574, simulando endpoints para exercícios com chamadas HTTP concorrentes.

---

### 📌 Observações  
- Todos os exercícios foram projetados com foco em boas práticas de Engenharia de Dados.  
- O projeto utiliza logs para acompanhamento e debug, os logs foram versionados para demostrar a funcionalidade do exercicio 15
- Simulações foram aplicadas para representar cenários reais de ETL e monitoramento.
