# 🚀 Projeto Final PySpark: Relatório de Pedidos Críticos

## 🎓 1. Informações Essenciais

| Detalhe | Valor |
| :--- | :--- |
| **Disciplina** | Data Engineering Programming |
| **Professor** | Marcelo Barbosa Pinto |
| **Integrante(s)** | [Seu Nome Completo] |
| **RM** | [Seu Número de RM] |
| **Link do GitHub** | [COLE AQUI O LINK DO SEU REPOSITÓRIO PÚBLICO] |

---

## 2. 🎯 Escopo de Negócio e Objetivo

O objetivo deste projeto é construir um pipeline PySpark modular para processar dados de **Pagamentos** (`.json.gz`) e **Pedidos** (`.csv.gz`) e gerar um relatório focado em pedidos com falha.

### Critérios de Filtragem (Lógica de Negócios)

O relatório final (`relatorio_pedidos_2025.parquet`) deve incluir apenas pedidos que satisfaçam *todas* as seguintes condições:

1.  **Status do Pagamento:** Pagamento **recusado** (`status` = `false`).
2.  **Avaliação de Fraude:** Fraude classificada como **legítima** (`fraude` = `false`).
3.  **Ano de Referência:** Pedidos feitos no ano de **2025**.

O resultado deve ser ordenado por **Estado (UF)**, **Forma de Pagamento** e **Data do Pedido**.

---

## 3. 🛠️ Guia Passo a Passo: Criação e Desenvolvimento

Esta seção detalha a configuração e a implementação do projeto, seguindo os padrões de **POO** e **Injeção de Dependências**.

### 3.1. Estrutura do Ambiente e Setup

| Passo | Descrição | Comandos no Terminal |
| :--- | :--- | :--- |
| **Criar e Estruturar** | Cria o diretório raiz e todos os pacotes (`src/`, `config/`, `tests/`) necessários para o projeto POO. | `mkdir data-engineering-pyspark`<br>`cd data-engineering-pyspark`<br>`mkdir -p config src/spark_manager src/io src/business_logic src/orchestration tests data/input src/data/output` |
| **Init & Files** | Cria os arquivos `__init__.py` (pacotes) e os arquivos de gerenciamento. | `find . -type d \( -name 'config' -o -name 'src' ... \) -exec touch {}/__init__.py \;`<br>`touch requirements.txt src/main.py` |
| **Setup Python** | Cria o ambiente virtual (`venv`) e instala as dependências principais (`pyspark`, `pytest`). | `python3 -m venv .venv`<br>`source .venv/bin/activate`<br>`pip install -r requirements.txt` |
| **Datasets** | Confirma que os arquivos `.gz` (2024/2025) estão na pasta de entrada. | (Verificar pasta `data/input/`) |

### 3.2. Implementação do Código (POO)

O projeto é dividido em classes com responsabilidades únicas, promovendo modularidade:

| Arquivo/Classe | Responsabilidade Principal | Critérios Atendidos |
| :--- | :--- | :--- |
| `config/spark_config.py` | Armazena configurações centralizadas (e.g., `target_year=2025`). | **Configurações Centralizadas** (4) |
| `src/io/data_io.py` | Realiza I/O. Contém os **Schemas Explícitos** (`ORDERS_SCHEMA`, `PAYMENTS_SCHEMA`). | **Schemas Explícitos** (1), I/**O** (6) |
| `src/business_logic/sales_report_logic.py` | Implementa a filtragem e ordenação. Utiliza **Try/Catch** (10) e **Logging** (9) para registro de etapas. | **Lógica de Negócios** (7), Logging (9), Erros (10) |
| `src/orchestration/pipeline_orchestrator.py` | Sequencia a leitura, transformação e escrita do relatório. | **Orquestração** (8) |
| **Observação:** Todas as classes atendem ao requisito de **Orientação a Objetos** (Critério 2). |

### 3.3. Injeção de Dependências (DI)

O **`src/main.py`** atua como o **Aggregation Root** (Critério 3).

1.  **Instanciação:** Todas as dependências (`SparkConfig`, `DataIO`, `SalesReportLogic`, etc.) são criadas em `main.py`.
2.  **Injeção:** O `main.py` passa (injeta) essas instâncias no construtor do `PipelineOrchestrator`, que então coordena a execução.

---

## 4. 🚀 Execução e Testes Unitários

### 4.1. Execução do Pipeline

A execução é feita através do ponto de entrada do projeto:

```bash
# Executado a partir do diretório data-engineering-pyspark/
python src/main.py
