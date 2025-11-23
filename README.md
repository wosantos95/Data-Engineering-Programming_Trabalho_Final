# Data Engineering Programming – Trabalho Final (Pipeline PySpark)

Este README detalha o passo a passo acadêmico para recriar todo o projeto de pipeline de dados em PySpark.

## Estrutura do Projeto

    data-engineering-pyspark/
    │
    ├── main.py
    ├── pyproject.toml
    ├── requirements.txt
    │
    ├── data/
    │   ├── input/
    │   │   ├── pagamentos-[*].json.gz
    │   │   └── pedidos-[*].csv.gz
    │   │ 
    │   └── output/
    │       └── relatorio_parquet
    │           └──.part-[*].snappy.parquet.crc
    │ 
    │ 
    ├── src/
    │   ├── business_logic/
    │   │   └── sales_report_logic.py
    │   │
    │   ├── config/
    │   │   └── spark_config.py
    │   │
    │   ├── io/
    │   │   └── data_io.py
    │   │
    │   ├── orchestration/
    │   │   └── pipeline_orchestrator.py
    │   │
    │   └── spark_manager/
    │       └── session_manager.py
    │
    └── tests/
        └── test_sales_report_logic.py


## 1. Preparação do Ambiente

### 1.1 Criar pasta do projeto

```bash
mkdir Data-Enginnering-Programming_Trabalho_Final
cd Data-Enginnering-Programming_Trabalho_Final
```

### 1.2 Criar ambiente virtual
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 1.3 Instalar dependências
Crie o arquivo `requirements.txt`:

```
pyspark==4.0.0
pytest>=7.0.0,<8.0.0
```
Instale:
```bash
pip install -r requirements.txt
```

---

## 2. Estrutura do Projeto

```bash
mkdir -p src/business_logic
mkdir -p src/config
mkdir -p src/io
mkdir -p src/orchestration
mkdir -p src/spark_manager
mkdir -p tests
mkdir -p data/input
mkdir -p data/output
```

Estrutura final:
```
Data-Enginnering-Programming_Trabalho_Final/
  src/
    business_logic/
    config/
    io/
    orchestration/
    spark_manager/
  tests/
  data/input/
  data/output/
  main.py
  requirements.txt
  pyproject.toml
```

---

## 3. Criando Módulos

### 3.1 Spark Session Manager (`src/spark_manager/session_manager.py`)
```python
from pyspark.sql import SparkSession

class SparkSessionManager:
    def __init__(self, app_name: str, master: str):
        self.app_name = app_name
        self.master = master
        self._spark = None

    def get_spark(self):
        if self._spark is None:
            self._spark = (
                SparkSession.builder
                .appName(self.app_name)
                .master(self.master)
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate()
            )
        return self._spark

    def stop(self):
        if self._spark:
            self._spark.stop()
            self._spark = None
```

### 3.2 Configurações (`src/config/spark_config.py`)
```python
from dataclasses import dataclass

@dataclass
class AppConfig:
    pagamentos_path: str = "data/input/pagamentos*.json.gz"
    pedidos_path: str = "data/input/pedidos*.csv.gz"
    output_path: str = "data/output/relatorio_parquet"
    ano_relatorio: int = 2025
    app_name: str = "FIAP_Trabalho_Final"
    master: str = "local[*]"
```

### 3.3 IO de Dados (`src/io/data_io.py`)
```python
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, LongType
)

class DataIO:
    def __init__(self, spark):
        self.spark = spark

    def read_pagamentos(self, path):
        schema = StructType([
            StructField("id_pedido", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("valor_pagamento", DoubleType(), True),
            StructField("status", BooleanType(), True),
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), True),
                StructField("score", DoubleType(), True)
            ])),
            StructField("data_processamento", TimestampType(), True)
        ])
        return self.spark.read.schema(schema).json(path)

    def read_pedidos(self, path):
        schema = StructType([
            StructField("id_pedido", StringType(), True),
            StructField("produto", StringType(), True),
            StructField("valor_unitario", DoubleType(), True),
            StructField("quantidade", LongType(), True),
            StructField("data_criacao", TimestampType(), True),
            StructField("uf", StringType(), True),
            StructField("id_cliente", LongType(), True),
        ])
        return self.spark.read.option("header", "true").option("sep", ";").schema(schema).csv(path)

    def write_parquet(self, df, path):
        df.write.mode("overwrite").parquet(path)
```

### 3.4 Lógica de Negócio (`src/business_logic/sales_report_logic.py`)
```python
import logging
from pyspark.sql.functions import col, year

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class SalesReportLogic:
    def __init__(self, io_manager, config):
        self.io = io_manager
        self.config = config

    def build_report(self):
        """Método principal que lê os dados do IO e retorna o relatório"""
        try:
            logger.info("Lendo pagamentos...")
            pagamentos = self.io.read_pagamentos(self.config.pagamentos_path)

            logger.info("Lendo pedidos...")
            pedidos = self.io.read_pedidos(self.config.pedidos_path)

            return self._process_dataframes(pedidos, pagamentos)

        except Exception as e:
            logger.exception("Erro ao processar relatório")
            raise

    def _process_dataframes(self, pedidos_df, pagamentos_df):
        """Processa DataFrames recebidos e retorna o relatório"""
        pagamentos_filtrados = pagamentos_df.filter(
            (col("status") == False) &
            (col("avaliacao_fraude.fraude") == False)
        )

        pedidos_df = pedidos_df.withColumn(
            "valor_total_pedido",
            col("valor_unitario") * col("quantidade")
        )

        joined = pedidos_df.join(pagamentos_filtrados, "id_pedido", "inner")
        joined = joined.filter(year(col("data_criacao")) == self.config.ano_relatorio)

        result = joined.select(
            col("id_pedido"),
            col("uf").alias("estado_uf"),
            col("forma_pagamento"),
            col("valor_total_pedido"),
            col("data_criacao").alias("data_pedido")
        ).orderBy("estado_uf", "forma_pagamento", "data_pedido")

        return result
```

### 3.5 Orquestrador (`src/orchestration/pipeline_orchestrator.py`)
```python
import logging
logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self, processor, io_manager, spark_manager, config):
        self.processor = processor
        self.io = io_manager
        self.spark_manager = spark_manager
        self.config = config

    def run(self):
        spark = self.spark_manager.get_spark()
        try:
            logger.info("Iniciando pipeline...")
            df = self.processor.build_report()
            logger.info("Gravando parquet...")
            self.io.write_parquet(df, self.config.output_path)
        finally:
            logger.info("Encerrando sessão Spark")
            self.spark_manager.stop()
```

### 3.6 Arquivo principal (`main.py`)
```python
from src.config.spark_config import AppConfig
from src.spark_manager.session_manager import SparkSessionManager
from src.io.data_io import DataIO
from src.business_logic.sales_report_logic import SalesReportLogic
from src.orchestration.pipeline_orchestrator import PipelineOrchestrator

def main():
    cfg = AppConfig()
    spark_manager = SparkSessionManager(cfg.app_name, cfg.master)
    spark = spark_manager.get_spark()
    io_manager = DataIO(spark)
    processor = SalesReportLogic(io_manager, cfg)
    orchestrator = PipelineOrchestrator(processor, io_manager, spark_manager, cfg)
    orchestrator.run()
if __name__ == "__main__":
    main()

```

---

## 4. Inserir Arquivos de Entrada
Coloque os CSV/JSON de pedidos e pagamentos em `data/input/`.

---

## 5. Testes Unitários
Crie `tests/test_sales_report_logic.py` com PySpark para validar o processamento.

---

## 6. Executar Pipeline
```bash
python main.py
```
Arquivos Parquet serão gerados em `data/output/relatorio_parquet`.

---

## 7. Validar Resultado
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/output/relatorio_parquet")
df.show()
```

---

## 8. Conclusão
Este passo a passo recria todo o projeto, desde a configuração do ambiente, criação dos módulos, orquestração