# FIAP - Data Engineering Programming – Trabalho Final

## Objetivo
Desenvolver um projeto PySpark que gere um relatório de pedidos de venda filtrando apenas:
- Pagamentos recusados (`status=false`)
- Pagamentos legítimos (`fraude=false`)
- Pedidos do ano de 2025

O relatório terá:
1. ID do pedido
2. Estado (UF)
3. Forma de pagamento
4. Valor total do pedido
5. Data do pedido

O relatório será salvo em **formato Parquet**, ordenado por UF, forma de pagamento e data.

---

## Estrutura do Projeto

data-engineering-pyspark/
├── config/
│ └── spark_config.py
├── src/
│ ├── business_logic/
│ │ └── sales_report_logic.py
│ ├── io/
│ │ └── data_io.py
│ ├── orchestration/
│ │ └── pipeline_orchestrator.py
│ ├── spark_manager/
│ │ └── session_manager.py
│ └── main.py
├── tests/
│ └── test_sales_report_logic.py
├── data/
│ └── download_data.sh
├── pyproject.toml
├── requirements.txt
└── README.md

bash
Copiar código

---

## Passo a passo para replicar

### 1. Criar pastas principais

```bash
mkdir -p ~/environment/data-engineering-pyspark
cd ~/environment/data-engineering-pyspark
mkdir -p data/input
mkdir -p data/output
mkdir -p src/business_logic src/io src/orchestration src/spark_manager src/config
mkdir -p tests
2. Criar o script para baixar os datasets
Crie o arquivo data/download_data.sh com o conteúdo:

bash
Copiar código
#!/usr/bin/env bash
set -e

ROOT="$PWD"
INPUT_DIR="$ROOT/data/input"

echo "🧽 Limpando diretórios..."
rm -rf "$ROOT/data/tmp-pagamentos" "$ROOT/data/tmp-pedidos"
mkdir -p "$INPUT_DIR"
rm -rf "$INPUT_DIR"/*

echo ""
echo "⬇ Baixando TODOS os arquivos de PAGAMENTOS (via API GitHub)..."

curl -s https://api.github.com/repos/infobarbosa/dataset-json-pagamentos/contents/data/pagamentos \
| grep "download_url" \
| cut -d '"' -f 4 \
| while read url; do
      echo "Baixando: $(basename $url)"
      curl -L "$url" -o "$INPUT_DIR/$(basename $url)"
  done

echo ""
echo "⬇ Baixando TODOS os arquivos de PEDIDOS (pasta data/pedidos)..."

curl -s https://api.github.com/repos/infobarbosa/datasets-csv-pedidos/contents/data/pedidos \
| grep "download_url" \
| cut -d '"' -f 4 \
| while read url; do
      echo "Baixando: $(basename $url)"
      curl -L "$url" -o "$INPUT_DIR/$(basename $url)"
  done

echo ""
echo "📂 Arquivos baixados:"
ls -lh "$INPUT_DIR"

echo ""
echo "✅ Processo concluído com sucesso!"
Depois, dê permissão de execução:

bash
Copiar código
chmod +x data/download_data.sh
E execute:

bash
Copiar código
./data/download_data.sh
3. Criar o ambiente Python
bash
Copiar código
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
4. Estrutura de código
Configuração (src/config/spark_config.py)
python
Copiar código
from dataclasses import dataclass

@dataclass
class AppConfig:
    pagamentos_path: str = "data/input/pagamentos*.json.gz"
    pedidos_path: str = "data/input/pedidos*.csv.gz"
    output_path: str = "data/output/relatorio_parquet"
    ano_relatorio: int = 2025
    app_name: str = "FIAP_Trabalho_Final"
    master: str = "local[*]"
Lógica de Negócio (src/business_logic/sales_report_logic.py)
python
Copiar código
import logging
from pyspark.sql.functions import col, year

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class SalesReportLogic:
    def __init__(self, io_manager, config):
        self.io = io_manager
        self.config = config

    def build_report(self):
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
Data IO (src/io/data_io.py)
python
Copiar código
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, LongType

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
Spark Manager (src/spark_manager/session_manager.py)
python
Copiar código
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
Pipeline Orchestrator (src/orchestration/pipeline_orchestrator.py)
python
Copiar código
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
Main (src/main.py)
python
Copiar código
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
5. Executar o pipeline
bash
Copiar código
python src/main.py
O relatório será gerado em:

bash
Copiar código
data/output/relatorio_parquet
6. Rodar testes unitários
bash
Copiar código
pytest -v tests/test_sales_report_logic.py
7. Visualizar primeiras linhas do relatório
python
Copiar código
from src.spark_manager.session_manager import SparkSessionManager
from src.io.data_io import DataIO
from src.config.spark_config import AppConfig

cfg = AppConfig()
spark_manager = SparkSessionManager(cfg.app_name, cfg.master)
spark = spark_manager.get_spark()
io_manager = DataIO(spark)
df = spark.read.parquet(cfg.output_path)
df.show(10, truncate=False)
spark_manager.stop()
