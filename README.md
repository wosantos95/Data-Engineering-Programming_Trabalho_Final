# Data-Engineering-Programming_Trabalho-Final_Projeto-Pyspark

Este guia detalha os passos para configurar e executar um projeto de pipeline de dados com PySpark.

## 1. Configuração Inicial

1.1 Instale o Java 17:
```bash 
O PySpark exige a instalação do Java.

sudo apt update -y && sudo apt upgrade -y
sudo apt install -y openjdk-17-jdk
````

## 2. Criação de pasta para o projeto

2.1 Crie uma pasta para o projeto:
```bash 
mkdir -p data-engineering-pyspark/src
mkdir -p data-engineering-pyspark/data/input
mkdir -p data-engineering-pyspark/data/output
````

2.2 Acesse a pasta do projeto:
```bash 
cd data-engineering-pyspark
````

## 3. Criar um ambiente virtual e instalação das dependencias 

Instale o PySpark e a dependência pyyaml (necessária para o arquivo de configuração YAML).
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark pyyaml
````

## 4. Baixe os datasets

4.1 Crie o aquivo src/download_data.sh
```bash
touch src/download_data.sh
````

4.2. Copie o script abaixo no arquivo:

Este script baixa automaticamente o conteúdo dos repositórios necessários para a execução:

https://github.com/infobarbosa/datasets-csv-pedidos

https://github.com/infobarbosa/dataset-json-pagamentos
```bash
<!-- end list -->

#!/usr/bin/env bash
set -e

# Nota: Ajuste esta variavel se o seu caminho for diferente.
ROOT="/home/ubuntu/environment/data-engineering-pyspark"
INPUT_DIR="$ROOT/data/input"

echo "🧽 Limpando diretórios..."
rm -rf "$ROOT/data/tmp-pagamentos" "$ROOT/data/tmp-pedidos"
mkdir -p "$INPUT_DIR"
rm -rf "$INPUT_DIR"/*

echo ""
echo "⬇ Baixando TODOS os arquivos de PAGAMENTOS (via API GitHub)..."

# lista todos os arquivos da pasta data/pagamentos/
curl -s [https://api.github.com/repos/infobarbosa/dataset-json-pagamentos/contents/data/pagamentos](https://api.github.com/repos/infobarbosa/dataset-json-pagamentos/contents/data/pagamentos) \
| grep "download_url" \
| cut -d '"' -f 4 \
| while read url; do
      echo "Baixando: $(basename $url)"
      curl -L "$url" -o "$INPUT_DIR/$(basename $url)"
  done

echo ""
echo "⬇ Baixando TODOS os arquivos de PEDIDOS (pasta data/pedidos)..."

curl -s [https://api.github.com/repos/infobarbosa/datasets-csv-pedidos/contents/data/pedidos](https://api.github.com/repos/infobarbosa/datasets-csv-pedidos/contents/data/pedidos) \
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
````

4.3 Execute o script para baixar os datasets.

Não se esqueça de conceder permissão de execução: chmod +x src/download_data.sh.
```bash
./src/download_data.sh
````

5. Criação do script src/main.py

5.1 Crie o script src/main.py
```bash
touch src/main.py
````

5.2 Adicione o conteúdo abaixo no arquivo src/main.py:
```bash
from pyspark.sql import functions as F

from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

config = carregar_config()

app_name = config["spark"]["app_name"]
path_pagamentos = config["paths"]["pagamentos"]
path_pedidos = config["paths"]["pedidos"]
pedidos_csv_options = config["file_options"]["pedidos_csv"]
path_output = config["paths"]["output"]

print("Iniciando Spark...")
spark = SparkSessionManager.get_spark_session(app_name)
dh = DataHandler(spark)
tr = Transformation()

try:
    print("Lendo pagamentos...")
    df_pagamentos = dh.load_pagamentos(path_pagamentos)

    print("Lendo pedidos...")
    df_pedidos = dh.load_pedidos(path_pedidos, pedidos_csv_options)

    print("Extraindo campo fraude...")
    df_pagamentos = df_pagamentos.withColumn("fraude", F.col("avaliacao_fraude.fraude"))

    print("Filtrando pagamentos recusados e legítimos...")
    df_pag_filtrado = tr.filtrar_pagamentos_validos(df_pagamentos)

    print("Filtrando pedidos de 2025...")
    df_pedidos_2025 = tr.filtrar_pedidos_2025(df_pedidos)

    print("Calculando valor total...")
    df_pedidos_2025 = tr.adicionar_valor_total(df_pedidos_2025)

    print("Realizando JOIN...")
    df_join = tr.join_pedidos_pagamentos(df_pedidos_2025, df_pag_filtrado)

    print("Selecionando colunas finais...")
    df_resultado = tr.selecionar_campos_finais(df_join)

    print("Ordenando relatório...")
    df_resultado = tr.ordenar_relatorio(df_resultado)

    print("Resultado (20 linhas):")
    df_resultado.show(20, truncate=False)

    print(f"Gravando parquet em: {path_output}")
    dh.write_parquet(df_resultado, path_output)

except Exception as e:
    print(f"Erro no pipeline: {e}")
finally:
    print("Finalizado!")
    spark.stop()
````

6. Centralizando as Configurações

6.1 Instale a dependência pyyaml:

pip install pyyaml


6.2 Crie um arquivo config/settings.yaml:

mkdir config
touch config/settings.yaml


6.3 Adicione o seguinte conteúdo ao arquivo config/settings.yaml:

spark:
  app_name: "Analise de Pedidos 2025"

paths:
  pagamentos: "data/input/pagamentos-*.json.gz"
  pedidos: "data/input/pedidos-*.csv.gz"
  output: "data/output/relatorio_pedidos_2025"

file_options:
  pedidos_csv:
    compression: "gzip"
    header: true
    sep: ";"


6.4 Crie o diretório e o arquivo de inicialização:

mkdir -p src/config
touch src/config/__init__.py


6.5 Crie o arquivo src/config/settings.py:

touch src/config/settings.py


6.6 Cole o código abaixo no conteudo do arquivo:

import yaml

def carregar_config(path: str = "config/settings.yaml") -> dict:
    """Carrega um arquivo de configuração YAML."""
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


7. Gerenciando a Sessão Spark

7.1 Crie o diretório e o arquivo de inicialização:

mkdir -p src/session
touch src/session/__init__.py


7.2 Crie o arquivo src/session/spark_session.py:

touch src/session/spark_session.py


7.3 Adicione o seguinte código a ele:

from pyspark.sql import SparkSession

class SparkSessionManager:
    """Gerencia a criação da SparkSession."""

    @staticmethod
    def get_spark_session(app_name: str) -> SparkSession:
        return (
            SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.ui.showConsoleProgress", "true")
                .getOrCreate()
        )


8. Pacote de Leitura e Escrita de Dados (I/O)

8.1 Crie o diretório e o arquivo de inicialização:

mkdir -p src/io_utils
touch src/io_utils/__init__.py


8.2 Crie o arquivo src/io_utils/data_handler.py:

touch src/io_utils/data_handler.py


8.3 Adicione o seguinte código a ele:

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    FloatType, TimestampType, BooleanType, DoubleType
)

class DataHandler:
    """Realiza leitura/escrita de dados."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pagamentos(self) -> StructType:
        return StructType([
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), True),
                StructField("score", DoubleType(), True),
            ])),
            StructField("data_processamento", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("id_pedido", StringType(), True),
            StructField("status", BooleanType(), True),
            StructField("valor_pagamento", DoubleType(), True)
        ])

    def _get_schema_pedidos(self) -> StructType:
        return StructType([
            StructField("ID_PEDIDO", StringType(), True),
            StructField("PRODUTO", StringType(), True),
            StructField("VALOR_UNITARIO", FloatType(), True),
            StructField("QUANTIDADE", LongType(), True),
            StructField("DATA_CRIACAO", TimestampType(), True),
            StructField("UF", StringType(), True),
            StructField("ID_CLIENTE", LongType(), True)
        ])

    def load_pagamentos(self, path: str) -> DataFrame:
        schema = self._get_schema_pagamentos()
        return (
            self.spark.read
                .schema(schema)
                .option("compression", "gzip")
                .json(path)
        )

    def load_pedidos(self, path: str, options: dict) -> DataFrame:
        schema = self._get_schema_pedidos()
        return (
            self.spark.read
                .options(**options)
                .schema(schema)
                .csv(path)
        )

    def write_parquet(self, df: DataFrame, path: str):
        df.write.mode("overwrite").parquet(path)
        print(f"✔ Arquivo gerado em: {path}")


9. Isolando a Lógica de Negócio

9.1 Crie o diretório e o arquivo de inicialização:

mkdir -p src/processing
touch src/processing/__init__.py


9.2 Crie o diretório e o arquivo de inicialização:

touch src/processing/transformations.py


9.3 Adicione o seguinte código a ele:

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class Transformation:

    def filtrar_pagamentos_validos(self, pagamentos_df: DataFrame) -> DataFrame:
        return (
            pagamentos_df
                .filter(F.col("status") == False)
                .filter(F.col("fraude") == False)
        )

    def filtrar_pedidos_2025(self, pedidos_df: DataFrame) -> DataFrame:
        return pedidos_df.filter(F.year("DATA_CRIACAO") == 2025)

    def adicionar_valor_total(self, pedidos_df: DataFrame) -> DataFrame:
        return pedidos_df.withColumn(
            "valor_total",
            F.col("VALOR_UNITARIO") * F.col("QUANTIDADE")
        )

    def join_pedidos_pagamentos(self, pedidos_df, pagamentos_df):
        return pedidos_df.join(
            pagamentos_df,
            pedidos_df.ID_PEDIDO == pagamentos_df.id_pedido,
            "inner"
        )

    def selecionar_campos_finais(self, df):
        return df.select(
            df.ID_PEDIDO.alias("id_pedido"),
            df.UF.alias("estado"),
            df.forma_pagamento,
            df.valor_total,
            df.DATA_CRIACAO.alias("data_pedido")
        )

    def ordenar_relatorio(self, df):
        return df.orderBy("estado", "forma_pagamento", "data_pedido")


9.4 Faça o teste:

Execute o pipeline a partir da pasta raiz do projeto, garantindo que o ambiente virtual esteja ativado (source .venv/bin/activate).

spark-submit src/main.py
