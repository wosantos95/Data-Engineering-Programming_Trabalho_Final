# src/io/data_io.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
import logging

class DataIO:
    """Classe para operações de I/O (Leitura/Escrita) de dados."""

    # Schema Explícito para Pagamentos (JSON)
    PAYMENTS_SCHEMA = StructType([
        StructField("id_pedido", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("status", BooleanType(), True),
        StructField("fraude", BooleanType(), True)
    ])

    # Schema Explícito para Pedidos (CSV)
    ORDERS_SCHEMA = StructType([
        StructField("id_pedido", StringType(), True),
        StructField("data_pedido", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("valor_total", DoubleType(), True)
    ])

    # Recebe a sessão Spark via Injeção de Dependência
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def read_payments(self, path: str) -> DataFrame:
        """Lê o dataset de pagamentos em formato JSON.GZ."""
        self.logger.info(f"Lendo dados de pagamentos de: {path}")
        return self.spark.read.schema(self.PAYMENTS_SCHEMA).json(path)

    def read_orders(self, path: str) -> DataFrame:
        """Lê o dataset de pedidos em formato CSV.GZ."""
        self.logger.info(f"Lendo dados de pedidos de: {path}")
        return self.spark.read.schema(self.ORDERS_SCHEMA).csv(path, header=True)
        
    def write_report(self, df: DataFrame, path: str):
        """Grava o DataFrame do relatório em formato Parquet."""
        self.logger.info(f"Escrevendo relatório final em Parquet para: {path}")
        df.write.mode("overwrite").parquet(path)