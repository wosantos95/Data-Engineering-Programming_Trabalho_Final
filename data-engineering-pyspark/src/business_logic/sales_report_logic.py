# src/business_logic/sales_report_logic.py

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, year, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
import logging

# Configuração global de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SalesReportLogic:
    """Classe contendo a lógica de negócios para gerar o relatório de pedidos."""

    # Recebe o ano e a sessão Spark via Injeção de Dependência
    def __init__(self, target_year: int, spark: SparkSession):
        self.target_year = target_year
        self.spark = spark 
        self.logger = logging.getLogger(__name__)

    def generate_report(self, orders_df: DataFrame, payments_df: DataFrame) -> DataFrame:
        """Executa a lógica principal (filtros, join, ordenação)."""
        self.logger.info("Iniciando geração do relatório de pedidos.")
        
        # Estrutura try/catch para tratamento de erros
        try:
            # 1. Pré-processamento e Filtro do ano (2025)
            orders_processed = (
                orders_df
                # data_pedido tem formato dd/MM/yyyy
                .withColumn("data_pedido_dt", to_date(col("data_pedido"), "dd/MM/yyyy"))
                .filter(year(col("data_pedido_dt")) == self.target_year)
                .select("id_pedido", "uf", "valor_total", col("data_pedido_dt").alias("data_do_pedido"))
            )
            self.logger.info(f"Filtro de pedidos de {self.target_year} aplicado.")

            # 2. Filtro do Pagamento/Fraude
            # Requisito: Pagamentos recusados (status=false) E fraude legítima (fraude=false)
            payments_filtered = (
                payments_df
                .filter((col("status") == False) & (col("fraude") == False))
                .select(col("id_pedido").alias("id_pedido_pag"), "forma_pagamento")
            )
            self.logger.info("Filtro de pagamentos recusados e legítimos aplicado.")

            # 3. Join dos DataFrames
            report_df = (
                orders_processed.join(
                    payments_filtered,
                    orders_processed["id_pedido"] == payments_filtered["id_pedido_pag"],
                    "inner"
                )
                .drop("id_pedido_pag")
            )

            # 4. Seleção e Ordenação Final
            final_report = (
                report_df
                .select(
                    col("id_pedido").alias("Identificador do pedido"),
                    col("uf").alias("Estado (UF)"),
                    col("forma_pagamento").alias("Forma de pagamento"),
                    col("valor_total").alias("Valor total do pedido"),
                    col("data_do_pedido").alias("Data do pedido")
                )
                # Ordenação: estado (UF), forma de pagamento e data de criação do pedido
                .orderBy(col("Estado (UF)"), col("Forma de pagamento"), col("Data do pedido"))
            )
            
            self.logger.info("Relatório final gerado com sucesso.")
            return final_report

        except Exception as e:
            # Tratamento de Erros e Logging (Registro do erro)
            self.logger.error(f"❌ Erro na lógica de negócios: {e}", exc_info=True)
            
            # Retorna DataFrame vazio em caso de falha
            empty_schema = StructType([StructField("Identificador do pedido", StringType(), True)])
            return self.spark.createDataFrame([], empty_schema)