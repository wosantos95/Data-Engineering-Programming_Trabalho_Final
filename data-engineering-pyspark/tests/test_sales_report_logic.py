# tests/test_sales_report_logic.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, DateType
import logging

# Adiciona o pacote raiz para que o teste encontre a classe de lógica
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importação Absoluta Corrigida
from src.business_logic.sales_report_logic import SalesReportLogic

# Define o ano alvo do projeto (2025)
TARGET_YEAR = 2025

# Fixture para criar uma sessão Spark local (simulada) para testes
@pytest.fixture(scope="session")
def spark_session():
    """Cria e retorna uma SparkSession local para testes."""
    spark = (
        SparkSession.builder
        .appName("UnitTestSparkSession")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()

# Fixture para instanciar a classe de Lógica de Negócios
@pytest.fixture
def sales_report_logic(spark_session):
    """Instancia a classe SalesReportLogic."""
    # Desliga o logging do PySpark durante o teste para não poluir a saída
    spark_session.sparkContext.setLogLevel("ERROR")
    return SalesReportLogic(target_year=TARGET_YEAR, spark=spark_session)

# ----------------------------------------------------------------------
# DADOS DE TESTE: Simulam a entrada da Lógica de Negócios
# ----------------------------------------------------------------------

# 1. Dados de Pedidos (Orders)
# id_pedido, data_pedido, uf, valor_total
ORDERS_DATA = [
    # 1. Deve ser INCLUÍDO (2025)
    ("P001", "15/01/2025", "SP", 500.00),
    # 2. Deve ser EXCLUÍDO (Ano errado: 2024)
    ("P002", "01/01/2024", "RJ", 200.00),
    # 3. Deve ser INCLUÍDO (2025)
    ("P003", "20/02/2025", "MG", 100.00),
    # 4. Deve ser INCLUÍDO (2025)
    ("P004", "10/05/2025", "SP", 300.00),
]

ORDERS_SCHEMA = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("data_pedido", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("valor_total", DoubleType(), True)
])

# 2. Dados de Pagamentos (Payments)
# id_pedido, forma_pagamento, status (Recusado=False), fraude (Legítimo=False)
PAYMENTS_DATA = [
    # 1. P001: INCLUÍDO (status=FALSe, fraude=FALSe) - Critério OK
    ("P001", "Pix", False, False),
    # 2. P003: EXCLUÍDO (status=TRUE) - Pagamento APROVADO (Não entra no relatório)
    ("P003", "Boleto", True, False),
    # 3. P004: EXCLUÍDO (fraude=TRUE) - Fraude detectada (Não entra no relatório)
    ("P004", "Cartao", False, True),
    # 4. P005: INCLUÍDO (status=FALSe, fraude=FALSe) - Este ID não está na tabela de pedidos
    ("P005", "TED", False, False),
    # 5. P006: INCLUÍDO (status=FALSe, fraude=FALSe) - Critério OK
    ("P006", "Pix", False, False),
]

PAYMENTS_SCHEMA = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("status", BooleanType(), True),
    StructField("fraude", BooleanType(), True)
])

# ----------------------------------------------------------------------
# TESTE PRINCIPAL
# ----------------------------------------------------------------------

def test_generate_report_successful(spark_session, sales_report_logic):
    """
    Testa se o relatório final contém apenas os pedidos que atendem aos
    critérios (Ano=2025, Status=False, Fraude=False) e se a ordenação está correta.
    """
    # 1. Cria DataFrames simulados
    orders_df = spark_session.createDataFrame(ORDERS_DATA, ORDERS_SCHEMA)
    payments_df = spark_session.createDataFrame(PAYMENTS_DATA, PAYMENTS_SCHEMA)

    # 2. Executa a lógica
    result_df = sales_report_logic.generate_report(orders_df, payments_df)

    # 3. Validação do Esquema (Estrutura e Nomes)
    expected_column_names = [
        "Identificador do pedido", 
        "Estado (UF)", 
        "Forma de pagamento", 
        "Valor total do pedido", 
        "Data do pedido"
    ]
    assert result_df.columns == expected_column_names

    # 4. Validação dos Resultados (Apenas P001 deve sobreviver ao JOIN e aos filtros)
    # P002 (Ano 2024) excluído.
    # P003 (Status True) excluído.
    # P004 (Fraude True) excluído.
    # P005 (Sem ID de Pedido) excluído pelo INNER JOIN.

    result_count = result_df.count()
    assert result_count == 1, f"Esperava 1 linha, mas encontrei {result_count}"

    # 5. Validação dos Dados Finais (para garantir que o P001 foi o único)
    final_row = result_df.collect()[0]
    assert final_row["Identificador do pedido"] == "P001"
    assert final_row["Estado (UF)"] == "SP"
    assert final_row["Forma de pagamento"] == "Pix"
    assert final_row["Valor total do pedido"] == 500.00

    # 6. Validação da Ordenação (Testar a ordenação é mais complexo em unit tests)
    # Já que temos apenas uma linha, a ordenação está implicitamente correta.
    # Se tivéssemos mais dados, precisaríamos carregar a lista e verificar a ordem.