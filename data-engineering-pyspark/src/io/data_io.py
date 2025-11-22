from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType,
    LongType
)

class DataIO:
    def __init__(self, spark):
        self.spark = spark

    def read_pagamentos(self, path):
        schema = StructType([
            StructField("id_pedido", StringType()),
            StructField("forma_pagamento", StringType()),
            StructField("valor_pagamento", DoubleType()),
            StructField("status", BooleanType()),
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType()),
                StructField("score", DoubleType())
            ])),
            StructField("data_processamento", TimestampType())
        ])
        return self.spark.read.schema(schema).json(path)

    def read_pedidos(self, path):
        schema = StructType([
            StructField("id_pedido", StringType()),
            StructField("produto", StringType()),
            StructField("valor_unitario", DoubleType()),
            StructField("quantidade", LongType()),
            StructField("data_criacao", TimestampType()),
            StructField("uf", StringType()),
            StructField("id_cliente", LongType()),
        ])
        return (
            self.spark.read
            .option("header", "true")
            .option("sep", ";")
            .schema(schema)
            .csv(path)
        )

    def write_parquet(self, df, path):
        df.write.mode("overwrite").parquet(path)