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
        try:
            logger.info("Lendo pagamentos...")
            pagamentos = self.io.read_pagamentos(self.config.pagamentos_path)

            logger.info("Lendo pedidos...")
            pedidos = self.io.read_pedidos(self.config.pedidos_path)

            logger.info("Filtrando pagamentos recusados e sem fraude...")
            pagamentos_filtrados = pagamentos.filter(
                (col("status") == False) &
                (col("avaliacao_fraude.fraude") == False)
            )

            logger.info("Calculando valor total do pedido...")
            pedidos = pedidos.withColumn(
                "valor_total_pedido",
                col("valor_unitario") * col("quantidade")
            )

            logger.info("Realizando join...")
            joined = (
                pedidos.join(pagamentos_filtrados,
                "id_pedido", "inner")
            )

            logger.info("Filtrando pedidos do ano...")
            joined = joined.filter(year(col("data_criacao")) == self.config.ano_relatorio)

            logger.info("Selecionando colunas finais...")
            result = joined.select(
                col("id_pedido"),
                col("uf").alias("estado_uf"),
                col("forma_pagamento"),
                col("valor_total_pedido"),
                col("data_criacao").alias("data_pedido")
            ).orderBy("estado_uf", "forma_pagamento", "data_pedido")

            return result

        except Exception as e:
            logger.exception("Erro ao processar relatório")
            raise