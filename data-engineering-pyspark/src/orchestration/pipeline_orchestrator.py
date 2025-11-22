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
