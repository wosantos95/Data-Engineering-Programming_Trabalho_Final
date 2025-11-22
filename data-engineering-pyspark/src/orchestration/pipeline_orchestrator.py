# src/orchestration/pipeline_orchestrator.py

from src.spark_manager.session_manager import SparkSessionManager
from src.io.data_io import DataIO
from src.business_logic.sales_report_logic import SalesReportLogic
from config.spark_config import SparkConfig
import logging

class PipelineOrchestrator:
    """Classe para orquestrar o fluxo completo do pipeline de dados."""

    # Recebe todas as dependências via Injeção de Dependência
    def __init__(self, config: SparkConfig, spark_manager: SparkSessionManager, data_io: DataIO, logic: SalesReportLogic):
        self.config = config
        self.spark_manager = spark_manager
        self.data_io = data_io
        self.logic = logic
        self.logger = logging.getLogger(__name__)

    def run_pipeline(self):
        """Executa a sequência de passos."""
        self.logger.info("Pipeline: Iniciando a orquestração.")

        try:
            # 1. Leitura de Dados (usando os paths da config)
            payments_df = self.data_io.read_payments(self.config.payments_path)
            orders_df = self.data_io.read_orders(self.config.orders_path)

            # 2. Lógica de Negócios
            report_df = self.logic.generate_report(orders_df, payments_df)
            
            # 3. Escrita do Relatório (Parquet)
            self.data_io.write_report(report_df, self.config.output_path)

            self.logger.info("Pipeline concluído com sucesso!")

        except Exception as e:
            self.logger.error(f"Pipeline: Falha grave na execução: {e}", exc_info=True)
            raise 

        finally:
            self.spark_manager.stop_session()
            self.logger.info("Pipeline: Sessão Spark finalizada.")