from config.spark_config import AppConfig
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