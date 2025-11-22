# src/main.py

import sys
import os
# Linhas para adicionar o diretório raiz ao caminho de busca
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.spark_config import SparkConfig

# --- CORREÇÃO: Prefixar as importações com 'src.' ---
from src.spark_manager.session_manager import SparkSessionManager
from src.io.data_io import DataIO
from src.business_logic.sales_report_logic import SalesReportLogic
from src.orchestration.pipeline_orchestrator import PipelineOrchestrator
# ----------------------------------------------------

import logging
# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("--- START: main.py (Aggregation Root) ---")

    try:
        # 1. INSTANCIAÇÃO de TODAS as dependências (Injeção de Dependências)
        
        config = SparkConfig() # Configuração
        
        spark_manager = SparkSessionManager(config=config) # Gerenciador de Sessão
        spark_session = spark_manager.get_session() # Obtém a sessão

        data_io = DataIO(spark=spark_session) # I/O (depende da sessão)

        # Lógica de Negócios (depende do ano e da sessão para o try/catch)
        logic = SalesReportLogic(target_year=config.target_year, spark=spark_session) 

        # 2. INJEÇÃO das dependências no Orchestrator
        orchestrator = PipelineOrchestrator(
            config=config,
            spark_manager=spark_manager,
            data_io=data_io,
            logic=logic
        )

        # 3. Execução do pipeline
        orchestrator.run_pipeline()
        
    except Exception as e:
        logger.critical(f"❌ Erro Crítico, pipeline interrompido: {e}")
        
    logger.info("--- END: main.py ---")