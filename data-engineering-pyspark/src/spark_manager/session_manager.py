# src/spark_manager/session_manager.py

from pyspark.sql import SparkSession
from config.spark_config import SparkConfig 

class SparkSessionManager:
    """Classe para gerenciar a sessão Spark."""

    # Recebe a configuração via Injeção de Dependência
    def __init__(self, config: SparkConfig):
        self.config = config
        self._spark_session = None

    def get_session(self) -> SparkSession:
        """Cria e retorna a sessão Spark."""
        if self._spark_session is None:
            self._spark_session = (
                SparkSession.builder
                .appName(self.config.app_name)
                .master(self.config.master)
                .getOrCreate()
            )
        return self._spark_session

    def stop_session(self):
        """Para a sessão Spark."""
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None