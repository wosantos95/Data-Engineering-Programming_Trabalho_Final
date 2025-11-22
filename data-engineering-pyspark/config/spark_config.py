# config/spark_config.py

class SparkConfig:
    """Classe para configurações centrais da aplicação."""

    def __init__(self):
        self.app_name = "DataEngineeringFinalProject"
        self.master = "local[*]"
        # Caminhos locais para os arquivos GZ (conforme sua pasta data/input)
        self.payments_path = "data/input/pagamentos-*.json.gz"
        self.orders_path = "data/input/pedidos-*.csv.gz"
        self.output_path = "data/output/relatorio_pedidos_2025.parquet"
        self.target_year = 2025 # Ano de interesse para o relatório