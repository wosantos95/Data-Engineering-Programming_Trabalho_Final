
from dataclasses import dataclass

class AppConfig:
    pagamentos_path: str = "data/input/pagamentos*.json.gz"
    pedidos_path: str = "data/input/pedidos*.csv.gz"
    output_path: str = "data/output/relatorio_parquet"
    ano_relatorio: int = 2025
    app_name: str = "FIAP_Trabalho_Final"
    master: str = "local[*]"