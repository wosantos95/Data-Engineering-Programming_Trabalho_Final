# FIAP - Data Engineering Programming – Trabalho Final

## Descrição do Projeto
Este projeto em **PySpark** gera um relatório de pedidos de venda filtrando apenas:
- Pagamentos recusados (`status=false`)
- Pagamentos legítimos (`fraude=false`)
- Pedidos do ano de 2025

O relatório inclui:
1. Identificador do pedido (`id_pedido`)
2. Estado (`uf`)
3. Forma de pagamento
4. Valor total do pedido
5. Data do pedido  

O relatório é salvo em **formato Parquet**, ordenado por UF, forma de pagamento e data do pedido.

---

## Estrutura do Projeto

data-engineering-pyspark/
├── config/
│ └── spark_config.py
├── src/
│ ├── business_logic/
│ │ └── sales_report_logic.py
│ ├── io/
│ │ └── data_io.py
│ ├── orchestration/
│ │ └── pipeline_orchestrator.py
│ ├── spark_manager/
│ │ └── session_manager.py
│ └── main.py
├── tests/
│ └── test_sales_report_logic.py
├── requirements.txt
├── pyproject.toml
└── README.md

yaml
Copiar código

---

## Pré-requisitos

- Python 3.10+
- Java 11+ (necessário para PySpark)
- PySpark 3.4.1
- PyTest 7.x

---

## Instalação

1. Clone o repositório:

```bash
git clone <URL_DO_REPOSITORIO>
cd <REPO>/data-engineering-pyspark
Crie e ative um ambiente virtual:

bash
Copiar código
python3 -m venv .venv
source .venv/bin/activate
Instale as dependências:

bash
Copiar código
pip install -r requirements.txt
Execução
Execute o pipeline principal:

bash
Copiar código
python3 main.py
O relatório será gerado em:

bash
Copiar código
data/output/relatorio_parquet
Visualizar Relatório no Terminal
python
Copiar código
from config.spark_config import AppConfig
from src.spark_manager.session_manager import SparkSessionManager
from src.io.data_io import DataIO
from src.business_logic.sales_report_logic import SalesReportLogic

cfg = AppConfig()
spark_manager = SparkSessionManager(cfg.app_name, cfg.master)
spark = spark_manager.get_spark()

io_manager = DataIO(spark)
processor = SalesReportLogic(io_manager, cfg)

df = processor.build_report()
df.show(10, truncate=False)

spark_manager.stop()
Testes Unitários
Rodar os testes:

bash
Copiar código
pytest -v tests/test_sales_report_logic.py
Verifique se todos os testes passam e se a lógica de filtragem e join está correta.

Logging e Tratamento de Erros
Utiliza logging para registrar etapas do pipeline.

Erros na lógica de negócios são capturados com try/except e logados.

Repositório
Código-fonte completo: data-engineering-pyspark/

Testes: tests/

Relatório Parquet: data/output/relatorio_parquet

Instruções detalhadas neste README.md

yaml
Copiar código

---

Se você quiser, posso também gerar **um comando único pronto para o terminal Cloud9**, que já roda o pipeline e mostra as primeiras linhas do relatório, para que você copie e cole direto.  

Quer que eu faça isso também?






