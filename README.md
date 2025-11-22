#!/usr/bin/env bash
#
# SCRIPT DE AUTOMACAO PARA SETUP DO PROJETO PYSPARK
# ----------------------------------------------------
# 1. Cria a estrutura de diretórios.
# 2. Configura o ambiente virtual e instala dependências (pyspark, pyyaml).
# 3. Cria o arquivo de configuração YAML.
# 4. Cria todos os módulos Python na pasta src/.
# 5. Cria o script de download de dados (src/download_data.sh).
#

echo "==============================================="
echo "INICIANDO SETUP DO PROJETO PYSPARK"
echo "==============================================="

# Define o nome da pasta raiz do projeto
PROJECT_DIR="data-engineering-pyspark"

# Verifica se o diretório já existe
if [ -d "$PROJECT_DIR" ]; then
    echo "Diretório '$PROJECT_DIR' já existe. Acessando..."
    cd "$PROJECT_DIR"
else
    echo "Criando e acessando diretório raiz: $PROJECT_DIR"
    mkdir -p "$PROJECT_DIR"
    cd "$PROJECT_DIR"
fi

# 1. CRIAÇÃO DA ESTRUTURA DE DIRETÓRIOS
echo "1/7 - Criando estrutura de diretórios..."
mkdir -p src/{config,session,io_utils,processing}
mkdir -p data/{input,output}
mkdir -p config
touch src/config/__init__.py src/session/__init__.py src/io_utils/__init__.py src/processing/__init__.py
echo "Estrutura criada."

# 2. SETUP DO AMBIENTE VIRTUAL E DEPENDÊNCIAS
echo "2/7 - Configurando ambiente virtual Python e instalando PySpark e pyyaml..."
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark pyyaml
echo "Ambiente configurado e dependências instaladas."

# 3. CRIAÇÃO DO ARQUIVO DE CONFIGURAÇÃO (YAML)
echo "3/7 - Criando config/settings.yaml..."
cat > config/settings.yaml << 'EOF_YAML'
spark:
  app_name: "Analise de Pedidos 2025"

paths:
  pagamentos: "data/input/pagamentos-*.json.gz"
  pedidos: "data/input/pedidos-*.csv.gz"
  output: "data/output/relatorio_pedidos_2025"

file_options:
  pedidos_csv:
    compression: "gzip"
    header: true
    sep: ";"
EOF_YAML
echo "settings.yaml criado."

# 4. CRIAÇÃO DOS MÓDULOS PYTHON (SRC)
echo "4/7 - Criando módulos Python..."

# src/config/settings.py
cat > src/config/settings.py << 'EOF_CONFIG_PY'
import yaml

def carregar_config(path: str = "config/settings.yaml") -> dict:
    """Carrega um arquivo de configuração YAML."""
    # O path e ajustado para funcionar a partir da raiz do projeto, onde main.py e executado.
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)
EOF_CONFIG_PY

# src/session/spark_session.py
cat > src/session/spark_session.py << 'EOF_SESSION_PY'
from pyspark.sql import SparkSession

class SparkSessionManager:
    """Gerencia a criação da SparkSession."""

    @staticmethod
    def get_spark_session(app_name: str) -> SparkSession:
        return (
            SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.ui.showConsoleProgress", "true")
                .getOrCreate()
        )
EOF_SESSION_PY

# src/io_utils/data_handler.py
cat > src/io_utils/data_handler.py << 'EOF_HANDLER_PY'
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    FloatType, TimestampType, BooleanType, DoubleType
)

class DataHandler:
    """Realiza leitura/escrita de dados."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pagamentos(self) -> StructType:
        return StructType([
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), True),
                StructField("score", DoubleType(), True),
            ])),
            StructField("data_processamento", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("id_pedido", StringType(), True),
            StructField("status", BooleanType(), True),
            StructField("valor_pagamento", DoubleType(), True)
        ])

    def _get_schema_pedidos(self) -> StructType:
        return StructType([
            StructField("ID_PEDIDO", StringType(), True),
            StructField("PRODUTO", StringType(), True),
            StructField("VALOR_UNITARIO", FloatType(), True),
            StructField("QUANTIDADE", LongType(), True),
            StructField("DATA_CRIACAO", TimestampType(), True),
            StructField("UF", StringType(), True),
            StructField("ID_CLIENTE", LongType(), True)
        ])

    def load_pagamentos(self, path: str) -> DataFrame:
        schema = self._get_schema_pagamentos()
        return (
            self.spark.read
                .schema(schema)
                .option("compression", "gzip")
                .json(path)
        )

    def load_pedidos(self, path: str, options: dict) -> DataFrame:
        schema = self._get_schema_pedidos()
        return (
            self.spark.read
                .options(**options)
                .schema(schema)
                .csv(path)
        )

    def write_parquet(self, df: DataFrame, path: str):
        df.write.mode("overwrite").parquet(path)
        print(f"✔ Arquivo gerado em: {path}")
EOF_HANDLER_PY

# src/processing/transformations.py
cat > src/processing/transformations.py << 'EOF_TRANSFORM_PY'
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class Transformation:

    def filtrar_pagamentos_validos(self, pagamentos_df: DataFrame) -> DataFrame:
        # Filtra por status=False (pago/nao-estornado) E fraude=False (legitimo)
        return (
            pagamentos_df
                .filter(F.col("status") == False)
                .filter(F.col("fraude") == False)
        )

    def filtrar_pedidos_2025(self, pedidos_df: DataFrame) -> DataFrame:
        return pedidos_df.filter(F.year("DATA_CRIACAO") == 2025)

    def adicionar_valor_total(self, pedidos_df: DataFrame) -> DataFrame:
        return pedidos_df.withColumn(
            "valor_total",
            F.col("VALOR_UNITARIO") * F.col("QUANTIDADE")
        )

    def join_pedidos_pagamentos(self, pedidos_df, pagamentos_df):
        return pedidos_df.join(
            pagamentos_df,
            pedidos_df.ID_PEDIDO == pagamentos_df.id_pedido,
            "inner"
        )

    def selecionar_campos_finais(self, df):
        return df.select(
            df.ID_PEDIDO.alias("id_pedido"),
            df.UF.alias("estado"),
            df.forma_pagamento,
            df.valor_total,
            df.DATA_CRIACAO.alias("data_pedido")
        )

    def ordenar_relatorio(self, df):
        return df.orderBy("estado", "forma_pagamento", "data_pedido")
EOF_TRANSFORM_PY

# src/main.py
cat > src/main.py << 'EOF_MAIN_PY'
from pyspark.sql import functions as F

from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

config = carregar_config()

app_name = config["spark"]["app_name"]
path_pagamentos = config["paths"]["pagamentos"]
path_pedidos = config["paths"]["pedidos"]
pedidos_csv_options = config["file_options"]["pedidos_csv"]
path_output = config["paths"]["output"]

print("Iniciando Spark...")
spark = SparkSessionManager.get_spark_session(app_name)
dh = DataHandler(spark)
tr = Transformation()

try:
    print("Lendo pagamentos...")
    df_pagamentos = dh.load_pagamentos(path_pagamentos)

    print("Lendo pedidos...")
    df_pedidos = dh.load_pedidos(path_pedidos, pedidos_csv_options)

    print("Extraindo campo fraude...")
    df_pagamentos = df_pagamentos.withColumn("fraude", F.col("avaliacao_fraude.fraude"))

    print("Filtrando pagamentos recusados e legítimos...")
    df_pag_filtrado = tr.filtrar_pagamentos_validos(df_pagamentos)

    print("Filtrando pedidos de 2025...")
    df_pedidos_2025 = tr.filtrar_pedidos_2025(df_pedidos)

    print("Calculando valor total...")
    df_pedidos_2025 = tr.adicionar_valor_total(df_pedidos_2025)

    print("Realizando JOIN...")
    df_join = tr.join_pedidos_pagamentos(df_pedidos_2025, df_pag_filtrado)

    print("Selecionando colunas finais...")
    df_resultado = tr.selecionar_campos_finais(df_join)

    print("Ordenando relatório...")
    df_resultado = tr.ordenar_relatorio(df_resultado)

    print("Resultado (20 linhas):")
    df_resultado.show(20, truncate=False)

    print(f"Gravando parquet em: {path_output}")
    dh.write_parquet(df_resultado, path_output)

except Exception as e:
    print(f"Erro no pipeline: {e}")
finally:
    print("Finalizado!")
    spark.stop()
EOF_MAIN_PY

# 5. CRIAÇÃO DO SCRIPT DE DOWNLOAD
echo "5/7 - Criando src/download_data.sh..."

# Nota: Mantendo o ROOT original do seu prompt.
ROOT_PATH_DOWNLOAD="/home/ubuntu/environment/data-engineering-pyspark"

cat > src/download_data.sh << EOF_DOWNLOAD
#!/usr/bin/env bash
set -e

ROOT="$ROOT_PATH_DOWNLOAD"
INPUT_DIR="\$ROOT/data/input"

echo "🧽 Limpando diretórios..."
rm -rf "\$ROOT/data/tmp-pagamentos" "\$ROOT/data/tmp-pedidos"
mkdir -p "\$INPUT_DIR"
rm -rf "\$INPUT_DIR"/*

echo ""
echo "⬇ Baixando TODOS os arquivos de PAGAMENTOS (via API GitHub)..."

# lista todos os arquivos da pasta data/pagamentos/
curl -s https://api.github.com/repos/infobarbosa/dataset-json-pagamentos/contents/data/pagamentos \\
| grep "download_url" \\
| cut -d '"' -f 4 \\
| while read url; do
      echo "Baixando: \$(basename \$url)"
      curl -L "\$url" -o "\$INPUT_DIR/\$(basename \$url)"
  done

echo ""
echo "⬇ Baixando TODOS os arquivos de PEDIDOS (pasta data/pedidos)..."

curl -s https://api.github.com/repos/infobarbosa/datasets-csv-pedidos/contents/data/pedidos \\
| grep "download_url" \\
| cut -d '"' -f 4 \\
| while read url; do
      echo "Baixando: \$(basename \$url)"
      curl -L "\$url" -o "\$INPUT_DIR/\$(basename \$url)"
  done

echo ""
echo "📂 Arquivos baixados:"
ls -lh "\$INPUT_DIR"

echo ""
echo "✅ Processo concluído com sucesso!"
EOF_DOWNLOAD

chmod +x src/download_data.sh
echo "src/download_data.sh criado e permissão de execução concedida."

echo "==============================================="
echo "SETUP CONCLUÍDO COM SUCESSO."
echo "Execute: cd $PROJECT_DIR && source .venv/bin/activate"
echo "Para o próximo passo, consulte o README.md."
echo "==============================================="
