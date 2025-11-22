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

print("Finalizado!")
spark.stop()
