#Objetivo: Ter uma tabela com todas as categorias que tem nos arquivos JSON
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,desc

# Unindo todas as tabelas
df_json = df_ca_json.union(df_br_json).union(df_de_json).union(df_fr_json).union(df_gb_json).union(df_in_json).union(df_jp_json).union(df_kr_json).union(df_mx_json).union(df_ru_json).union(df_us_json)

display(df_json.count())

#Transformação dos dados
df_json = df_json.select(explode(col("items")).alias("element"))
df_json = df_json.select("element.id", "element.snippet.title")
df_json = df_json.withColumnRenamed("title", "category")

# Elimando duplicadas
df_json = df_json.distinct()



display(df_json.count())
df_json.orderBy(("id")).show()
