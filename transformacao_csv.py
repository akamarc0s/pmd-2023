#Objetivo: Unir todas as tabelas do tipo csv, reduzir o periodo de data, agregações e adições/remoções de coluuna
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, year, when, desc, lit
from pyspark.sql.window import Window

#Adição de coluna de linguagem para cada tabela
df_br_csv = df_br_csv.withColumn("language", lit("BR"))
df_ca_csv = df_ca_csv.withColumn("language", lit("CA"))
df_de_csv = df_de_csv.withColumn("language", lit("DE"))
df_fr_csv = df_fr_csv.withColumn("language", lit("FR"))
df_gb_csv = df_gb_csv.withColumn("language", lit("GB"))
df_in_csv = df_in_csv.withColumn("language", lit("IN"))
df_jp_csv = df_jp_csv.withColumn("language", lit("JP"))
df_kr_csv = df_kr_csv.withColumn("language", lit("KR"))
df_mx_csv = df_mx_csv.withColumn("language", lit("MX"))
df_ru_csv = df_ru_csv.withColumn("language", lit("RU"))
df_us_csv = df_us_csv.withColumn("language", lit("US"))

#Unindo as tabelas
df_csv = df_ca_csv.union(df_br_csv).union(df_de_csv).union(df_fr_csv).union(df_gb_csv).union(df_in_csv).union(df_jp_csv).union(df_kr_csv).union(df_mx_csv).union(df_ru_csv).union(df_us_csv)

# Limpando as linhas duplicadas 
df_csv = df_csv.distinct()

# selecionando apenas as colunas que iremos utilizar em pelo menos uma consulta
columns_to_keep = ["title", "publishedAt", "channelTitle","categoryId","language","view_count","likes","comment_count"]

# adicionar coluna: numero de vezes que se manteve em trend (window function) e cast de valores (String->int)
w = Window.partitionBy("title","language")
df_csv = df_csv.withColumn("trending_days", count(col("title")).over(w)) \
    .withColumn("view_count", col("view_count").cast("int")) \
    .withColumn("likes", col("likes").cast("int")) \
    .withColumn("comment_count", col("comment_count").cast("int")) 

# agreção por soma do views, likes, commentarios, deslikes
columns_to_keep_agg = ["title", "publishedAt", "channelTitle","categoryId","language","trending_days"]
df_csv = df_csv.groupBy(columns_to_keep_agg).agg(sum(col("likes")).alias("likes"),sum(col("view_count")).alias("views")\
                                            ,sum(col("comment_count")).alias("comment_count"))

# join com tabela de categorias 
columns_to_keep = ["title", "publishedAt", "channelTitle","category","categoryId","language","trending_days","views","likes","comment_count"]
df_final = df_csv.join(df_json, df_csv.categoryId == df_json.id).drop(df_json.id)

df_final.select(columns_to_keep).orderBy(desc("views")).show()