from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, year, when, desc, monotonically_increasing_id,avg,asc

# Tabela canal
df_canal = df_final.groupBy("channelTitle").agg(sum(col("views")).alias("views"))
df_canal = df_canal.withColumn("id", monotonically_increasing_id() + 1)

# Tabela grupo
#adicionar coluna de faixa de visualizações
df_final = df_final.withColumn("rangeOfViews",
    when((col("views") >= 0) & (col("views") <= 1000000), "Grupo 1") \
    .when((col("views") > 1000000) & (col("views") <= 5000000), "Grupo 2") \
    .when(col("views") > 5000000, "Grupo 3"))

df_grupo = df_final.select("rangeOfViews").distinct()
df_grupo = df_grupo.withColumn("id",monotonically_increasing_id() + 1)
# Tabela categoria
df_categoria = df_final.select("categoryId","category").distinct()
df_categoria= df_categoria.withColumnRenamed("category","categoryName")
# Tabela paises  
df_pais = df_final.select("language").distinct()
df_pais = df_pais.withColumn("id", monotonically_increasing_id() + 1)
df_pais = df_pais.withColumnRenamed("language","languageName")

# Relação canal-categoria
df_r_canal_categoria = df_final.select("category","channelTitle").distinct()
df_r_canal_categoria = df_r_canal_categoria.where(col("channelTitle").isNotNull() & col("category").isNotNull())

# Relaçao canal-paises 
df_r_canal_paises = df_final.select("channelTitle","language").distinct()
df_r_canal_paises = df_r_canal_paises.where(col("channelTitle").isNotNull() & col("language").isNotNull())

#Relação entre canal-grupo
df_r_canal_grupo = df_final.select("channelTitle","rangeOfViews").distinct()
df_r_canal_grupo = df_r_canal_grupo.where(col("channelTitle").isNotNull() & col("rangeOfViews").isNotNull())

#Atributos de credenciais
url = "neo4j+s://beb1dcaf.databases.neo4j.io"
username="neo4j"
password="Vu41pYnHMzUq30d4Zk6zIeUre1T3K_vwpTKFU4Tq2P8"
#Adição tabela categoria
df_categoria.write.format("org.neo4j.spark.DataSource")\
        .mode("Overwrite")\
        .option("url", url)\
        .option("authentication.type", "basic")\
        .option("authentication.basic.username", username)\
        .option("authentication.basic.password", password)\
        .option("labels", ":Categoria")  \
        .option("node.keys", "categoryId")\
        .save()
#Adição tabela de canal 
df_canal.write.format("org.neo4j.spark.DataSource")\
        .mode("Overwrite")\
        .option("url", url)\
        .option("authentication.type", "basic")\
        .option("authentication.basic.username", username)\
        .option("authentication.basic.password", password)\
        .option("labels", ":Canal")  \
        .option("node.keys", "id")\
        .save()

#Adição tabela grupo
df_grupo.write.format("org.neo4j.spark.DataSource")\
        .mode("Overwrite")\
        .option("url", url)\
        .option("authentication.type", "basic")\
        .option("authentication.basic.username", username)\
        .option("authentication.basic.password", password)\
        .option("labels", ":Grupo")  \
        .option("node.keys", "id")\
        .save()
#Adição tabelas paises 
df_pais.write.format("org.neo4j.spark.DataSource")\
        .mode("Overwrite")\
        .option("url", url)\
        .option("authentication.type", "basic")\
        .option("authentication.basic.username", username)\
        .option("authentication.basic.password", password)\
        .option("labels", ":Pais")  \
        .option("node.keys", "id")\
        .save()
#Adição relação canal-categoria
df_r_canal_categoria.repartition(1).write.format("org.neo4j.spark.DataSource")\
    .mode("Overwrite")\
    .option("url", url)\
    .option("authentication.type", "basic")\
    .option("authentication.basic.username", username)\
    .option("authentication.basic.password", password)\
    .option("relationship", "TEM_VIDEO_NA")\
    .option("relationship.save.strategy", "keys")\
    .option("relationship.source.labels", ":Canal")\
    .option("relationship.source.save.mode", "overwrite")\
    .option("relationship.source.node.keys", "channelTitle")\
    .option("relationship.target.labels", ":Categoria")\
    .option("relationship.target.node.keys", "category")\
    .option("relationship.target.save.mode", "overwrite")\
    .save()
#Adição relação canal-paises
df_r_canal_paises.repartition(1).write.format("org.neo4j.spark.DataSource")\
    .mode("Overwrite")\
    .option("url", url)\
    .option("authentication.type", "basic")\
    .option("authentication.basic.username", username)\
    .option("authentication.basic.password", password)\
    .option("relationship", "TEM_VIDEO_DE")\
    .option("relationship.save.strategy", "keys")\
    .option("relationship.source.labels", ":Canal")\
    .option("relationship.source.save.mode", "overwrite")\
    .option("relationship.source.node.keys", "channelTitle")\
    .option("relationship.target.labels", ":Pais")\
    .option("relationship.target.node.keys", "language")\
    .option("relationship.target.save.mode", "overwrite")\
    .save()
#Adição relação canal-grupo
df_r_canal_grupo.repartition(1).write.format("org.neo4j.spark.DataSource")\
    .mode("Overwrite")\
    .option("url", url)\
    .option("authentication.type", "basic")\
    .option("authentication.basic.username", username)\
    .option("authentication.basic.password", password)\
    .option("relationship", "TEM_VIDEO_NO")\
    .option("relationship.save.strategy", "keys")\
    .option("relationship.source.labels", ":Canal")\
    .option("relationship.source.save.mode", "overwrite")\
    .option("relationship.source.node.keys", "channelTitle")\
    .option("relationship.target.labels", ":Grupo")\
    .option("relationship.target.node.keys", "rangeOfViews")\
    .option("relationship.target.save.mode", "overwrite")\
    .save()

#Teste de leitura 
test_read = spark.read.format("org.neo4j.spark.DataSource")\
    .option("url", url)\
    .option("authentication.type", "basic")\
    .option("authentication.basic.username", username)\
    .option("authentication.basic.password", password)\
    .option("labels", ":Categoria")\
    .load()\
    .show()