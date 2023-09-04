#Definições padrões
file_type = "csv"
# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","


# Dados do Canadá (CA)
file_location_ca = "/FileStore/tables/CA_youtube_trending_data.csv"
# The applied options are for CSV files. For other file types, these will be ignored.
df_ca_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("multiline",True)\
  .load(file_location_ca)

df_ca_json = spark.read.option("multiline","true").json("/FileStore/CA_category_id.json")


# Dados do Brasil (BR)
file_location_br = "/FileStore/tables/BR_youtube_trending_data.csv"
# The applied options are for CSV files. For other file types, these will be ignored.
df_br_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("multiline",True)\
  .load(file_location_br)

df_br_json = spark.read.option("multiline","true").json("/FileStore/BR_category_id.json")

 # Dados da Alemanha (DE)
file_location_de = "/FileStore/DE_youtube_trending_data.csv"
# The applied options are for CSV files. For other file types, these will be ignored.
df_de_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("multiline",True)\
  .load(file_location_de)

df_de_json = spark.read.option("multiline","true").json("/FileStore/DE_category_id.json")

  # Dados da França (FR)
file_location_fr = "/FileStore/FR_youtube_trending_data.csv"
# The applied options are for CSV files. For other file types, these will be ignored.
df_fr_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("multiline",True)\
  .load(file_location_fr)

df_fr_json = spark.read.option("multiline","true").json("/FileStore/FR_category_id.json")


  # Dados da Gra-Bretanha (GB)
file_location_gb = "/FileStore/GB_youtube_trending_data.csv"
# The applied options are for CSV files. For other file types, these will be ignored.
df_gb_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("multiline",True)\
  .load(file_location_gb)

df_gb_json = spark.read.option("multiline","true").json("/FileStore/GB_category_id.json")

  # Dados da India (IN)
file_location_in = "/FileStore/IN_youtube_trending_data.csv"
# The applied options are for CSV files. For other file types, these will be ignored.
df_in_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("multiline",True)\
  .load(file_location_in)

df_in_json = spark.read.option("multiline","true").json("/FileStore/IN_category_id.json")


  # Dados do Japão (JP)
file_location_jp = "/FileStore/JP_youtube_trending_data.csv"
# The applied options are for CSV files. For other file types, these will be ignored.
df_jp_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("multiline",True)\
  .load(file_location_jp)

df_jp_json = spark.read.option("multiline","true").json("/FileStore/JP_category_id.json")


  # Dados da Coreia (KR)
file_location_kr = "/FileStore/KR_youtube_trending_data.csv"
# The applied options are for CSV files. For other file types, these will be ignored.
df_kr_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("multiline",True)\
  .load(file_location_kr)

df_kr_json = spark.read.option("multiline","true").json("/FileStore/KR_category_id.json")


# Dados do México (MX)
file_location_mx  = "/FileStore/MX_youtube_trending_data.csv"
# The applied options are for CSV files. For other file types, these will be ignored.
df_mx_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("multiline",True)\
  .load(file_location_mx)

df_mx_json = spark.read.option("multiline","true").json("/FileStore/MX_category_id.json")


# Dados da Russia (RU)
file_location_ru  = "/FileStore/RU_youtube_trending_data.csv"
# The applied options are for CSV files. For other file types, these will be ignored.
df_ru_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("multiline",True)\
  .load(file_location_ru)

df_ru_json = spark.read.option("multiline","true").json("/FileStore/RU_category_id.json")


  # Dados dos EUA (US)
file_location_us  = "/FileStore/US_youtube_trending_data.csv"
# The applied options are for CSV files. For other file types, these will be ignored.
df_us_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("multiline",True)\
  .load(file_location_us)

df_us_json = spark.read.option("multiline","true").json("/FileStore/US_category_id.json")

