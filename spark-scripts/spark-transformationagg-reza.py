import os
from pathlib import Path
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, col, when
from dotenv import load_dotenv

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv("POSTGRES_CONTAINER_NAME")
postgres_dw_db = os.getenv("POSTGRES_DW_DB")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")

spark_host = "spark://dibimbing-dataeng-spark-master:7077"

sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(
        pyspark.SparkConf()
        .setAppName("Dibimbing")
        .setMaster("local")
        .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")
    )
)
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f"jdbc:postgresql://{postgres_host}:5432/{postgres_dw_db}"
jdbc_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
}

# =================================================================================
# Read Data

# Membaca tabel olist_customers
df_customers = spark.read.jdbc(
    url=jdbc_url, table="public.olist_customers", properties=jdbc_properties
)
df_customers.show(5)

# Membaca tabel olist_order_items
df_order_items = spark.read.jdbc(
    url=jdbc_url, table="public.olist_order_items", properties=jdbc_properties
)
df_order_items.show(5)

# Membaca tabel olist_order
df_order = spark.read.jdbc(
    url=jdbc_url, table="public.olist_order", properties=jdbc_properties
)
df_order.show(5)

# Membaca tabel olist_product_translation
df_product_translation = spark.read.jdbc(
    url=jdbc_url, table="public.olist_product_translation", properties=jdbc_properties
)
df_product_translation.show(5)

# Membaca tabel olist_products
df_products = spark.read.jdbc(
    url=jdbc_url, table="public.olist_products", properties=jdbc_properties
)
df_products.show(5)

# Membaca tabel olist_sellers
df_sellers = spark.read.jdbc(
    url=jdbc_url, table="public.olist_sellers", properties=jdbc_properties
)
df_sellers.show(5)

# Membaca tabel retail
df_retail = spark.read.jdbc(
    url=jdbc_url, table="public.retail", properties=jdbc_properties
)
df_retail.show(5)

# =================================================================================
# Transformation Join

# set join preferences
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Untuk Analisa pertama
# Join tiga tabel, olist_order_items & olist_products & olist seller
df_analisa_pertama = df_order_items.join(df_products, "product_id").join(
    df_sellers, "seller_id"
)
df_analisa_pertama.show()

# Untuk Analisis Kedua
# Join tiga tabel, olist_order_items & olist_products & olist product translation
df_analisa_kedua = df_order_items.join(df_products, "product_id").join(
    df_product_translation, "product_category_name"
)
df_analisa_kedua.show()

# Untuk Analisis Ketiga
# Join dua tabel, olist_order_dataset & olist_customers
df_analisa_ketiga = df_order.join(df_customers, "customer_id")
df_analisa_ketiga.show()

# =================================================================================
# Menyimpan hasil join ke CSV

# Join analisa pertama
df_analisa_pertama.write.mode("overwrite").csv(
    "/data/hasiljoin_analisa_pertama.csv", header=True
)

# join analisa kedua
df_analisa_kedua.write.mode("overwrite").csv(
    "/data/hasiljoin_analisa_kedua.csv", header=True
)

# join analisa ketiga
df_analisa_ketiga.write.mode("overwrite").csv(
    "/data/hasiljoin_analisa_ketiga.csv", header=True
)

# =================================================================================
# Aggregation and Analysis

# ANALISIS PERTAMA
df_analisa_pertama.createOrReplaceTempView("analisa_pertama")

hasil_analisis_pertama = spark.sql(
    """
SELECT 
    seller_id,
    product_category_name,
    count(order_id) AS Total_Sold_Product
FROM
    analisa_pertama
GROUP BY
    product_category_name, seller_id
ORDER BY
    count(order_id) DESC
"""
)
hasil_analisis_pertama.show()

# ANALISIS KEDUA
df_analisa_kedua.createOrReplaceTempView("analisa_kedua")
hasil_analisis_kedua = spark.sql(
    """
SELECT
    product_category_name_english AS Product,
    count(order_id) AS Total_Sold_Product
FROM
    analisa_kedua
GROUP BY
    product_category_name_english
ORDER BY
    count(order_id) DESC

"""
)
hasil_analisis_kedua.show()

# ANALISIS KETIGA
df_analisa_ketiga.createOrReplaceTempView("analisa_ketiga")

hasil_analisis_ketiga = spark.sql(
    """
SELECT
    customer_city,
    count(order_id) AS Total_Sold_Product
FROM
    analisa_ketiga
GROUP BY
    customer_city
ORDER BY
    count(order_id) DESC
"""
)
hasil_analisis_ketiga.show()

# =================================================================================
# CSV

# Menyimpan hasil analisis pertama ke CSV
hasil_analisis_pertama.write.mode("overwrite").csv(
    "/data/analisis_pertama.csv", header=True
)

# Menyimpan hasil analisis kedua ke CSV
hasil_analisis_kedua.write.mode("overwrite").csv(
    "/data/analisis_kedua.csv", header=True
)

# Menyimpan hasil analisis ketiga ke CSV
hasil_analisis_ketiga.write.mode("overwrite").csv(
    "/data/analisis_ketiga.csv", header=True
)
