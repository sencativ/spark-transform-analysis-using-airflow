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
        .setMaster(spark_host)
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
# SCHEMA

# Sellers
schema_sellers = "seller_id STRING, seller_zip_code_prefix INT, seller_city STRING, seller_state STRING"

# Products
schema_products = "product_id STRING, product_category_name STRING, product_name_lenght INT, product_description_lenght INT, product_photos_qty INT, product_weight_g DOUBLE, product_length_cm DOUBLE, product_height_cm DOUBLE, product_width_cm DOUBLE"

# Customers
schema_customers = "customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix INT, customer_city STRING, customer_state STRING"

# Order Items
schema_order_items = "order_id STRING, order_item_id INT, product_id STRING, seller_id STRING, shipping_limit_date TIMESTAMP, price DOUBLE, freight_value DOUBLE"

# Order
schema_order = " order_id STRING, customer_id STRING, order_status STRING, order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP, order_delivered_carrier_date TIMESTAMP, order_delivered_customer_date TIMESTAMP, order_estimated_delivery_date TIMESTAMP"

# Product translation
schema_product_translation = (
    "product_category_name string, product_category_name_english string"
)

# =================================================================================
# LOADING DATA

# Sellers
df_sellers = spark.read.csv(
    "/data/olist/olist_sellers_dataset.csv", header=True, schema=schema_sellers
)

# Products
df_products = spark.read.csv(
    "/data/olist/olist_products_dataset.csv", header=True, schema=schema_products
)

# Customers
df_customers = spark.read.csv(
    "/data/olist/olist_customers_dataset.csv", header=True, schema=schema_customers
)

# Order Items
df_order_items = spark.read.csv(
    "/data/olist/olist_order_items_dataset.csv", header=True, schema=schema_order_items
)

# Order
df_order = spark.read.csv(
    "/data/olist/olist_orders_dataset.csv", header=True, schema=schema_order
)

# Product translation
df_product_translation = spark.read.csv(
    "/data/olist/product_category_name_translation.csv",
    header=True,
    schema=schema_product_translation,
)

# =================================================================================
# Write to PostgreSQL

# Product Translation
(
    df_product_translation.write.mode(
        "overwrite"
    ).jdbc(  # Use overwrite mode to replace the table
        jdbc_url,
        "public.olist_product_translation",  # Table name
        properties=jdbc_properties,
    )
)

# Sellers
df_sellers.write.mode("overwrite").jdbc(
    jdbc_url,
    "public.olist_sellers",  # Nama tabel untuk sellers
    properties=jdbc_properties,
)

# Products
df_products.write.mode("overwrite").jdbc(
    jdbc_url,
    "public.olist_products",  # Nama tabel untuk products
    properties=jdbc_properties,
)

# Customers
df_customers.write.mode("overwrite").jdbc(
    jdbc_url,
    "public.olist_customers",  # Nama tabel untuk customers
    properties=jdbc_properties,
)

# Order Items
df_order_items.write.mode("overwrite").jdbc(
    jdbc_url,
    "public.olist_order_items",  # Nama tabel untuk order items
    properties=jdbc_properties,
)

# Order
df_order.write.mode("overwrite").jdbc(
    jdbc_url,
    "public.olist_order",  # Nama tabel untuk order
    properties=jdbc_properties,
)

# =================================================================================
# Show

# Product Translation
df_product_translation = spark.read.jdbc(
    jdbc_url, "public.olist_product_translation", properties=jdbc_properties
)

# Sellers
df_sellers = spark.read.jdbc(
    jdbc_url, "public.olist_sellers", properties=jdbc_properties
)

# Products
df_products = spark.read.jdbc(
    jdbc_url, "public.olist_products", properties=jdbc_properties
)

# Customers
df_customers = spark.read.jdbc(
    jdbc_url, "public.olist_customers", properties=jdbc_properties
)

# Order Items
df_order_items = spark.read.jdbc(
    jdbc_url, "public.olist_order_items", properties=jdbc_properties
)

# Order
df_order = spark.read.jdbc(jdbc_url, "public.olist_order", properties=jdbc_properties)

# Display a sample of each DataFrame to verify
df_product_translation.show(5)
df_sellers.show(5)
df_products.show(5)
df_customers.show(5)
df_order_items.show(5)
df_order.show(5)
