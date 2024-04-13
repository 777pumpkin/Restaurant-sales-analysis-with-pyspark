# Databricks notebook source
#  /FileStore/tables/sales_csv.txt
#  /FileStore/tables/menu_csv.txt

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

sales_schema=StructType([StructField("item_id",IntegerType()),
                         StructField("customer_id",StringType()),
                         StructField("order_date",DateType()),
                         StructField("country",StringType()),
                         StructField("order_type",StringType())])

# COMMAND ----------

menu_schema=StructType([StructField("item_id",IntegerType()),
                        StructField("item_name",StringType()),
                        StructField("item_price",StringType())])

# COMMAND ----------

sales_df=spark.read.format("csv").schema(sales_schema).option("inferSchema","true").load("/FileStore/tables/sales_csv.txt")
sales_df.show()

# COMMAND ----------

sales_df.printSchema()

# COMMAND ----------

menu_df = spark.read.option("delimiter", ",").schema(menu_schema).csv("/FileStore/tables/menu_csv.txt")
menu_df.show()
menu_df.printSchema()

# COMMAND ----------

menu_df=menu_df.withColumn("item_price",col("item_price").cast(DecimalType(5,1)))
menu_df.show()
menu_df.printSchema()

# COMMAND ----------

sales_df=sales_df.withColumnRenamed("item_id","dish_id")
sales_menu_df=sales_df.join(menu_df,sales_df["dish_id"]==menu_df["item_id"],"inner")
sales_menu_df.show()

# COMMAND ----------

display(sales_menu_df.groupBy(col("customer_id")).agg(sum(col("item_price")).alias("revenue")).orderBy(col("revenue").desc()))

# COMMAND ----------

display(sales_menu_df.groupBy(col("item_name")).agg(sum(col("item_price")).alias("revenue")).orderBy(col("revenue").desc()))

# COMMAND ----------

display(sales_menu_df.groupBy(month(col("order_date")).alias("order_month")).agg(sum(col("item_price")).alias("revenue")).orderBy(col("revenue").desc()))

# COMMAND ----------

display(sales_menu_df.groupBy(quarter(col("order_date")).alias("order_quarter")).agg(sum(col("item_price")).alias("revenue")).orderBy(col("revenue").desc()))

# COMMAND ----------

display(sales_menu_df.groupBy(year(col("order_date")).alias("order_year")).agg(sum(col("item_price")).alias("revenue")).orderBy(col("revenue").desc()))

# COMMAND ----------

display(sales_menu_df.groupBy(col("item_name")).agg(count(col("item_id")).alias("total_orders")).orderBy(col("total_orders").desc()))

# COMMAND ----------

display(sales_menu_df.groupBy(col("item_name")).agg(count(col("item_id")).alias("total_orders")).orderBy(col("total_orders").desc()).limit(5))

# COMMAND ----------

display(sales_menu_df.groupBy(col("item_name")).agg(count(col("item_id")).alias("total_orders")).orderBy(col("total_orders").desc()).limit(1))

# COMMAND ----------

display(sales_menu_df.groupBy(col("customer_id")).agg(count(col("item_id")).alias("total_visits")).orderBy(col("total_visits").desc()))

# COMMAND ----------

display(sales_menu_df.groupBy(col("country")).agg(count(col("item_id")).alias("total_orders")).orderBy(col("total_orders").desc()))

# COMMAND ----------

display(sales_menu_df.groupBy(col("order_type")).agg(count(col("item_id")).alias("total_orders")).orderBy(col("total_orders").desc()))

# COMMAND ----------


