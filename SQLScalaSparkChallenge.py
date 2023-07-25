from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc, count, countDistinct, when, col, row_number
from pyspark.sql.window import Window

# Set the Hadoop home directory and suppress logging
import os
os.environ["hadoop.home.dir"] = "C:\\hadoop"
import logging
logging.getLogger("org").setLevel(logging.ERROR)

# Create a SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("PySpark Challenge") \
    .getOrCreate()

# Replace the following paths with the absolute paths to your CSV files
members_csv_path = r"C:\Users\medis\PycharmProjects\Sql_Scala_Challenge\members.csv"
sales_csv_path = r"C:\Users\medis\PycharmProjects\Sql_Scala_Challenge\sales.csv"
menu_csv_path = r"C:\Users\medis\PycharmProjects\Sql_Scala_Challenge\menu.csv"

# Read the CSV files
membersDF = spark.read.schema("customer_id STRING, join_date DATE").option("header", True).csv(members_csv_path)
salesDF = spark.read.schema("customer_id STRING, order_date DATE, product_id INT").option("header", True).csv(sales_csv_path)
menuDF = spark.read.schema("product_id INT, product_name STRING, price INT").option("header", True).csv(menu_csv_path)

# Query 1: What is the total amount each customer spent at the restaurant?
total_amount_spent_df = salesDF.join(menuDF, salesDF["product_id"] == menuDF["product_id"], "inner") \
    .groupBy("customer_id").agg(sum("price").alias("total_amount_spent")).sort(desc('total_amount_spent'))
total_amount_spent_df.show()

# Query 2: How many days has each customer visited the restaurant?
days_visited_df = salesDF.groupby("customer_id") \
    .agg(countDistinct("order_date").alias("Days_Visited")) \
    .orderBy(col("Days_Visited").desc())
days_visited_df.show()

# Query 3: What was the first item from the menu purchased by each customer?
testWindow = Window.partitionBy("customer_id").orderBy("order_date")
first_item_df = salesDF.withColumn("row_number", row_number().over(testWindow)) \
    .join(menuDF, salesDF["product_id"] == menuDF["product_id"]) \
    .where(col("row_number") == 1).select("customer_id", "product_name")
first_item_df.show()

#Query 4. What is the most purchased item on the menu and how many times was it purchased by all customers?
salesDF1 = salesDF.withColumnRenamed("product_id", "spid")
salesDF1.join(menuDF, menuDF["product_id"] == salesDF1["spid"], "inner").groupBy("product_name").agg(count("product_id").alias("amt_ordered")).sort(desc("amt_ordered")).limit(1).show()

#Query 5 Which item was the most popular for each customer
salesDF1 = salesDF.withColumnRenamed("product_id", "pid")
salesDF1.groupBy("customer_id").agg(
    count(when(salesDF1["pid"] == 1,1)).alias("Sushi Ordered"),
    count(when(salesDF1["pid"] == 2,1)).alias("Curry Ordered"),
    count(when(salesDF1["pid"] == 3,1)).alias("Ramen Ordered")).sort(("customer_id"))\
    .show()

# Create temporary views for the DataFrames
membersDF.createOrReplaceTempView("members")
salesDF.createOrReplaceTempView("sales")
menuDF.createOrReplaceTempView("menu")

# Query to find the first item purchased by each customer after they became a member
spark.sql("""
SELECT m.customer_id, MIN(s.order_date) AS first_purchase_date, menu.product_name AS first_purchased_item
FROM members m
JOIN sales s ON m.customer_id = s.customer_id
JOIN menu ON s.product_id = menu.product_id
WHERE s.order_date >= m.join_date
GROUP BY m.customer_id, menu.product_name
""").show()

# Query to find the item purchased just before the customer became a member
spark.sql("""
SELECT m.customer_id, MAX(s.order_date) AS last_purchase_date, menu.product_name AS last_purchased_item
FROM members m
JOIN sales s ON m.customer_id = s.customer_id
JOIN menu ON s.product_id = menu.product_id
WHERE s.order_date < m.join_date
GROUP BY m.customer_id, menu.product_name
""").show()


# Query to find the total items and amount spent for each member before they became a member
spark.sql("""
SELECT m.customer_id, COUNT(s.product_id) AS total_items_purchased, SUM(menu.price) AS total_amount_spent
FROM members m
JOIN sales s ON m.customer_id = s.customer_id
JOIN menu ON s.product_id = menu.product_id
WHERE s.order_date < m.join_date
GROUP BY m.customer_id
""").show()

# Query to calculate points for each customer considering the 2x multiplier for sushi
spark.sql("""
SELECT s.customer_id,
       SUM(CASE WHEN menu.product_name = 'Sushi' THEN (menu.price * 2 * 10) ELSE (menu.price * 10) END) AS total_points
FROM sales s
JOIN menu ON s.product_id = menu.product_id
GROUP BY s.customer_id
""").show()


