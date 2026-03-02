import dlt
from pyspark.sql.functions import *

@dlt.table(name="gold.fact_sales")
def gold_fact_sales():
    return (
        dlt.read("silver.fact_sales_base")
            .withColumn("purchase_date",
                        to_date("order_purchase_timestamp"))
            .withColumn("delivery_date",
                        to_date("order_delivered_customer_date"))
    )

@dlt.table(name="gold.revenue_daily")
def revenue_daily():
    return (
        dlt.read("gold.fact_sales")
            .groupBy("purchase_date")
            .agg(
                sum("total_amount").alias("daily_revenue"),
                count("order_id").alias("total_orders")
            )
    )

@dlt.table(name="gold.revenue_by_category")
def revenue_by_category():

    fact = dlt.read("gold.fact_sales")
    products = dlt.read("silver.dim_product_scd")

    return (
        fact.join(products, "product_id")
            .groupBy("product_category_name_english")
            .agg(
                sum("total_amount").alias("category_revenue"),
                count("order_id").alias("total_orders")
            )
    )

@dlt.table(name="gold.late_delivery_rate")
def late_delivery_rate():
    return (
        dlt.read("gold.fact_sales")
            .withColumn(
                "is_late",
                when(col("delivery_delay_days") > 0, 1).otherwise(0)
            )
            .groupBy("purchase_date")
            .agg(
                (sum("is_late") / count("*")).alias("late_delivery_rate")
            )
    )