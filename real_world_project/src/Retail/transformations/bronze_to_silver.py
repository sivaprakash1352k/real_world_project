import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="silver.customers_clean",
           )
def customers_clean():
    return (
        spark.readStream.table("customers_bronze")
            .select(
                col("customer_id"),
                col("customer_unique_id"),
                col("customer_city"),
                col("customer_state"),
                col("ingestion_timestamp")
            )
            .dropDuplicates(["customer_id"])
    )

@dlt.table(
    name="silver.orders_clean"
)
def orders_clean():
    return (
        dlt.read_stream("orders_bronze")
            .withColumn("order_purchase_timestamp",
                        col("order_purchase_timestamp").cast("timestamp"))
            .withColumn("order_approved_at",
                        col("order_approved_at").cast("timestamp"))
            .withColumn("order_delivered_carrier_date",
                        col("order_delivered_carrier_date").cast("timestamp"))
            .withColumn("order_delivered_customer_date",
                        col("order_delivered_customer_date").cast("timestamp"))
            .withColumn("order_estimated_delivery_date",
                        col("order_estimated_delivery_date").cast("timestamp"))
            .dropDuplicates(["order_id"])
    )

@dlt.table(
    name="silver.order_items_clean"
)
def order_items_clean():
    return (
        dlt.read_stream("order_items_bronze")
            .withColumn("price", col("price").cast("double"))
            .withColumn("freight_value", col("freight_value").cast("double"))
            .dropDuplicates(["order_id", "order_item_id"])
    )

@dlt.table(
name="silver.products_clean"
)
def products_clean():
    return (
        dlt.read_stream("products_bronze")
            .select(
                col("product_id"),
                col("product_category_name"),
                col("product_weight_g").cast("int"),
                col("product_length_cm").cast("int"),
                col("product_height_cm").cast("int"),
                col("product_width_cm").cast("int"),
                col("ingestion_timestamp")
            )
            .dropDuplicates(["product_id"])
    )

@dlt.table(
name="silver.product_category_translation_clean"
)
def product_category_translation_clean():
    return (
        dlt.read_stream("product_category_name_translation_bronze")
            .select(
                col("product_category_name"),
                col("product_category_name_english")
            )
            .dropDuplicates(["product_category_name"])
    )

@dlt.table(
    name="silver.products_enriched"
)
def products_enriched():
    return (
        dlt.read("silver.products_clean")
            .join(
                dlt.read("silver.product_category_translation_clean"),
                on="product_category_name",
                how="left"
            )
    )

@dlt.table(
    name="silver.sellers_clean"
)
def sellers_clean():
    return (
        dlt.read_stream("sellers_bronze")
            .select(
                col("seller_id"),
                col("seller_zip_code_prefix"),
                col("seller_city"),
                col("seller_state")
            )
            .dropDuplicates(["seller_id"])
    )

@dlt.table(
    name="silver.payments_clean"
)
def payments_clean():
    return (
        dlt.read_stream("payments_bronze")
            .withColumn("payment_installments",
                        col("payment_installments").cast("int"))
            .withColumn("payment_value",
                        col("payment_value").cast("double"))
    )

@dlt.table(
    name="silver.reviews_clean"
)
def reviews_clean():
    return (
        dlt.read_stream("reviews_bronze")
            .withColumn("review_creation_date",
                        col("review_creation_date").cast("timestamp"))
            .withColumn("review_answer_timestamp",
                        col("review_answer_timestamp").cast("timestamp"))
            .dropDuplicates(["review_id"])
    )

dlt.create_streaming_table(
    name = 'silver.dim_product_scd'
)

dlt.create_auto_cdc_flow(
    source="silver.products_enriched",
    target="silver.dim_product_scd",
    keys=["product_id"],
    sequence_by=col("ingestion_timestamp"),
    stored_as_scd_type="2",
    except_column_list=["ingestion_timestamp"]
)

@dlt.table(name="silver.dim_customer")
def dim_customer():
    return (
        dlt.read("silver.customers_clean")
            .withColumn("customer_key",
                        monotonically_increasing_id())
    )

@dlt.table(name="silver.dim_seller")
def dim_seller():
    return (
        dlt.read("silver.sellers_clean")
            .withColumn("seller_key",
                        monotonically_increasing_id())
    )

@dlt.table(name="silver.dim_date")
def dim_date():

    orders = dlt.read("silver.orders_clean")

    return (
        orders.select(
            to_date("order_purchase_timestamp").alias("date")
        )
        .distinct()
        .withColumn("date_key",
                    date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .withColumn("day", dayofmonth("date"))
        .withColumn("quarter", quarter("date"))
        .withColumn("week_of_year", weekofyear("date"))
        .withColumn("is_weekend",
                    when(dayofweek("date").isin(1,7), True).otherwise(False))
    )

@dlt.table(name="silver.fact_sales_base")
def fact_sales_base():

    orders = dlt.read("silver.orders_clean")
    items = dlt.read("silver.order_items_clean")
    products = dlt.read("silver.dim_product_scd")
    customers = dlt.read("silver.dim_customer")
    sellers = dlt.read("silver.dim_seller")
    joined = (
    items
    .join(orders, items.order_id == orders.order_id, "inner")
    .join(products, items.product_id == products.product_id, "left")
    .join(customers, orders.customer_id == customers.customer_id, "left")
    .join(sellers, items.seller_id == sellers.seller_id, "left")
    )
    return(
        joined.select(
            # Degenerate dimensions
            items.order_id,
            items.order_item_id,

            # Foreign keys (for now natural keys)
            products.product_id,
            customers.customer_id,
            sellers.seller_id,

            # Measures
            items.price,
            items.freight_value,
            (items.price + items.freight_value).alias("total_amount"),

            # Dates
            orders.order_purchase_timestamp,
            orders.order_delivered_customer_date,

            # Derived metric
            datediff(
                orders.order_delivered_customer_date,
                orders.order_purchase_timestamp
            ).alias("delivery_delay_days"),

            orders.order_status
        )
    )
