from pyspark.sql.functions import avg
from pyspark.sql.functions import datediff

from spark_session import get_spark

spark = get_spark()

orders = spark.read.parquet(
    "data/bronze/olist_orders_dataset"
)

print("=" * 50)
print("DATA QUALITY REPORT - ORDERS")
print("=" * 50)

# Total de registros
print(f"Total de pedidos: {orders.count()}")

print("\nNULOS POR COLUNA\n")

for column in orders.columns:

    null_count = orders.filter(
        orders[column].isNull()
    ).count()

    print(
        f"{column}: {null_count} valores nulos"
    )

# Pedidos por status
print("\nPEDIDOS POR STATUS\n")

orders.groupBy(
    "order_status"
).count().show()

# Pedidos sem aprovação
not_approved = orders.filter(
    orders.order_approved_at.isNull()
).count()

print(f"\nPedidos sem aprovação: {not_approved}")

# Pedidos sem entrega
not_delivered = orders.filter(
    orders.order_delivered_customer_date.isNull()
).count()

print(f"\nPedidos sem entrega: {not_delivered}")

# Tempo médio de entrega
delivery_df = (
    orders
    .filter(
        orders.order_delivered_customer_date.isNotNull()
    )
    .withColumn(
        "delivery_days",
        datediff(
            orders.order_delivered_customer_date,
            orders.order_purchase_timestamp
        )
    )
)

print("\nTEMPO MÉDIO DE ENTREGA\n")

delivery_df.select(
    avg("delivery_days")
).show()
