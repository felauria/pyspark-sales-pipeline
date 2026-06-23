from spark_session import get_spark

spark = get_spark()

orders = spark.read.parquet(
    "data/bronze/olist_orders_dataset"
)

print("=" * 50)
print("DATA QUALITY REPORT")
print("=" * 50)

# Total de registros
total = orders.count()

print(f"Total de registros: {total}")

# Verifica nulos
for column in orders.columns:

    null_count = orders.filter(
        orders[column].isNull()
    ).count()

    print(
        f"{column}: {null_count} valores nulos"
    )
