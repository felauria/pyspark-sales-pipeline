from spark_session import get_spark

spark = get_spark()

df = spark.read.parquet(
    "data/bronze/olist_orders_dataset"
)

print("Quantidade de registros:")
print(df.count())

print("Schema:")

df.printSchema()

print("Primeiros registros:")

df.show(5)
