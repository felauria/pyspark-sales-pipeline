from spark_session import get_spark

spark = get_spark()

df = spark.read.parquet(
    "data/silver/orders"
)

print(df.count())

df.printSchema() # printa o schema do DF, mostrando os nomes das colunas e seus tipos de dados.

df.show(5)
