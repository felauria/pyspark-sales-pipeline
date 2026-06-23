from pyspark.sql.functions import col # Usada para referenciar uma coluna pelo nome.
from pyspark.sql.functions import datediff # Usada para calcular a diferença entre duas datas.
from pyspark.sql.functions import year # Usada para extrair o ano de uma coluna de data.
from pyspark.sql.functions import month # Usada para extrair o mês de uma coluna de data.
from pyspark.sql.functions import when # Usada para criar uma coluna condicional, semelhante a um "if" em programação.

from spark_session import get_spark

spark = get_spark()

# ==================================================
# LEITURA DA CAMADA BRONZE
# ==================================================

orders = spark.read.parquet(
    "data/bronze/olist_orders_dataset"
)

print("Total Bronze:")
print(orders.count())

# irá remover registros sem cliente
orders = orders.filter(
    col("customer_id").isNotNull()
)

# Retorna um novo DF adicionando uma coluna ou substituindo a coluna existente que tem o mesmo nome.
# Aqui adiciona a coluna "delivery_days" que calcula a diferença entre as datas
orders = orders.withColumn(
    "delivery_days",
    datediff(
        col("order_delivered_customer_date"),
        col("order_purchase_timestamp")
    )
)

# Adiciona uma coluna "purchase_year" que extrai o ano da coluna "order_purchase_timestamp".
orders = orders.withColumn(
    "purchase_year",
    year(
        col("order_purchase_timestamp")
    )
)

# Adiciona uma coluna "purchase_month" que extrai o mês da coluna "order_purchase_timestamp".
orders = orders.withColumn(
    "purchase_month",
    month(
        col("order_purchase_timestamp")
    )
)

# Adiciona uma coluna "is_delayed" que indica se o pedido foi entregue com atraso.
orders = orders.withColumn(
    "is_delayed",
    when( # Utiliza o "when" para criar a condição
        col("order_delivered_customer_date")
        >
        col("order_estimated_delivery_date"),
        1
    ).otherwise(0) # Se a condição não for atendida, atribui 0 (não atrasado)
)

orders.select( # literalmente seleciona as colunas que queremos visualizar no DataFrame final.
    "order_id",
    "delivery_days",
    "purchase_year",
    "purchase_month",
    "is_delayed"
).show(10, truncate=False) # Mostra os 10 primeiros registros do DataFrame resultante

# Salva o DataFrame resultante na camada Silver em formato Parquet
orders.write \
    .mode("overwrite") \
    .parquet(
        "data/silver/orders"
    )
