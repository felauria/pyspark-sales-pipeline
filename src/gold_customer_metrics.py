from pyspark.sql.functions import countDistinct # Conta valores distintos
from pyspark.sql.functions import sum # Soma os valores de uma coluna
from pyspark.sql.functions import avg # Calcula a média dos valores de uma coluna
from pyspark.sql.functions import col # Referência para colunas
from pyspark.sql.window import Window # Cria janelas para Window Functions
from pyspark.sql.functions import rank # Cria ranking

from spark_session import get_spark

spark = get_spark()

# Leu o parquet de sales da camada Silver
# ----------------------------------------------------------
# A tabela sales possui granularidade de ITEM.
#
# Cada linha representa um produto vendido dentro de um pedido.
#
# Por isso:
#
# count(order_id) -> ERRADO
# porque um mesmo pedido pode aparecer várias vezes.
#
# countDistinct(order_id) -> CORRETO
# porque conta apenas pedidos únicos.
#
# Outro cuidado:
#
# payment_value NÃO pode ser usado aqui.
#
# payment_value possui granularidade de pedido.
# Como a tabela está em granularidade de item,
# o valor seria duplicado após os joins.
#
# Por isso vamos utilizar:
#
# price + freight_value
#
# Ambas as colunas vêm da tabela order_items e possuem
# a mesma granularidade da tabela sales.
# ----------------------------------------------------------

sales = spark.read.parquet(
    "data/silver/sales"
)


gold_custumer_metrics = (
    sales
    .groupBy("customer_unique_id")
    .agg(
        # Quantidade de pedidos distintos feitos pelo cliente
        countDistinct("order_id")
            .alias("total_orders"),

        # Receita total gerada pelo cliente
        #
        # Utilizamos price + freight_value porque ambas
        # possuem granularidade de item.
        sum(
            col("price") + col("freight_value")
        ).alias("total_spent"),

        # Valor médio por item vendido
        avg(
            col("price") + col("freight_value")
        ).alias("average_item_value")
    )
)

# Cria uma janela ordenando pelo total gasto em ordem decrescente
WINDOW_SPEC = Window.orderBy(
    gold_custumer_metrics.total_spent.desc()
)

# Cria ranking dos clientes
gold_custumer_metrics = gold_custumer_metrics.withColumn(
    "customer_rank",
    rank().over(WINDOW_SPEC)
)

# Salva a camada Gold
gold_custumer_metrics.write \
    .mode("overwrite") \
    .parquet(
        "data/gold/gold_custumer_metrics"
    )

# Exibe os 20 melhores clientes
gold_custumer_metrics.orderBy(
    "customer_rank"
).show(20, truncate=False)

gold_custumer_metrics.orderBy(
    "total_orders",
    ascending=False
).show(20, truncate=False)
