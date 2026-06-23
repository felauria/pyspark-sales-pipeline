from spark_session import get_spark

spark = get_spark()



#  ======================================
#  Lendo as tabelas da camada Bronze
#  ======================================

orders = spark.read.parquet(
    "data/bronze/olist_orders_dataset"
)

customers = spark.read.parquet(
    "data/bronze/olist_customers_dataset"
)

items = spark.read.parquet(
    "data/bronze/olist_order_items_dataset"
)

payments = spark.read.parquet(
    "data/bronze/olist_order_payments_dataset"
)

products = spark.read.parquet(
    "data/bronze/olist_products_dataset"
)

# ======================================

# tabela de vendas (sales) é criada a partir do join das tabelas de pedidos e clientes,
# usando a coluna customer_id como chave de junção.

# O tipo de junção é "left", o que significa que todos os registros da tabela orders serão
# mantidos, mesmo que não haja correspondência na tabela customers.
sales = orders.join(
    customers, # tabela alvo
    on="customer_id", # nome da primary key
    how="left" # tipo de join
)

# Fazemos o join da tabela sales com as outras tabelas usando as chaves relacionadas
# Podemos fazer todos os joins da seguinte forma:
sales = orders \
    .join(customers, on="customer_id", how="left") \
    .join(items, on="order_id", how="left") \
    .join(payments, on="order_id", how="left") \
    .join(products, on="product_id", how="left")
#! Deve se atentar a ordem dos joins, para que tenha a chave correta para cada join.

print(f"Total registros: {sales.count()}")
sales.printSchema()

sales.write \
    .mode("overwrite") \
    .parquet(
        "data/silver/sales"
    )

print("Tabela Silver criada com sucesso.")

print("\nVendas por estado:")
sales.groupBy(
    "customer_state"
).count().show()

print("\nTOTAL DE REGISTROS")
print(sales.count())

print("\nSCHEMA")
sales.printSchema()

print("\nAMOSTRA DOS DADOS")

sales.select(
    "order_id",
    "customer_state",
    "product_category_name",
    "price",
    "payment_value"
).show(20, truncate=False)
