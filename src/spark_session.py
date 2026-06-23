from pyspark.sql import SparkSession

def get_spark():
    """
    Cria e retorna uma SparkSession.

    A SparkSession é o ponto de entrada para utilizar o Spark.
    É através dela que conseguimos:
    - Ler arquivos
    - Executar Spark SQL
    - Criar DataFrames
    - Salvar dados
    """

    spark = (
        SparkSession.builder
        .appName("Olist Data Pipeline")
        # memória do driver
        .config("spark.driver.memory", "4g")
        # número de partições padrão
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    return spark
