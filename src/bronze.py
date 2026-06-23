from pathlib import Path

from spark_session import get_spark

# Cria sessão Spark
spark = get_spark()

# Caminho dos arquivos originais
RAW_PATH = "data/raw"

# Caminho da camada Bronze
BRONZE_PATH = "data/bronze"

# Lista todos os CSVs da pasta raw
csv_files = Path(RAW_PATH).glob("*.csv")

for file in csv_files:

    print(f"Lendo arquivo: {file.name}")

    # Lê CSV
    df = spark.read.csv(
        str(file),
        header=True,
        inferSchema=True
    )

    # Nome da tabela
    table_name = file.stem

    print(f"Salvando tabela Bronze: {table_name}")

    # Salva em formato parquet
    df.write.mode("overwrite").parquet(
        f"{BRONZE_PATH}/{table_name}"
    )

print("Bronze Layer criada com sucesso.")
