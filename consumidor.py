from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, IntegerType, StringType

# 1. Crear sesión de Spark con soporte para Kafka
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingTarea3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Leer el flujo de datos en tiempo real desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "datos-streaming") \
    .load()

# 3. Convertir los datos binarios que llegan de Kafka a un formato legible (JSON)
schema = StructType() \
    .add("id_evento", IntegerType()) \
    .add("valor", IntegerType()) \
    .add("estado", StringType())

df_json = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 4. Realizar alguna operación o análisis en tiempo real (ejemplo: filtrar los mayores a 50)
df_filtrado = df_json.filter(col("valor") > 50)

# 5. Mostrar los resultados en la consola en tiempo real
query = df_filtrado.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
