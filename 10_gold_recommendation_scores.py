# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 10 - Gold: score base por restaurante x clima
# MAGIC score_base = afinidade(clima, categoria) x confianca da classificacao.
# MAGIC "Aberto agora" e proximidade entram no notebook 11, porque dependem da hora e do usuario.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_REST = "workspace.silver.restaurants"
GOLD_AFIN   = "workspace.gold.dim_afinidade_clima_comida"
GOLD_SCORES = "workspace.gold.recommendation_scores"

rest = spark.table(SILVER_REST)
afin = spark.table(GOLD_AFIN).withColumnRenamed("afinidade", "afinidade_clima")

# Confianca na classificacao: cuisine > nome > amenity/default
confianca = (F.when(F.col("categoria_fonte") == "cuisine", 1.0)
              .when(F.col("categoria_fonte") == "nome", 0.9)
              .otherwise(0.8))

scores = (rest.join(afin, "categoria_comida", "left")
    .withColumn("afinidade_clima", F.coalesce("afinidade_clima", F.lit(0.5)))
    .withColumn("clima_tag", F.coalesce("clima_tag", F.lit("ameno")))
    .withColumn("confianca", confianca)
    .withColumn("score_base", F.round(F.col("afinidade_clima") * F.col("confianca"), 3))
    .select("restaurant_id", "name", "amenity", "categoria_comida", "categoria_fonte",
            "bairro", "lat", "lon", "opening_hours", "has_opening_hours",
            "clima_tag", "afinidade_clima", "confianca", "score_base"))

(scores.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(GOLD_SCORES))

# COMMAND ----------

# Checagens
total_rest = rest.count()
g = spark.table(GOLD_SCORES)
print("Restaurantes na Silver:", total_rest)
print("Linhas na Gold:", g.count(), f"(esperado {total_rest * 4})")
assert g.count() == total_rest * 4, "Esperado 4 linhas (uma por clima) por restaurante"
assert g.filter("score_base IS NULL").count() == 0, "score_base nulo"

# Top 3 categorias por clima (deve fazer sentido: chuva/frio -> sopa, massa; calor -> acai, saudavel)
w = Window.partitionBy("clima_tag").orderBy(F.desc("score"))
display(
    g.groupBy("clima_tag", "categoria_comida").agg(F.max("score_base").alias("score"))
     .withColumn("r", F.row_number().over(w)).filter("r <= 3")
     .orderBy("clima_tag", "r")
)