# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 09 - Gold: contexto de clima
# MAGIC Transforma cada hora da Silver em uma tag: chuvoso, calor_intenso, frio ou ameno.

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_CLIMA = "workspace.silver.weather_hourly"
GOLD_CLIMA = "workspace.gold.weather_context"

# Limiares (ajuste se quiser)
CHUVA_MM = 0.5      # mm na hora
CHUVA_PROB = 60     # % de probabilidade (so existe na previsao)
CALOR_C = 30        # sensacao termica >= 30 -> calor intenso
FRIO_C = 17         # sensacao termica <= 17 -> frio

df = spark.table(SILVER_CLIMA)

chuvoso = F.coalesce(
    (F.col("precipitation_mm") >= CHUVA_MM) | (F.col("precipitation_probability") >= CHUVA_PROB),
    F.lit(False),
)
sensacao = F.coalesce("apparent_temperature_c", "temperature_c")

gold = (df
    .withColumn("sensacao_c", sensacao)
    .withColumn("clima_tag",
        F.when(chuvoso, "chuvoso")                       # chuva vence calor e frio
         .when(F.col("sensacao_c") >= CALOR_C, "calor_intenso")
         .when(F.col("sensacao_c") <= FRIO_C, "frio")
         .otherwise("ameno"))
    .withColumn("data_ref", F.to_date("observed_at"))
    .withColumn("hora", F.hour("observed_at"))
    .withColumn("dia_semana_num", F.dayofweek("observed_at"))   # 1 = domingo
    .select("cidade", "observed_at", "data_ref", "hora", "dia_semana_num", "tipo",
            "temperature_c", "apparent_temperature_c", "sensacao_c",
            "precipitation_mm", "precipitation_probability", "clima_tag"))

(gold.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(GOLD_CLIMA))

# COMMAND ----------

g = spark.table(GOLD_CLIMA)
print("Linhas Silver:", df.count(), "| Linhas Gold:", g.count())
assert g.count() == df.count(), "Gold de clima com quantidade diferente da Silver"
display(g.groupBy("tipo", "clima_tag").count().orderBy("tipo", "clima_tag"))