# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08 - Gold: afinidade clima x categoria de comida
# MAGIC Tabela de regras (hipotese do projeto, NAO e dado real). Escala de 0 a 1.

# COMMAND ----------

from pyspark.sql import functions as F

TABELA_GOLD = "workspace.gold.dim_afinidade_clima_comida"

# Colunas: chuvoso, frio, ameno, calor_intenso
AFINIDADE = {
    "sopas_caldos":     (1.0, 1.0, 0.4, 0.0),
    "massas_pizza":     (0.9, 0.8, 0.6, 0.3),
    "brasileira":       (0.8, 0.8, 0.6, 0.4),
    "hamburguer":       (0.8, 0.6, 0.7, 0.5),
    "lanches":          (0.7, 0.6, 0.7, 0.5),
    "doces_sobremesas": (0.7, 0.7, 0.6, 0.5),
    "cafe_padaria":     (0.8, 0.9, 0.6, 0.3),
    "churrasco_carnes": (0.6, 0.7, 0.7, 0.3),
    "asiatica":         (0.7, 0.7, 0.7, 0.6),
    "internacional":    (0.6, 0.6, 0.7, 0.5),
    "frutos_do_mar":    (0.3, 0.3, 0.7, 0.8),
    "saudavel":         (0.3, 0.3, 0.7, 1.0),
    "sorvete_acai":     (0.1, 0.0, 0.6, 1.0),
    "geral":            (0.5, 0.5, 0.5, 0.5),   # fallback neutro
}
TAGS = ["chuvoso", "frio", "ameno", "calor_intenso"]

linhas = [
    (categoria, tag, float(score))
    for categoria, scores in AFINIDADE.items()
    for tag, score in zip(TAGS, scores)
]
dim = spark.createDataFrame(linhas, "categoria_comida string, clima_tag string, afinidade double")

(dim.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(TABELA_GOLD))

# COMMAND ----------

# Toda categoria da Silver precisa ter afinidade
silver = spark.table("workspace.silver.restaurants")
sem_afinidade = (silver.select("categoria_comida").distinct()
                 .join(spark.table(TABELA_GOLD).select("categoria_comida").distinct(),
                       "categoria_comida", "left_anti"))
n = sem_afinidade.count()
print("Linhas na dim:", spark.table(TABELA_GOLD).count(), "(esperado 56)")
print("Categorias da Silver sem afinidade:", n)
assert n == 0, "Existe categoria na Silver sem afinidade na Gold"
display(spark.table(TABELA_GOLD).orderBy("categoria_comida", "clima_tag"))