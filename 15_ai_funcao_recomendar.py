# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 15 - IA: função de recomendação por localização (ferramenta para Genie e agentes)
# MAGIC Registra no Unity Catalog a funcao SQL `gold.recomendar_por_local(p_lat, p_lon, p_n)`. Ela recebe a posicao do usuario e devolve os restaurantes recomendados para o clima de agora, pesando a distancia.
# MAGIC
# MAGIC **Por que existe:** funcoes do Unity Catalog podem ser usadas como **ferramenta** por um Genie Agent (como "trusted asset") e por agentes em codigo. A regra de negocio fica num lugar so, governada, e a IA chama a funcao em vez de inventar o calculo.
# MAGIC
# MAGIC **Regra:** `score_final = score_v1 x exp(-distancia_km / 1,5)`, apenas para restaurantes a ate 3 km. A distancia usa a formula de Haversine.
# MAGIC
# MAGIC **Limite conhecido:** le `gold.recommendations_today`, que guarda so os melhores de cada zona (grade de ~1 km). Entao os candidatos sao os melhores das zonas proximas, e nao todos os restaurantes do raio.
# MAGIC
# MAGIC Idempotente (`CREATE OR REPLACE`). Depende do notebook `11`.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE FUNCTION workspace.gold.recomendar_por_local(
  p_lat DOUBLE COMMENT 'Latitude do usuario, por exemplo -23.5614',
  p_lon DOUBLE COMMENT 'Longitude do usuario, por exemplo -46.6559',
  p_n INT DEFAULT 10 COMMENT 'Quantidade de recomendacoes a retornar'
)
RETURNS TABLE (
  posicao INT,
  restaurante STRING,
  categoria_comida STRING,
  bairro STRING,
  distancia_km DOUBLE,
  clima_tag STRING,
  aberto_agora BOOLEAN,
  score_final DOUBLE
)
COMMENT 'Recomenda restaurantes de Sao Paulo para o clima da hora atual, perto da posicao informada (ate 3 km). Use quando o usuario informar uma localizacao. Ordena por score_final, que combina afinidade com o clima, horario de funcionamento e distancia.'
RETURN
  WITH cand AS (
    SELECT
      name, categoria_comida, bairro, clima_tag, aberto_agora, score_v1,
      2 * 6371 * asin(sqrt(
        pow(sin(radians(lat - p_lat) / 2), 2)
        + cos(radians(p_lat)) * cos(radians(lat)) * pow(sin(radians(lon - p_lon) / 2), 2)
      )) AS dist_km
    FROM workspace.gold.recommendations_today
  ),
  pont AS (
    SELECT *, score_v1 * exp(-dist_km / 1.5) AS score_final
    FROM cand
    WHERE dist_km <= 3.0
  ),
  ranq AS (
    SELECT *, row_number() OVER (ORDER BY score_final DESC, name) AS posicao
    FROM pont
  )
  SELECT
    posicao, name, categoria_comida, bairro,
    ROUND(dist_km, 2), clima_tag, aberto_agora, ROUND(score_final, 3)
  FROM ranq
  WHERE posicao <= p_n
  ORDER BY posicao
""")
print("Funcao criada: workspace.gold.recomendar_por_local")

# COMMAND ----------

# Teste: usuario na Av. Paulista
res = spark.sql("SELECT * FROM workspace.gold.recomendar_por_local(-23.5614, -46.6559, 10)")
n = res.count()
print("Linhas retornadas:", n)
assert n > 0, "A funcao nao retornou nada. Confira se recommendations_today tem restaurantes perto da Av. Paulista."
display(res)