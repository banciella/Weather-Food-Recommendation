# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 11 - Gold: recomendacoes de hoje
# MAGIC Combina o clima da hora atual (Sao Paulo), "aberto agora" e o score base.
# MAGIC Gera `recommendations_today` por zona (grade de ~1 km) e a funcao `recomendar(lat, lon)`.

# COMMAND ----------

import re
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window

GOLD_CLIMA  = "workspace.gold.weather_context"
GOLD_SCORES = "workspace.gold.recommendation_scores"
GOLD_HOJE   = "workspace.gold.recommendations_today"

FORCAR_TAG = None   # para testar outro clima, ex.: "chuvoso" (nesse caso NAO grava a tabela)

agora_sp = datetime.now(ZoneInfo("America/Sao_Paulo"))
hora_ref = agora_sp.strftime("%Y-%m-%d %H:00:00")
dia_semana = agora_sp.weekday()                 # 0 = segunda
minutos = agora_sp.hour * 60 + agora_sp.minute
print("Agora (SP):", agora_sp.strftime("%Y-%m-%d %H:%M"))

# COMMAND ----------

# Parser simples de opening_hours do OpenStreetMap.
# Retorna True (aberto), False (fechado) ou None (ausente ou formato nao suportado).
_DIAS = {"mo": 0, "tu": 1, "we": 2, "th": 3, "fr": 4, "sa": 5, "su": 6}
_DIA = r"(?:Mo|Tu|We|Th|Fr|Sa|Su)"
_DIAS_RE = rf"{_DIA}(?:\s*[-,]\s*{_DIA})*"
_HORA = r"\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}"
_HORAS_RE = rf"{_HORA}(?:\s*,\s*{_HORA})*"
_REGRA = re.compile(rf"^(?:({_DIAS_RE})\s+)?(?:({_HORAS_RE})|(off|closed))$", re.I)


def _expandir_dias(txt):
    dias = []
    for parte in re.sub(r"\s+", "", txt).split(","):
        if "-" in parte:
            a, b = parte.split("-")
            i, fim = _DIAS[a.lower()], _DIAS[b.lower()]
            while True:
                dias.append(i)
                if i == fim:
                    break
                i = (i + 1) % 7
        else:
            dias.append(_DIAS[parte.lower()])
    return dias


def _intervalo(txt):
    ini, fim = [p.strip() for p in txt.split("-")]
    h1, m1 = [int(x) for x in ini.split(":")]
    h2, m2 = [int(x) for x in fim.split(":")]
    a, b = h1 * 60 + m1, h2 * 60 + m2
    if b <= a:          # passa da meia-noite
        b += 1440
    return a, b


def aberto_agora(oh, wd, minutos):
    if oh is None:
        return None
    s = oh.strip()
    if not s or "||" in s:
        return None
    if s == "24/7":
        return True
    sched = {}
    for regra in s.split(";"):
        regra = regra.strip()
        if not regra or re.match(r"^(PH|SH)\b", regra, re.I):
            continue
        m = _REGRA.match(regra)
        if not m:
            return None
        dias_txt, horas_txt, off = m.groups()
        dias = _expandir_dias(dias_txt) if dias_txt else list(range(7))
        intervalos = [] if off else [_intervalo(h) for h in horas_txt.split(",")]
        for d in dias:
            sched[d] = intervalos
    if not sched:
        return None
    for a, b in sched.get(wd, []):
        if a <= minutos < b:
            return True
    for a, b in sched.get((wd - 1) % 7, []):      # turno de ontem que passou da meia-noite
        if b > 1440 and minutos < b - 1440:
            return True
    return False


aberto_udf = F.udf(lambda oh: aberto_agora(oh, dia_semana, minutos), BooleanType())

# COMMAND ----------

# Clima da hora atual (previsao). Se nao existir, o pipeline precisa ser reexecutado.
linha = (spark.table(GOLD_CLIMA)
         .filter(F.col("tipo") == "forecast")
         .filter(F.date_format("observed_at", "yyyy-MM-dd HH:mm:ss") == hora_ref)
         .first())
if linha is None:
    raise RuntimeError(f"Sem previsao para {hora_ref}. Rode 04_bronze_clima, 06_silver_clima e 09_gold_weather_context.")

tag = FORCAR_TAG or linha["clima_tag"]
if FORCAR_TAG:
    print("ATENCAO: clima forcado para teste ->", FORCAR_TAG)
print("Clima:", linha["observed_at"], "| tag:", tag, "| sensacao:", linha["sensacao_c"], "C")

base_hoje = (spark.table(GOLD_SCORES)
    .filter(F.col("clima_tag") == tag)
    .withColumn("aberto_agora", aberto_udf("opening_hours"))
    .withColumn("fator_aberto",
        F.when(F.col("aberto_agora") == True, 1.0)       # confirmado aberto
         .when(F.col("aberto_agora").isNull(), 0.85)     # sem horario/formato desconhecido
         .otherwise(0.0))                                # fechado
    .filter("fator_aberto > 0")
    .withColumn("score_v1", F.round(F.col("score_base") * F.col("fator_aberto"), 3)))

# COMMAND ----------

# Tabela por zona (grade de 0,01 grau, ~1 km)
semente = F.xxhash64("restaurant_id", F.lit(hora_ref[:10]))   # desempate que varia por dia
zona = (base_hoje
    .withColumn("zona_id", F.concat_ws("_", F.floor(F.col("lat") * 100).cast("string"),
                                             F.floor(F.col("lon") * 100).cast("string")))
    .withColumn("zona_lat", (F.floor(F.col("lat") * 100) + 0.5) / 100)
    .withColumn("zona_lon", (F.floor(F.col("lon") * 100) + 0.5) / 100))

w_cat = Window.partitionBy("zona_id", "categoria_comida").orderBy(F.desc("score_v1"), semente)
w_zona = Window.partitionBy("zona_id").orderBy(F.desc("score_v1"), "rank_categoria", semente)

hoje = (zona
    .withColumn("rank_categoria", F.row_number().over(w_cat))
    .filter("rank_categoria <= 2")                 # no maximo 2 por categoria em cada zona
    .withColumn("rank_zona", F.row_number().over(w_zona))
    .filter("rank_zona <= 10")
    .withColumn("cidade", F.lit(linha["cidade"]))
    .withColumn("hora_ref", F.lit(linha["observed_at"]))
    .withColumn("clima_tag", F.lit(tag))
    .withColumn("sensacao_c", F.lit(linha["sensacao_c"]))
    .select("cidade", "hora_ref", "clima_tag", "sensacao_c", "zona_id", "zona_lat", "zona_lon",
            "rank_zona", "restaurant_id", "name", "amenity", "categoria_comida", "bairro",
            "lat", "lon", "aberto_agora", "score_v1"))

if FORCAR_TAG is None:
    hoje.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_HOJE)
    h = spark.table(GOLD_HOJE)
    print("Linhas:", h.count(), "| Zonas:", h.select("zona_id").distinct().count())
    assert h.count() > 0, "recommendations_today vazia"
    display(h.groupBy("categoria_comida").count().orderBy(F.desc("count")))
else:
    print("Teste com clima forcado: tabela NAO gravada.")

# COMMAND ----------

# Simula o usuario abrindo o app: recomenda pela localizacao (proximidade entra aqui)
def recomendar(lat, lon, n=10, raio_km=3.0, escala_km=1.5):
    graus_lat = raio_km / 111.0
    graus_lon = raio_km / (111.0 * 0.92)          # cos(23,5 graus) ~ 0,92
    R = 6371.0
    dlat = F.radians(F.col("lat") - F.lit(lat))
    dlon = F.radians(F.col("lon") - F.lit(lon))
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(F.lit(lat))) * F.cos(F.radians(F.col("lat"))) * F.sin(dlon / 2) ** 2)
    dist = 2 * R * F.asin(F.sqrt(a))

    cand = (base_hoje
        .filter(F.col("lat").between(lat - graus_lat, lat + graus_lat)
                & F.col("lon").between(lon - graus_lon, lon + graus_lon))
        .withColumn("dist_km", F.round(dist, 2))
        .filter(F.col("dist_km") <= raio_km)
        .withColumn("proximidade", F.exp(-F.col("dist_km") / F.lit(escala_km)))
        .withColumn("score_final", F.round(F.col("score_v1") * F.col("proximidade"), 3)))

    w1 = Window.partitionBy("categoria_comida").orderBy(F.desc("score_final"))
    w2 = Window.orderBy(F.desc("score_final"), "rank_cat")
    return (cand.withColumn("rank_cat", F.row_number().over(w1))
                .filter("rank_cat <= 2")               # diversidade: 2 por categoria
                .withColumn("rank", F.row_number().over(w2))
                .filter(F.col("rank") <= n)
                .select("rank", "name", "categoria_comida", "bairro", "dist_km",
                        "aberto_agora", "score_v1", "proximidade", "score_final"))

print("Usuario na Av. Paulista:")
display(recomendar(-23.5614, -46.6559))