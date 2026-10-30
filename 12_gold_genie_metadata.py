# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 12 - Gold: descrições para o Genie Agent
# MAGIC Grava comentarios (descricao de tabelas e colunas) no Unity Catalog. O Genie usa esses textos para entender o que cada campo significa e gerar o SQL certo.
# MAGIC
# MAGIC **Tabelas descritas:**
# MAGIC - `gold.recommendations_today`: top restaurantes por zona para o clima da hora atual
# MAGIC - `gold.weather_context`: clima hora a hora, com a tag de clima
# MAGIC - `gold.recommendation_scores`: score base de cada restaurante em cada clima
# MAGIC - `gold.dim_afinidade_clima_comida`: nota de afinidade entre clima e categoria de comida
# MAGIC - `silver.restaurants`: cadastro de restaurantes (1 linha por restaurante)
# MAGIC
# MAGIC As tabelas Gold sao regravadas (`overwrite`) a cada execucao do job, o que pode apagar os comentarios. Por isso este notebook e a **ultima task** do job e e **idempotente**: roda sempre e reaplica tudo. Colunas que nao existirem na tabela sao ignoradas, com aviso.

# COMMAND ----------

def esc(texto):
    return texto.replace("'", "\\'")


def comentar(tabela, descricao_tabela, colunas):
    existentes = {c.lower() for c in spark.table(tabela).columns}
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{esc(descricao_tabela)}'")
    aplicadas, ignoradas = 0, []
    for col, desc in colunas.items():
        if col.lower() in existentes:
            spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {col} COMMENT '{esc(desc)}'")
            aplicadas += 1
        else:
            ignoradas.append(col)
    print(f"{tabela}: {aplicadas} colunas comentadas | ignoradas: {ignoradas or 'nenhuma'}")

# COMMAND ----------

comentar(
    "workspace.gold.recommendations_today",
    "Recomendacoes de restaurantes para o clima da hora atual em Sao Paulo. Cada linha e um restaurante sugerido em uma zona (grade de ~1 km). Contem so as melhores opcoes (ate 10 por zona), nao o cadastro completo.",
    {
        "cidade": "Cidade da recomendacao (sao_paulo).",
        "hora_ref": "Hora do clima usada na recomendacao (horario de Sao Paulo).",
        "clima_tag": "Tag do clima da hora: chuvoso, frio, ameno ou calor_intenso.",
        "sensacao_c": "Sensacao termica em graus Celsius na hora de referencia.",
        "zona_id": "Identificador da zona (grade de ~1 km), formato latitude_longitude x100.",
        "zona_lat": "Latitude do centro da zona.",
        "zona_lon": "Longitude do centro da zona.",
        "rank_zona": "Posicao do restaurante na zona (1 = melhor recomendacao).",
        "restaurant_id": "Identificador do restaurante no OpenStreetMap (tipo + id).",
        "name": "Nome do restaurante.",
        "amenity": "Tipo de estabelecimento no OpenStreetMap: restaurant, fast_food, cafe ou ice_cream.",
        "categoria_comida": "Categoria de comida: sopas_caldos, massas_pizza, brasileira, hamburguer, lanches, doces_sobremesas, cafe_padaria, churrasco_carnes, asiatica, internacional, frutos_do_mar, saudavel, sorvete_acai ou geral.",
        "bairro": "Bairro do restaurante, quando disponivel.",
        "lat": "Latitude do restaurante.",
        "lon": "Longitude do restaurante.",
        "aberto_agora": "true = aberto agora; null = horario de funcionamento desconhecido. Restaurantes fechados ja foram removidos.",
        "score_v1": "Score da recomendacao de 0 a 1 (afinidade do clima x confianca da classificacao x fator de horario). Maior = mais indicado.",
    },
)

comentar(
    "workspace.gold.weather_context",
    "Clima de Sao Paulo hora a hora, com tag de clima. Tem previsao (tipo = forecast, proximos 7 dias) e historico (tipo = historical, ultimos 30 dias). Uma linha por hora e tipo.",
    {
        "cidade": "Cidade (sao_paulo).",
        "observed_at": "Data e hora da medicao ou previsao (horario de Sao Paulo).",
        "data_ref": "Data (sem hora).",
        "hora": "Hora do dia, de 0 a 23.",
        "dia_semana_num": "Dia da semana: 1 = domingo, 2 = segunda, ..., 7 = sabado.",
        "tipo": "forecast = previsao; historical = historico observado.",
        "temperature_c": "Temperatura do ar em graus Celsius.",
        "apparent_temperature_c": "Sensacao termica em graus Celsius.",
        "sensacao_c": "Sensacao termica usada na classificacao (se nao houver, usa a temperatura).",
        "precipitation_mm": "Chuva em milimetros na hora.",
        "precipitation_probability": "Probabilidade de chuva em %. Existe so na previsao; no historico e nula.",
        "clima_tag": "chuvoso (chuva >= 0,5 mm ou probabilidade >= 60%), calor_intenso (sensacao >= 30 C), frio (sensacao <= 17 C) ou ameno. Chuva tem prioridade.",
    },
)

comentar(
    "workspace.gold.recommendation_scores",
    "Score base de cada restaurante em cada tag de clima. Tem 4 linhas por restaurante (uma por clima), entao NAO usar para contar restaurantes: use silver.restaurants. Nao considera horario nem distancia.",
    {
        "restaurant_id": "Identificador do restaurante no OpenStreetMap.",
        "name": "Nome do restaurante.",
        "amenity": "Tipo de estabelecimento: restaurant, fast_food, cafe ou ice_cream.",
        "categoria_comida": "Categoria de comida do restaurante.",
        "categoria_fonte": "Como a categoria foi definida: cuisine (tag de cozinha), nome, amenity ou default.",
        "bairro": "Bairro do restaurante, quando disponivel.",
        "lat": "Latitude.",
        "lon": "Longitude.",
        "opening_hours": "Horario de funcionamento no formato do OpenStreetMap (pode ser nulo).",
        "has_opening_hours": "true se o restaurante tem horario cadastrado.",
        "clima_tag": "Tag de clima a que a linha se refere: chuvoso, frio, ameno ou calor_intenso.",
        "afinidade_clima": "Afinidade da categoria com o clima, de 0 a 1.",
        "confianca": "Confianca na classificacao: 1,0 (cuisine), 0,9 (nome) ou 0,8 (amenity/default).",
        "score_base": "afinidade_clima x confianca, de 0 a 1.",
    },
)

comentar(
    "workspace.gold.dim_afinidade_clima_comida",
    "Tabela de regras do projeto: nota de 0 a 1 de afinidade entre cada categoria de comida e cada tag de clima. Sao hipoteses do projeto, nao dado observado.",
    {
        "categoria_comida": "Categoria de comida.",
        "clima_tag": "Tag de clima: chuvoso, frio, ameno ou calor_intenso.",
        "afinidade": "Nota de 0 a 1 (1 = combina muito com o clima).",
    },
)

comentar(
    "workspace.silver.restaurants",
    "Cadastro de restaurantes de Sao Paulo vindos do OpenStreetMap, 1 linha por restaurante. Usar esta tabela para contar restaurantes e para perguntas sobre o cadastro (categorias, bairros, horarios).",
    {
        "restaurant_id": "Identificador unico no OpenStreetMap (tipo + id).",
        "name": "Nome do restaurante.",
        "amenity": "Tipo de estabelecimento: restaurant, fast_food, cafe ou ice_cream.",
        "cuisine_raw": "Tag de cozinha original do OpenStreetMap (pode ser nula).",
        "categoria_comida": "Categoria de comida atribuida pelo projeto.",
        "categoria_fonte": "Como a categoria foi definida: cuisine, nome, amenity ou default.",
        "bairro": "Bairro, quando disponivel.",
        "lat": "Latitude.",
        "lon": "Longitude.",
        "opening_hours": "Horario de funcionamento no formato do OpenStreetMap (pode ser nulo).",
        "has_opening_hours": "true se tem horario cadastrado.",
    },
)

# COMMAND ----------

# Conferencia: mostra o que ficou gravado
for t in ["workspace.gold.recommendations_today", "workspace.gold.weather_context"]:
    display(spark.sql(f"DESCRIBE TABLE {t}"))