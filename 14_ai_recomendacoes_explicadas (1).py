# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 14 - IA: explicações das recomendações com LLM
# MAGIC Usa um LLM (`ai_query`) em batch para escrever, em portugues, **por que** cada restaurante combina com o clima de agora. O resultado vira a tabela `gold.recommendations_explained`.
# MAGIC
# MAGIC **Etapas:**
# MAGIC 1. Seleciona as melhores recomendações de `gold.recommendations_today` (ate 3 por zona, no maximo 200 linhas, para respeitar a cota)
# MAGIC 2. Monta um prompt **fundamentado nos dados da linha**: clima, restaurante, categoria, bairro e se esta aberto. O LLM so pode usar essas informacoes
# MAGIC 3. Chama o LLM com `failOnError => false`, entao uma linha com erro nao derruba o lote. O resultado bruto e gravado **uma vez** numa tabela temporaria, para o LLM nao ser chamado duas vezes
# MAGIC 4. Descobre sozinho os nomes dos campos do retorno (`response`/`result` e `errorMessage`/`errorStatus`), que variam entre versoes do Databricks
# MAGIC 5. **Guarda de qualidade:** respostas vazias, longas demais ou que citem nota, avaliacao, preco ou tempo de entrega (informacoes que nao existem nos dados) sao descartadas
# MAGIC 6. Linhas descartadas ganham uma explicacao por **template** (`fonte_explicacao = template`), entao a tabela nao fica com buraco
# MAGIC 7. Se **nenhuma** resposta do LLM for aproveitada, o notebook falha com as mensagens de erro e **nao sobrescreve** a tabela final (a ultima versao boa fica preservada)
# MAGIC
# MAGIC **Antes de rodar:** execute o `13_ai_teste_llm` e ajuste `LLM_ENDPOINT` abaixo.
# MAGIC
# MAGIC **Depende de:** `gold.recommendations_today` (notebook `11`). Entra no job como task depois do `11`.

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F

GOLD_HOJE = "workspace.gold.recommendations_today"
GOLD_RAW = "workspace.gold.recommendations_llm_raw"       # tabela temporaria, apagada no final
GOLD_EXPL = "workspace.gold.recommendations_explained"

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"   # ajuste com o resultado do notebook 13
TOP_POR_ZONA = 3
MAX_LINHAS = 200
MAX_TOKENS = 80

agora_sp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
print("Endpoint:", LLM_ENDPOINT, "| agora (SP):", agora_sp)

# COMMAND ----------

base = (spark.table(GOLD_HOJE)
        .filter(F.col("rank_zona") <= TOP_POR_ZONA)
        .orderBy("rank_zona", "zona_id")
        .limit(MAX_LINHAS))

clima_txt = (F.when(F.col("clima_tag") == "chuvoso", "chuva")
              .when(F.col("clima_tag") == "frio", "frio")
              .when(F.col("clima_tag") == "calor_intenso", "calor intenso")
              .otherwise("clima ameno"))
categoria_txt = F.regexp_replace("categoria_comida", "_", " ")
aberto_txt = F.when(F.col("aberto_agora") == True, "sim").otherwise("nao informado")
sensacao_txt = F.coalesce(F.round("sensacao_c", 0).cast("int").cast("string"), F.lit("?"))

INSTRUCOES = (
    "Você escreve recomendações curtas para um app de delivery, em português do Brasil. "
    "Use SOMENTE os dados fornecidos. Não invente prato, nota, avaliação, preço, tempo de entrega ou distância. "
    "Responda com UMA frase de até 25 palavras, sem emojis, ligando o clima ao tipo de comida."
)

prompt = F.concat(
    F.lit(INSTRUCOES + "\n\nDados:\n"),
    F.lit("- Clima: "), clima_txt, F.lit(" (sensação térmica de "), sensacao_txt, F.lit(" °C)\n"),
    F.lit("- Restaurante: "), F.col("name"), F.lit("\n"),
    F.lit("- Categoria: "), categoria_txt, F.lit("\n"),
    F.lit("- Bairro: "), F.coalesce(F.col("bairro"), F.lit("não informado")), F.lit("\n"),
    F.lit("- Aberto agora: "), aberto_txt,
)


def chamar_llm(df_base, com_parametros):
    extra = (f", modelParameters => named_struct('max_tokens', {MAX_TOKENS}, 'temperature', 0.2)"
             if com_parametros else "")
    chamada = f"ai_query('{LLM_ENDPOINT}', prompt, failOnError => false{extra})"
    return df_base.withColumn("prompt", prompt).withColumn("_r", F.expr(chamada))


# O struct bruto (_r) e gravado UMA vez; as colunas derivadas vem da tabela, sem chamar o LLM de novo
try:
    chamar_llm(base, True).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_RAW)
    print("LLM chamado com modelParameters (max_tokens e temperature).")
except Exception as e1:
    print("Falhou com modelParameters, tentando sem eles:", str(e1).replace("\n", " ")[:300])
    try:
        chamar_llm(base, False).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_RAW)
        print("LLM chamado sem modelParameters.")
    except Exception as e2:
        raise RuntimeError(
            "Nao foi possivel chamar o LLM. Rode o notebook 13 e confira LLM_ENDPOINT. Erro: " + str(e2)[:400]
        )

# COMMAND ----------

def escolher_campos(nomes):
    """Descobre os nomes dos campos do retorno do ai_query com failOnError => false."""
    c_resp = next((c for c in ("response", "result") if c in nomes), None)
    c_erro = next((c for c in ("errorMessage", "errorStatus") if c in nomes), None)
    return c_resp, c_erro


raw = spark.table(GOLD_RAW)
campos = [f.name for f in raw.schema["_r"].dataType.fields]
c_resp, c_erro = escolher_campos(campos)
print("Campos do retorno:", campos, "| resposta =", c_resp, "| erro =", c_erro)
if c_resp is None:
    raise RuntimeError(f"Nao encontrei o campo de resposta no retorno do ai_query: {campos}")

raw = (raw
       .withColumn("resposta_llm", F.col("_r").getField(c_resp).cast("string"))
       .withColumn("erro_llm", F.col("_r").getField(c_erro).cast("string") if c_erro else F.lit(None).cast("string"))
       .drop("_r"))

resp = F.regexp_replace(F.trim(F.col("resposta_llm")), r"\s+", " ")
invalida = (
    resp.isNull()
    | (F.length(resp) == 0)
    | (F.length(resp) > 300)
    | resp.rlike(r"(?i)(\b(nota|notas|avalia\w*|estrelas?|reais|minutos|entrega)\b|R\$)")
)

template = F.concat(
    F.lit("Com "), clima_txt, F.lit(", "), F.col("name"), F.lit(" ("), categoria_txt, F.lit(") é uma boa pedida"),
    F.when(F.col("bairro").isNotNull(), F.concat(F.lit(" em "), F.col("bairro"))).otherwise(F.lit("")),
    F.lit("."),
)

final = (raw
         .withColumn("explicacao", F.when(invalida, template).otherwise(resp))
         .withColumn("fonte_explicacao", F.when(invalida, "template").otherwise("llm"))
         .withColumn("llm_endpoint", F.lit(LLM_ENDPOINT))
         .withColumn("gerado_em", F.lit(agora_sp).cast("timestamp"))
         .drop("prompt", "resposta_llm"))

total = final.count()
n_llm = final.filter("fonte_explicacao = 'llm'").count()
n_erro = final.filter(F.col("erro_llm").isNotNull()).count()
print(f"Linhas: {total} | via LLM: {n_llm} | com erro do LLM: {n_erro}")

if n_llm == 0:
    print("Exemplos de erro devolvidos pelo LLM:")
    display(final.filter(F.col("erro_llm").isNotNull()).select("erro_llm").distinct().limit(5))
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_RAW}")
    raise RuntimeError("Nenhuma resposta do LLM foi aproveitada. A tabela final NAO foi sobrescrita. Veja os erros acima.")

final.drop("erro_llm").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_EXPL)
spark.sql(f"DROP TABLE IF EXISTS {GOLD_RAW}")

# COMMAND ----------

# Descricoes para o Genie (reaplicadas a cada execucao, pois o overwrite pode apagar os comentarios)
def esc(t):
    return t.replace("'", "\\'")

spark.sql(f"COMMENT ON TABLE {GOLD_EXPL} IS '{esc('Recomendacoes da hora atual com uma explicacao curta em portugues de por que o restaurante combina com o clima. Contem so uma amostra das melhores recomendacoes (ate 3 por zona, limitada em linhas), nao o cadastro. A explicacao vem de um LLM ou, se a resposta do LLM for invalida, de um template.')}'")
existentes = {c.lower() for c in spark.table(GOLD_EXPL).columns}
for col, desc in {
    "explicacao": "Frase curta em portugues ligando o clima ao restaurante.",
    "fonte_explicacao": "llm = texto gerado pelo modelo; template = texto padrao usado quando a resposta do modelo foi invalida.",
    "llm_endpoint": "Endpoint do modelo usado na geracao.",
    "gerado_em": "Data e hora da geracao (horario de Sao Paulo).",
    "score_v1": "Score da recomendacao de 0 a 1. Maior = mais indicado.",
    "clima_tag": "Tag do clima: chuvoso, frio, ameno ou calor_intenso.",
}.items():
    if col in existentes:
        spark.sql(f"ALTER TABLE {GOLD_EXPL} ALTER COLUMN {col} COMMENT '{esc(desc)}'")

# COMMAND ----------

out = spark.table(GOLD_EXPL)
print("Linhas gravadas:", out.count())
display(out.groupBy("fonte_explicacao").count())
display(out.select("clima_tag", "name", "categoria_comida", "bairro", "explicacao", "fonte_explicacao").limit(15))