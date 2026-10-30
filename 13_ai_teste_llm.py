# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 13 - IA: teste de acesso a LLM no workspace
# MAGIC Descobre quais modelos de linguagem o seu workspace consegue chamar com `ai_query`, antes de construir qualquer coisa em cima deles.
# MAGIC
# MAGIC **O que faz:**
# MAGIC 1. Lista os endpoints de serving visiveis no workspace
# MAGIC 2. Testa cada modelo com um prompt minimo (`Responda apenas com a palavra: ok`), sem esconder erros: a mensagem real de cada falha aparece na tabela
# MAGIC 3. Mostra o status de cada um e sugere qual usar
# MAGIC
# MAGIC **Como usar o resultado:** copie o nome do primeiro modelo com status `ok` para a variavel `LLM_ENDPOINT` do notebook `14`.
# MAGIC
# MAGIC Usa no maximo 12 chamadas pequenas, entao consome pouca cota. Nao grava nenhuma tabela. Nao faz parte do job diario.

# COMMAND ----------

import re

# Usados apenas se a listagem de endpoints nao funcionar
CANDIDATOS = [
    "databricks-meta-llama-3-3-70b-instruct",
    "databricks-meta-llama-3-1-8b-instruct",
    "databricks-gpt-oss-20b",
    "databricks-gpt-oss-120b",
    "databricks-llama-4-maverick",
]

try:
    from databricks.sdk import WorkspaceClient
    listados = sorted(e.name for e in WorkspaceClient().serving_endpoints.list())
except Exception as e:
    listados = []
    print("Nao consegui listar os endpoints:", str(e)[:300])

print("Endpoints visiveis no Serving:", len(listados))
for n in listados:
    print("  -", n)

# COMMAND ----------

ignorar = re.compile(r"(embed|bge|gte|rerank)", re.I)
if listados:
    lista = [n for n in listados if not ignorar.search(n)][:12]
else:
    lista = CANDIDATOS
print("Vou testar:", lista)

resultados = []
for nome in lista:
    try:
        # Sem failOnError: se der problema, o erro real aparece (permissao, nome errado, modelo sem suporte)
        resposta = spark.sql(
            f"SELECT ai_query('{nome}', 'Responda apenas com a palavra: ok') AS r"
        ).first()["r"]
        if resposta is not None and str(resposta).strip():
            resultados.append((nome, "ok", str(resposta).strip()[:60]))
        else:
            resultados.append((nome, "sem resposta", ""))
    except Exception as e:
        resultados.append((nome, "erro", str(e).replace("\n", " ")[:300]))

display(spark.createDataFrame(resultados, "endpoint string, status string, detalhe string"))

# COMMAND ----------

ok = [r[0] for r in resultados if r[1] == "ok"]
if ok:
    preferidos = [n for n in ok if "llama-3-3-70b" in n] or [n for n in ok if "llama" in n] or ok
    print("Acesso a LLM confirmado. Modelos que responderam:", ok)
    print("Sugestao para o notebook 14:  LLM_ENDPOINT =", repr(preferidos[0]))
else:
    print("Nenhum modelo respondeu.")
    print("Copie as mensagens da coluna detalhe: elas dizem se o problema e permissao, regiao, nome do endpoint ou modelo sem suporte.")