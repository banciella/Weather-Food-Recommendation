Weather-Food-Recommendation
Pipeline de engenharia de dados no Databricks que cruza clima e restaurantes para recomendar comida conforme o tempo (por exemplo, sopa e massas em dia de chuva, açaí e sorvete em dia de calor intenso).

Projeto pessoal de portfólio, feito 100% com ferramentas gratuitas e open source.

Problema de negócio
Apps de delivery recomendam comida de acordo com clima, horário e local. Este projeto implementa esse conceito de ponta a ponta: ingestão de dados, limpeza, qualidade, orquestração e consumo por linguagem natural.

Fontes de dados
Fonte	Uso	Observação
Open-Meteo	Previsão e histórico de clima	Gratuita, sem chave de API
OpenStreetMap (Overpass API)	Restaurantes, lanchonetes, cafés e sorveterias	Gratuita, sem chave de API
Arquitetura (Medallion)
Open-Meteo e Overpass -> Bronze (JSON bruto) -> Silver (Delta limpo + Data Quality) -> Gold (features e scores) -> Genie Agent e Dashboard

Camada	Conteúdo
Bronze	JSON bruto das APIs, em Volume, particionado por ingestion_date
Silver	Tabelas Delta limpas, validadas e gravadas com MERGE (idempotente)
Gold	Features de clima e score de recomendação
Stack
Databricks Free Edition, PySpark, Delta Lake, Unity Catalog, Databricks Jobs, MLflow, Genie, GitHub.

Status do projeto
Integração Databricks e GitHub (Git folders)
Ambiente: schemas bronze, silver e gold e Volume de arquivos brutos
Bronze: clima de São Paulo (previsão e histórico)
Bronze: restaurantes de São Paulo
Silver: clima
Silver: restaurantes
Gold: tags de clima e score de recomendação (V1, por regras)
Job diário de orquestração
Dashboard e Genie Agent
V2 com machine learning (usando pedidos simulados)
Limitações conhecidas dos dados
A primeira ingestão de restaurantes trouxe 5.590 estabelecimentos (3.097 restaurantes, 1.758 fast foods, 496 cafés e 239 sorveterias).
Apenas ~57% têm a tag cuisine preenchida. Nos demais, o tipo de comida será inferido pela categoria e pelo nome.
Apenas ~14% têm opening_hours. Por isso, "aberto agora" será tratado como informação desconhecida, e não como filtro eliminatório.
Não há nota nem popularidade na fonte aberta. O score será construído por regras (V1) e depois comparado com um modelo treinado em pedidos simulados (V2). Dados simulados serão sempre identificados como tal.
Como reproduzir
Clone o repositório como Git folder no Databricks.
Rode os notebooks em ordem numérica (03_setup_ambiente, 04_bronze_clima, 05_bronze_restaurantes, ...).
