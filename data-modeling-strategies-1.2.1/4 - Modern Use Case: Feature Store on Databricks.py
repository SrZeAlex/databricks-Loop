# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Caso de uso moderno: Repositório de recursos na Databricks
# MAGIC Nesta demonstração, abordaremos o processo de uso do repositório de recursos do Databricks para criar, treinar e implantar modelos de machine learning usando recursos armazenados em um repositório centralizado. Abordaremos a instalação das bibliotecas necessárias, a criação e atualização de recursos, o treinamento de um modelo com esses recursos e a realização de inferência em lote para fazer previsões. Ao final desta demonstração, você ganhará experiência prática ao aproveitar o poder do repositório de recursos do Databricks para simplificar os fluxos de trabalho de machine learning e garantir consistência no gerenciamento de recursos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objetivos de Aprendizagem 
# MAGIC Ao final desta demonstração, você será capaz de:
# MAGIC - Aplicar o processo de instalação e configuração das bibliotecas da Repositório de recursos do Databricks.
# MAGIC - Criar e gerenciar tabelas de recursos no Databricks para armazenar e atualizar recursos.
# MAGIC - Treinar um modelo de machine learning usando recursos recuperadas da Repositório de recursos.
# MAGIC - Realizar inferência em lote para gerar previsões com base em dados de recursos.
# MAGIC - Avaliar a eficácia das atualizações periódicas de recursos para manter a precisão do modelo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨OBRIGATÓRIO - SELECT CLASSIC COMPUTE
# MAGIC Antes de executar células neste notebook, selecione seu cluster de compute clássico no laboratório. Lembre-se de que **Serverless** está habilitado por default.
# MAGIC
# MAGIC Siga estas etapas para selecionar o cluster de compute clássico:
# MAGIC * Navegue até o canto superior direito deste notebook e clique no menu dropdown para selecionar seu cluster. Por default, o notebook usará **Serverless**. <br>
# MAGIC
# MAGIC ##### **📌**Se o cluster estiver disponível, selecione-o e continue para a próxima célula. Se o cluster não for mostrado:
# MAGIC   - Na lista dropdown, selecione **More**.
# MAGIC   - No pop-up **Attach to an existing compute resource**, selecione a primeira lista dropdown. Você verá um nome de cluster exclusivo nessa lista dropdown. Selecione esse cluster.
# MAGIC
# MAGIC **NOTA:** Se o cluster tiver sido encerrado, talvez seja necessário reiniciá-lo para selecioná-lo. Para fazer isso:
# MAGIC 1. Clique com o botão direito do rato em **Compute** no painel de navegação esquerdo e selecione *Open in new tab*.
# MAGIC 2. Localize o ícone de triângulo à direita do nome do cluster de compute e clique nele.
# MAGIC 3. Aguarde alguns minutos para que o cluster seja iniciado.
# MAGIC 4. Quando o cluster estiver em execução, conclua as etapas acima para selecioná-lo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requisitos
# MAGIC
# MAGIC Analise os seguintes requisitos antes de iniciar a demonstração:
# MAGIC
# MAGIC * Para executar este notebook, você precisa usar um dos seguintes Databricks runtime(s): **15.4.x-scala2.12**
# MAGIC * Como alternativa, em um cluster de tempo de execução não-ML, instale manualmente as bibliotecas necessárias de maneira semelhante. (Para esta demonstração, temos um cluster não-ML)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarefas do Notebook:
# MAGIC Este Notebook orienta o processo de:
# MAGIC 1. **Installing Libraries** para repositório de recursos do Databricks.
# MAGIC 2. **Building Features** e salvando-os no repositório de recursos.
# MAGIC 3. **Training a Model** usando esses recursos.
# MAGIC 4. **Performing Batch Inference** com os mesmos recursos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Instalar bibliotecas necessárias
# MAGIC
# MAGIC Instalaremos a biblioteca Engenharia de recursos da Databricks (também conhecida como repositório de recursos) para que possamos criar definições de tabela, carregar conjuntos de treinamento e publicar recursos.

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

# MAGIC %md
# MAGIC Uma vez que a biblioteca é instalada, reiniciamos o kernel Python para que ele esteja totalmente disponível.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuração do ambiente
# MAGIC
# MAGIC - Vamos trabalhar em um catálogo e esquema a que você tem acesso.
# MAGIC - Ajuste os nomes do catálogo/esquema para corresponder ao seu ambiente ou convenções de nomenclatura.

# COMMAND ----------

import re
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()


user_id = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]
catalog_name = re.sub(r'[^a-zA-Z0-9]', '_', user_id)
silver_schema = "silver"
gold_schema = "gold"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE {silver_schema}")

print("Catalog and schemas set for feature development.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criando ou atualizando recursos
# MAGIC
# MAGIC **Objetivo**: Crie recursos no nível do cliente a partir de tabelas refinadas de exemplo (`refined_orders` e `refined_customer`):
# MAGIC - `total_orders`: Número total de pedidos por cliente
# MAGIC - `avg_order_value`: Preço médio por pedido
# MAGIC - `total_spending`: Valor total gasto no geral
# MAGIC - `market_segment`: Uma coluna categórica da dimensão Cliente

# COMMAND ----------

# Gere recursos básicos no nível do cliente agregando dados do pedido e juntando com as informações do cliente
from pyspark.sql.functions import col, count, avg, sum, current_timestamp

orders_df = spark.sql("SELECT * FROM refined_orders")
customers_df = spark.sql("SELECT * FROM refined_customer")

base_features_df = (
    orders_df.groupBy("customer_id")
    .agg(
        count("*").alias("total_orders"),
        avg("total_price").alias("avg_order_value"),
        sum("total_price").alias("total_spending")
    )
    .join(
        customers_df.select("customer_id", "market_segment"),
        on="customer_id",
        how="inner"
    )
    .withColumn("feature_update_ts", current_timestamp())
)

display(base_features_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Registrar ou Mesclar-Atualizar Tabela de Recursos
# MAGIC
# MAGIC Criaremos uma tabela de recursos chamada `customer_features` no esquema `gold`. Se já existir, mesclaremos os novos dados.

# COMMAND ----------

# Criar ou mesclar-atualizar a tabela 'customer_features' com recursos agregados no nível do cliente
feature_table_name = f"{catalog_name}.{gold_schema}.customer_features"

try:
    fs.create_table(
        name=feature_table_name,
        primary_keys=["customer_id"],
        schema=base_features_df.schema,
        description="Customer-level features derived from refined tables."
    )
    print(f"Feature table '{feature_table_name}' created.")
except Exception as e:
    print(f"Feature table might already exist: {e}")

fs.write_table(
    name=feature_table_name,
    df=base_features_df,
    mode="merge"  
)

print(f"Feature table '{feature_table_name}' updated with new features.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Agendando atualizações
# MAGIC
# MAGIC Este notebook (ou um subconjunto dele) pode ser agendado como um Databricks Job para atualizar periodicamente os recursos com dados atualizados. Isso garante que seus recursos permaneçam atualizados para tarefas de ML downstream.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Treinamento de modelos com o Repositório de recursos
# MAGIC
# MAGIC Vamos fazer o seguinte:
# MAGIC 1. Crie um rótulo simples: Os clientes que gastaram mais do que um determinado limite são considerados "grandes gastadores."
# MAGIC 2. Procure os mesmos recursos via `FeatureLookup` para treinamento.
# MAGIC 3. Treinar um modelo básico de Regressão Logística.

# COMMAND ----------

# Adicionar rótulo binário aos clientes com base em se seus gastos totais excedem um limite
from pyspark.sql.functions import when

threshold = 20000.0

labeled_df = (
    base_features_df
    .withColumn("label", when(col("total_spending") > threshold, 1).otherwise(0))
    .select("customer_id", "total_orders", "avg_order_value", "total_spending", "market_segment", "label")
)

display(labeled_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Criar um conjunto de treinamento com FeatureLookups
# MAGIC
# MAGIC Use uma pesquisa para recuperar colunas da tabela de recursos e associá-las ao nosso dataset rotulado.

# COMMAND ----------

# Crie um conjunto de treinamento pesquisando recursos do repositório de recursos e juntando-os com dados rotulados
from databricks.feature_store import FeatureLookup

feature_lookup = FeatureLookup(
    table_name=feature_table_name,
    feature_names=[
        "total_orders",
        "avg_order_value",
        "total_spending",
        "market_segment",
    ],
    lookup_key="customer_id",
)
labeled_df_clean = labeled_df.drop("total_orders", "avg_order_value", "total_spending", "market_segment")

training_set = fs.create_training_set(
    df=labeled_df_clean,
    feature_lookups=[feature_lookup],
    label="label",
    exclude_columns=["feature_update_ts"]
)

training_df = training_set.load_df()
display(training_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Treinar um Modelo de Regressão Logística Simples
# MAGIC
# MAGIC Vamos converter a coluna `market_segment` categórica em forma numérica, vetorizar todos os recursos e treinar um classificador binário.

# COMMAND ----------

# Construir e treinar um modelo de regressão logística usando um pipeline com etapas de transformação de recursos
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

# Indexador de string para a coluna categórica
indexer = StringIndexer(inputCol="market_segment", outputCol="market_segment_idx", handleInvalid="keep")
encoder = OneHotEncoder(inputCols=["market_segment_idx"], outputCols=["market_segment_vec"])

assembler = VectorAssembler(
    inputCols=["total_orders", "avg_order_value", "total_spending", "market_segment_vec"],
    outputCol="features"
)

lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])
model = pipeline.fit(training_df)

print("Model training complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 (opcional) Registrar modelo no MLflow
# MAGIC
# MAGIC Se você quiser acompanhar versões, métricas e implantar o modelo facilmente, registre-o no MLflow. Por uma questão de brevidade, vamos omitir esses detalhes aqui.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Inferência em lote usando o Repositório de recursos
# MAGIC
# MAGIC **Cenário**: Temos alguns IDs de clientes novos ou existentes e queremos prever quais podem ser os que gastam muito. Vamos fazer:
# MAGIC 1. Demonstrar a criação de um DataFrame de IDs de clientes.
# MAGIC 2. Procure os mesmos recursos via `FeatureLookup`.
# MAGIC 3. Gere previsões com nosso pipeline treinado.

# COMMAND ----------

# Selecione 5 clientes de amostra e adicione uma coluna de sinalizador para demonstração de inferência em lote
from pyspark.sql.functions import lit

sample_customers = (
    training_df.select("customer_id")
    .limit(5)
    .withColumn("batch_inference_example", lit(True))
)

display(sample_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Recuperar recursos e pontuar
# MAGIC
# MAGIC A fusão dos clientes preparados com nossa tabela de recursos resulta em um DataFrame final que pode ser executado através do modelo.

# COMMAND ----------

# Realizar inferência em lotes recuperando recursos para clientes de amostra e aplicando o modelo treinado
inference_lookup = FeatureLookup(
    table_name=feature_table_name,
    feature_names=[
        "total_orders",
        "avg_order_value",
        "total_spending",
        "market_segment"
    ],
    lookup_key="customer_id"
)

inference_set = fs.create_training_set(
    df=sample_customers,
    feature_lookups=[inference_lookup],
    label=None 
)

inference_df = inference_set.load_df()
predictions = model.transform(inference_df)
display(predictions.select("customer_id", "prediction", "probability"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Principais conceitos abordados:
# MAGIC - **Periodic Feature Updates** mantêm os recursos de entrada do seu modelo atualizados.
# MAGIC - **Single Source of Truth** para recursos via repositório de recursos leva a treinamento consistente e inferência.
# MAGIC - **Batch or Real-Time Inference** pode ser implementada usando as mesmas definições de recursos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC Nesta demonstração, implementamos com sucesso um pipeline completo usando o repositório de recursos da Databricks para gerenciar, treinar e prever com recursos de machine learning. Criamos e atualizamos tabelas de recursos, treinamos um modelo usando esses recursos e realizamos inferências em lote para fazer previsões. Seguindo esse fluxo de trabalho, você pode garantir consistência e eficiência no gerenciamento de recursos, permitindo que você crie e implante modelos de machine learning mais confiáveis. Etapas futuras podem incluir a implementação de inferência em tempo real ou a automatização de atualizações de recursos para fluxos de trabalho de dados mais dinâmicos.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
