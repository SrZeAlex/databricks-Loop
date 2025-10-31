# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Vault 2.0
# MAGIC Nesta demonstração, você mergulhará nos conceitos do Data Vault 2.0 e aprenderá como implementar seu modelo no Databricks. O Data Vault 2.0 fornece uma abordagem escalável e flexível para data warehousing, com foco na criação de Hubs, Links e Satélites para armazenamento e rastreamento eficientes dos principais dados de negócios. Você aprenderá a criar essas estruturas usando keys hash, configurar o pipeline ETL para carregar os dados e criar exibições de negócios para query do usuário final. Ao final desta demonstração, você estará equipado com as habilidades para implementar um modelo do Data Vault e otimizá-lo para desempenho e escalabilidade.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final desta demonstração, você será capaz de:
# MAGIC * Aplicar conceitos do Data Vault 2.0 para criar Hubs, Links e Satélites no Databricks.
# MAGIC * Projetar um modelo de dados escalável e flexível usando keys hash para otimização de desempenho.
# MAGIC * Implementar um pipeline ETL para carregar dados em componentes do Data Vault.
# MAGIC * Desenvolver views de negócios que simplifiquem a query e análise para usuários finais.
# MAGIC * Verificar a integridade e precisão do modelo do Data Vault com queries e verificações de amostra.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨OBRIGATÓRIO - SELECT CLASSIC COMPUTE
# MAGIC
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
# MAGIC 2. Localize o ícone de triângulo à direita do nome do cluster de computação e clique nele.
# MAGIC 3. Aguarde alguns minutos para que o cluster seja iniciado.
# MAGIC 4. Quando o cluster estiver em execução, conclua as etapas acima para selecioná-lo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requisitos
# MAGIC
# MAGIC Analise os seguintes requisitos antes de iniciar a demonstração:
# MAGIC
# MAGIC * Para executar este notebook, você precisa usar um dos seguintes Databricks runtime(s): **15.4.x-scala2.12**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Principais Conceitos
# MAGIC
# MAGIC 1. **Hub**: Contém uma lista exclusiva de keys de negócios e metadados relacionados
# MAGIC 2. **Link**: Conecta hubs para representar relacionamentos
# MAGIC 3. **Satélite**: Armazena os atributos descritivos de um hub ou link, permitindo o rastreamento histórico
# MAGIC
# MAGIC O Data Vault 2.0 enfatiza o uso de *hash keys* para desempenho e escalabilidade.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurando o ambiente
# MAGIC
# MAGIC Primeiro, vamos configurar nosso ambiente Databricks:

# COMMAND ----------

# DBTITLE 1,USE CATALOG, USE esquema prata
import re

# Obter o usuário atual e extrair o nome do catálogo dividindo o e-mail em '@' e tomando a primeira parte
user_id = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]

# Substitua todos os caracteres especiais no `user_id` por um sublinhado '_' para criar o nome do catálogo
catalog_name = re.sub(r'[^a-zA-Z0-9]', '_', user_id)

# Defina o nome do esquema a ser usado
silver_schema = "silver"

# COMMAND ----------

# Criar um widget para capturar o nome do catálogo e todos os nomes de esquema
dbutils.widgets.text("catalog_name", catalog_name)
dbutils.widgets.text("silver_schema", silver_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o catálogo atual para o nome do catálogo extraído
# MAGIC USE CATALOG IDENTIFIER(:catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o esquema atual para o nome do esquema definido
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criando tabelas do Data Vault 2.0
# MAGIC
# MAGIC ### 2.1 Hubs
# MAGIC
# MAGIC Os hubs armazenam as principais entidades de negócios. Cada hub contém uma lista exclusiva de keys de negócios e metadados relacionados.

# COMMAND ----------

# DBTITLE 1,CREATE TABLE H_Customer
# MAGIC %sql
# MAGIC -- Hub para o Customer
# MAGIC CREATE TABLE IF NOT EXISTS H_Customer
# MAGIC (
# MAGIC   customer_hk STRING NOT NULL COMMENT 'MD5(customer_id)',
# MAGIC   customer_id INT NOT NULL,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_h_customer PRIMARY KEY (customer_hk)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,CREATE TABLE H_Order
# MAGIC %sql
# MAGIC -- Hub para Pedidos
# MAGIC CREATE TABLE IF NOT EXISTS H_Order
# MAGIC (
# MAGIC   order_hk STRING NOT NULL COMMENT 'MD5(order_id)',
# MAGIC   order_id INT NOT NULL,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_h_order PRIMARY KEY (order_hk)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Ligações
# MAGIC
# MAGIC Os links representam relacionamentos entre hubs. Eles conectam diferentes entidades empresariais.

# COMMAND ----------

# DBTITLE 1,CREATE TABLE L_Customer_Order
# MAGIC %sql
# MAGIC -- Cria uma tabela de links para mapear clientes para seus pedidos com uma hash key primária
# MAGIC CREATE TABLE IF NOT EXISTS L_Customer_Order
# MAGIC (
# MAGIC   customer_order_hk STRING NOT NULL COMMENT 'MD5(customer_hk||order_hk)',
# MAGIC   customer_hk STRING NOT NULL,
# MAGIC   order_hk STRING NOT NULL,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_l_customer_order PRIMARY KEY (customer_order_hk)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Satélites
# MAGIC
# MAGIC Os satélites armazenam atributos descritivos e rastreiam as alterações ao longo do tempo. Eles são vinculados a hubs ou links por meio de hash keys.

# COMMAND ----------

# DBTITLE 1,CREATE TABLE S_Customer
# MAGIC %sql
# MAGIC -- Satélite para informações descritivas do cliente
# MAGIC CREATE TABLE IF NOT EXISTS S_Customer
# MAGIC (
# MAGIC   customer_hk STRING NOT NULL,
# MAGIC   hash_diff STRING NOT NULL COMMENT 'MD5 of all descriptive columns',
# MAGIC   name STRING,
# MAGIC   address STRING,
# MAGIC   nation_key INT,
# MAGIC   phone STRING,
# MAGIC   acct_bal DECIMAL(12,2),
# MAGIC   market_segment STRING,
# MAGIC   comment STRING,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_s_customer PRIMARY KEY (customer_hk, load_timestamp)
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Table S_Order
# MAGIC %sql
# MAGIC -- Satélite para informações descritivas do pedido
# MAGIC CREATE TABLE IF NOT EXISTS S_Order
# MAGIC (
# MAGIC   order_hk STRING NOT NULL,
# MAGIC   hash_diff STRING NOT NULL COMMENT 'MD5 of all descriptive columns',
# MAGIC   order_status STRING,
# MAGIC   total_price DECIMAL(12,2),
# MAGIC   order_date DATE,
# MAGIC   order_priority STRING,
# MAGIC   clerk STRING,
# MAGIC   ship_priority INT,
# MAGIC   comment STRING,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_s_order PRIMARY KEY (order_hk, load_timestamp)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Defina o catálogo e o esquema default no spark sql.

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {silver_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Processo ETL
# MAGIC
# MAGIC Agora que temos nossa estrutura do Data Vault implementada, vamos carregar alguns dados. Usaremos um processo simplificado de ETL:
# MAGIC
# MAGIC 1. Carregar Hubs  
# MAGIC 2. Carregar links  
# MAGIC 3. Carregar Satélites

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Funções auxiliares

# COMMAND ----------

# DBTITLE 1,Hashing definições de funções auxiliares
# Gere hash keys e colunas de hash diff para entidades do Data Vault, incluindo cliente, pedido e seus relacionamentos, usando MD5.
from pyspark.sql.functions import md5, concat_ws, col

# Defina uma função personalizada para gerar uma hash key para customer_id
def generate_customer_hash_keys(df):
    return df.withColumn(
        "customer_hk", 
        md5(col("customer_id").cast("string"))
    )

# Defina uma função personalizada para gerar uma hash key para order_id
def generate_order_hash_keys(df):
    return df.withColumn(
        "order_hk", 
        md5(col("order_id").cast("string"))
    )

# Definir uma função personalizada para gerar uma hash key composta para cliente e pedido
def generate_customer_order_hash_key(df):
    return df.withColumn(
        "customer_order_hk",
        md5(concat_ws("||", col("customer_hk"), col("order_hk")))
    )

# Definir uma função personalizada para gerar uma diferença de hash para detecção de alterações
def generate_hash_diff(df, columns):
    return df.withColumn("hash_diff", md5(concat_ws("||", *[col(c) for c in columns])))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Carregando tabela refinada (alteração de nome de coluna, conversão de tipo)

# COMMAND ----------

# DBTITLE 1,CREATE TABLE silver.refined_customer
# MAGIC %sql
# MAGIC -- Cria uma tabela refinada de dimensão do cliente na camada prata
# MAGIC CREATE TABLE IF NOT EXISTS silver.refined_customer (
# MAGIC   customer_id INT NOT NULL,
# MAGIC   name STRING,
# MAGIC   address STRING,
# MAGIC   nation_key INT,
# MAGIC   phone STRING,
# MAGIC   acct_bal DECIMAL(12, 2),
# MAGIC   market_segment STRING,
# MAGIC   comment STRING
# MAGIC );

# COMMAND ----------

# DBTITLE 1,CREATE TABLE silver.refined_orders
# MAGIC %sql
# MAGIC -- Cria uma tabela de fatos de pedidos refinados na camada prata
# MAGIC CREATE TABLE IF NOT EXISTS silver.refined_orders (
# MAGIC   order_id INT NOT NULL,
# MAGIC   customer_id INT NOT NULL,
# MAGIC   order_status STRING,
# MAGIC   total_price DECIMAL(12, 2),
# MAGIC   order_date DATE,
# MAGIC   order_priority STRING,
# MAGIC   clerk STRING,
# MAGIC   ship_priority INT,
# MAGIC   comment STRING
# MAGIC );

# COMMAND ----------

# DBTITLE 1,definições de função de carga ETL
# Definindo funções de carga ETL
from pyspark.sql.functions import col, to_date, current_timestamp

def etl_refined_customer():
    bronze_customer = spark.table("bronze.customer")
    refined_customer = bronze_customer.select(
        col("c_custkey").cast("int").alias("customer_id"),
        col("c_name").alias("name"),
        col("c_address").alias("address"),
        col("c_nationkey").cast("int").alias("nation_key"),
        col("c_phone").alias("phone"),
        col("c_acctbal").cast("decimal(12,2)").alias("acct_bal"),
        col("c_mktsegment").alias("market_segment"),
        col("c_comment").alias("comment")
    )
    refined_customer.write.mode("overwrite").saveAsTable("silver.refined_customer")

def etl_refined_orders():
    bronze_orders = spark.table("bronze.orders")
    refined_orders = bronze_orders.select(
        col("o_orderkey").cast("int").alias("order_id"),
        col("o_custkey").cast("int").alias("customer_id"),
        col("o_orderstatus").alias("order_status"),
        col("o_totalprice").cast("decimal(12,2)").alias("total_price"),
        to_date(col("o_orderdate"), "yyyy-MM-dd").alias("order_date"),
        col("o_orderpriority").alias("order_priority"),
        col("o_clerk").alias("clerk"),
        col("o_shippriority").cast("int").alias("ship_priority"),
        col("o_comment").alias("comment")
    )
    refined_orders.write.mode("overwrite").saveAsTable("silver.refined_orders")

# COMMAND ----------

# DBTITLE 1,etl_refined_customer(), etl_refined_orders()
# Executar carga ETL
etl_refined_customer()
etl_refined_orders()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Carregando o hub do cliente

# COMMAND ----------

# DBTITLE 1,MERGE INTO H_Customer USING customer_hub_stage
# Carregue e mescle novos registros de clientes na tabela do hub H_Customer com hash key e metadados
from pyspark.sql.functions import current_timestamp, lit
silver_customer_df = spark.sql("SELECT * FROM silver.refined_customer")

customer_hub_data = (
    generate_customer_hash_keys(silver_customer_df)
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("TPC-H"))
)

customer_hub_data.createOrReplaceTempView("customer_hub_stage")

spark.sql("""
MERGE INTO H_Customer AS target
USING customer_hub_stage AS source
ON target.customer_hk = source.customer_hk
WHEN NOT MATCHED THEN
  INSERT (customer_hk, customer_id, load_timestamp, record_source)
  VALUES (source.customer_hk, source.customer_id, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# DBTITLE 1,SELECT FROM H_Customer
# MAGIC %sql
# MAGIC -- Visualizar os primeiros 10 registros da tabela H_Customer Hub
# MAGIC SELECT * FROM H_Customer LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Carregando o satélite do cliente

# COMMAND ----------

# DBTITLE 1,MERGE INTO S_Customer USING customer_sat_stage
# Mesclar novos registros descritivos de clientes na tabela satélite S_Customer com hash diff e metadados
customer_sat_columns = ["name", "address", "nation_key", "phone", "acct_bal", "market_segment", "comment"]

customer_sat_data = generate_hash_diff(customer_hub_data, customer_sat_columns)
customer_sat_data.createOrReplaceTempView("customer_sat_stage")

spark.sql(f"""
MERGE INTO S_Customer AS target
USING customer_sat_stage AS source
ON target.customer_hk = source.customer_hk AND target.load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN
  INSERT (customer_hk, hash_diff, {', '.join(customer_sat_columns)}, load_timestamp, record_source)
  VALUES (source.customer_hk, source.hash_diff, {', '.join([f'source.{col}' for col in customer_sat_columns])}, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Carregando o Hub de Pedidos e o Satélite

# COMMAND ----------

# DBTITLE 1,MERGE INTO H_Order USING order_hub_stage, MERGE INTO S_Order USING order_sat_stage
# Carregue e mescle novos registros de pedidos em tabelas de hub e satélite com hash keys, hash diff e metadados
silver_orders_df = spark.sql("SELECT * FROM silver.refined_orders")

order_hub_data = (
    generate_order_hash_keys(silver_orders_df)
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("TPC-H"))
)

order_hub_data.createOrReplaceTempView("order_hub_stage")

spark.sql("""
MERGE INTO H_Order AS target
USING order_hub_stage AS source
ON target.order_hk = source.order_hk
WHEN NOT MATCHED THEN
  INSERT (order_hk, order_id, load_timestamp, record_source)
  VALUES (source.order_hk, source.order_id, source.load_timestamp, source.record_source)
""")

order_sat_columns = ["order_status", "total_price", "order_date", "order_priority", "clerk", "ship_priority", "comment"]

order_sat_data = generate_hash_diff(order_hub_data, order_sat_columns)
order_sat_data.createOrReplaceTempView("order_sat_stage")

spark.sql(f"""
MERGE INTO S_Order AS target
USING order_sat_stage AS source
ON target.order_hk = source.order_hk AND target.load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN
  INSERT (order_hk, hash_diff, {', '.join(order_sat_columns)}, load_timestamp, record_source)
  VALUES (source.order_hk, source.hash_diff, {', '.join([f'source.{col}' for col in order_sat_columns])}, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 Carregando o link pedido do cliente

# COMMAND ----------

# DBTITLE 1,MERGE INTO L_Customer_Order USING link_stage
# Criar e mesclar registros de link pedido do cliente com hash key e metadados combinados na tabela de links
from pyspark.sql.functions import concat_ws

link_data = (
    # Junte silver_orders_df a H_Customer e H_Order usando as keys naturais
    silver_orders_df.alias("orders")
    .join(spark.table("H_Customer").alias("hc"), on=[col("orders.customer_id") == col("hc.customer_id")], how="inner")
    .join(spark.table("H_Order").alias("ho"), on=[col("orders.order_id") == col("ho.order_id")], how="inner")
    # Selecione as hash keys já estabelecidas para ambos os hubs
    .select(
        col("hc.customer_hk").alias("customer_hk"),
        col("ho.order_hk").alias("order_hk")
    )
    # Criar uma hash key combinada
    .withColumn("customer_order_hk", md5(concat_ws("||", col("customer_hk"), col("order_hk"))))
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("TPC-H"))
)

link_data.createOrReplaceTempView("link_stage")


spark.sql("""
MERGE INTO L_Customer_Order AS target
USING link_stage AS source
ON target.customer_order_hk = source.customer_order_hk
WHEN NOT MATCHED THEN
  INSERT (customer_order_hk, customer_hk, order_hk, load_timestamp, record_source)
  VALUES (source.customer_order_hk, source.customer_hk, source.order_hk, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criando views de negócios
# MAGIC
# MAGIC Para facilitar a query para os usuários finais, podemos criar views que unem os vários componentes do Data Vault.

# COMMAND ----------

# DBTITLE 1,CREATE OR REPLACE VIEW Ouro.BV_Customer_Order
# MAGIC %sql
# MAGIC -- Views de Negócios combinando detalhes do cliente e do pedido (Isso pode ser materializado na camada Ouro, mas para este laboratório é apresentado como uma exibição)
# MAGIC
# MAGIC CREATE OR REPLACE VIEW gold.BV_Customer_Order AS
# MAGIC SELECT 
# MAGIC     hc.customer_id,
# MAGIC     sc.name AS customer_name,
# MAGIC     sc.address AS customer_address,
# MAGIC     ho.order_id,
# MAGIC     so.order_date,
# MAGIC     so.total_price,
# MAGIC     so.order_status
# MAGIC FROM 
# MAGIC     H_Customer hc
# MAGIC JOIN 
# MAGIC     S_Customer sc ON hc.customer_hk = sc.customer_hk
# MAGIC JOIN 
# MAGIC     L_Customer_Order lco ON hc.customer_hk = lco.customer_hk
# MAGIC JOIN 
# MAGIC     H_Order ho ON lco.order_hk = ho.order_hk
# MAGIC JOIN 
# MAGIC     S_Order so ON ho.order_hk = so.order_hk;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Exemplo de query
# MAGIC
# MAGIC Vamos executar uma query para demonstrar como usar nosso modelo do Data Vault:

# COMMAND ----------

# DBTITLE 1,SELECT * FROM gold.BV_Customer_Order
# MAGIC %sql
# MAGIC select * from gold.BV_Customer_Order

# COMMAND ----------

# DBTITLE 1,SELECT customer_name, total_sales FROM gold. BV_Customer_Order
# MAGIC %sql
# MAGIC -- Total de vendas por cliente
# MAGIC SELECT 
# MAGIC     customer_name,
# MAGIC     SUM(total_price) AS total_sales
# MAGIC FROM 
# MAGIC     gold.BV_Customer_Order
# MAGIC GROUP BY 
# MAGIC     customer_name
# MAGIC ORDER BY 
# MAGIC     total_sales DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Etapas de verificação
# MAGIC
# MAGIC Vamos realizar algumas verificações básicas para garantir que nosso Data Vault esteja funcionando corretamente:

# COMMAND ----------

# DBTITLE 1,Verifique as contagens de registros
# MAGIC %sql
# MAGIC -- Verificar contagens de registros
# MAGIC SELECT 'H_Customer' AS table_name, COUNT(*) AS record_count FROM H_Customer
# MAGIC UNION ALL
# MAGIC SELECT 'H_Order' AS table_name, COUNT(*) AS record_count FROM H_Order
# MAGIC UNION ALL
# MAGIC SELECT 'L_Customer_Order' AS table_name, COUNT(*) AS record_count FROM L_Customer_Order
# MAGIC UNION ALL
# MAGIC SELECT 'S_Customer' AS table_name, COUNT(*) AS record_count FROM S_Customer
# MAGIC UNION ALL
# MAGIC SELECT 'S_Order' AS table_name, COUNT(*) AS record_count FROM S_Order;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Verifique o pedido ao cliente
# MAGIC %sql
# MAGIC -- Verifique se cada pedido está associado a exatamente um cliente
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_orders,
# MAGIC   SUM(CASE WHEN customer_count = 1 THEN 1 ELSE 0 END) AS orders_with_one_customer,
# MAGIC   SUM(CASE WHEN customer_count != 1 THEN 1 ELSE 0 END) AS orders_with_multiple_customers
# MAGIC FROM (
# MAGIC   SELECT order_hk, COUNT(DISTINCT customer_hk) AS customer_count
# MAGIC   FROM L_Customer_Order
# MAGIC   GROUP BY order_hk
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza 
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,DROP Hubs, Links, Satélites, gold.BV_Customer_Order
# MAGIC %sql
# MAGIC -- Não estamos limpando a tabela refinada criada aqui, pois ela será necessária no laboratório da repositório de recursos
# MAGIC -- Remova os Satélites primeiro
# MAGIC DROP TABLE IF EXISTS S_Customer;
# MAGIC DROP TABLE IF EXISTS S_Order;
# MAGIC
# MAGIC -- Remova o Link
# MAGIC DROP TABLE IF EXISTS L_Customer_Order;
# MAGIC
# MAGIC -- Remova os Hubs
# MAGIC DROP TABLE IF EXISTS H_Customer;
# MAGIC DROP TABLE IF EXISTS H_Order;
# MAGIC
# MAGIC -- Eliminar views de negócios gold 
# MAGIC DROP VIEW IF EXISTS gold.BV_Customer_Order

# COMMAND ----------

# MAGIC %md
# MAGIC Remova todos os widgets criados durante a demonstração para limpar o ambiente do notebook.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC Nesta demonstração, implementamos com sucesso um modelo básico do Data Vault 2.0 no Databricks. Criamos Hubs, Links e Satélites usando hash keys, carregamos dados neles e configuramos visualizações de negócios para facilitar a querying. Você aprendeu como o Data Vault 2.0 fornece um método estruturado e flexível para gerenciar dados em larga escala. Com esse conhecimento, agora você pode criar e otimizar seus próprios modelos do Data Vault, oferecendo suporte a análises de dados escalonáveis e eficientes. Aprimoramentos futuros podem envolver a implementação de estratégias de carregamento incremental ou a expansão do cofre com mais entidades.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
