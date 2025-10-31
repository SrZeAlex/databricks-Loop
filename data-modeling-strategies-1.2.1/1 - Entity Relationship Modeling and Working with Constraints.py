# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Modelagem de Entidades e Relacionamentos e Manipulação de Restrições
# MAGIC Nesta demonstração, exploraremos como modelar dados usando o ERM (Entity Relationship Modeling) no Databricks. Você aprenderá a criar tabelas com restrições de primary key (PK) e foreign key (FK), carregar dados de datasets TPC-H, identificar violações de restrição e visualizar relacionamentos de tabela usando Entity Relationship Diagrams (ERDs). Além disso, você praticará o gerenciamento da consistência dos dados e a execução de operações de limpeza sem interromper o ambiente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final desta demonstração, você será capaz de:
# MAGIC - Aplicar restrições de primary e foreign key para modelar relacionamentos de tabela relacional no Databricks.
# MAGIC - Demonstrar como carregar dados em tabelas usando comandos SQL do esquema bronze.
# MAGIC - Avaliar o impacto de restrições não impostas e simular violações de integridade de dados.
# MAGIC - Analisar e solucionar problemas de violações de restrições usando operações SQL.
# MAGIC - Visualize Diagramas de Relacionamento de Entidade para interpretar relacionamentos de tabela de forma eficaz.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 1: Executar a configuração de laboratório (pré-requisito)
# MAGIC
# MAGIC Antes de prosseguir com o conteúdo desta e de outras atividades de laboratório, certifique-se de ter executado o script de configuração do laboratório.
# MAGIC Como resultado da execução do script, você criará:
# MAGIC 1. Um catálogo dedicado com o nome da sua conta de usuário de laboratório.  
# MAGIC 2. Esquemas nomeados `bronze`, `silver` e `gold` dentro do catálogo.  
# MAGIC 3. Tabelas TPC-H copiadas de Samples para o esquema `bronze`.

# COMMAND ----------

# DBTITLE 1,%run ./Includes/setup/lab_setup
# MAGIC %run ./Includes/setup/lab_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 2: Escolha o catálogo e o esquema de trabalho
# MAGIC
# MAGIC Ao longo deste laboratório, você pode criar novas tabelas em `silver`  (ou outro esquema de sua escolha). 
# MAGIC Para demonstração, usaremos o esquema `silver` no catálogo específico do usuário.  
# MAGIC

# COMMAND ----------

# DBTITLE 1,Catálogo de Trabalho e Esquema
import re

# Obter o usuário atual e extrair o nome do catálogo dividindo o e-mail em '@' e tomando a primeira parte
user_id = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]

# Substitua todos os caracteres especiais no `user_id` por um sublinhado '_' para criar o nome do catálogo
catalog_name = re.sub(r'[^a-zA-Z0-9]', '_', user_id) # Código novo

# Defina o nome do esquema a ser usado
schema_name = "silver"

# COMMAND ----------

# Criar um widget para capturar o nome do catálogo e todos os nomes de esquema
dbutils.widgets.text("catalog_name", catalog_name)
dbutils.widgets.text("schema_name", schema_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o catálogo atual para o nome do catálogo extraído
# MAGIC USE CATALOG IDENTIFIER(:catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defina o esquema atual para o nome do esquema definido
# MAGIC USE SCHEMA IDENTIFIER(:schema_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir os nomes default do catálogo e do esquema
# MAGIC SELECT current_catalog() AS Catalog_Name, current_schema() AS Schema_Name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 3: Criar tabelas com restrições
# MAGIC
# MAGIC Criaremos duas tabelas para demonstrar as restrições **Primary Key (PK)** e **Foreign Key (FK)**:
# MAGIC
# MAGIC 1. `lab_customer` com uma primary key em `c_custkey`.  
# MAGIC 2. `lab_orders` com:  
# MAGIC    - Uma primary key em `o_orderkey`.  
# MAGIC    - Uma foreign key em `o_custkey` referenciando `lab_customer(c_custkey)`.
# MAGIC
# MAGIC
# MAGIC Embora o Databricks **não** imponha restrições de primary key (PK) e foreign key (FK) por default, ele **os usa** para *explicar* as relações entre tabelas.
# MAGIC
# MAGIC Essas restrições são apenas informativas e servem para codificar relações entre campos em tabelas, sem impor a integridade dos dados.
# MAGIC
# MAGIC Essa abordagem permite que o Databricks forneça recursos úteis, como: Entity Relationship Diagrams (ERDs) no Catalog Explorer, que exibem visualmente as relações de primary key e foreign key entre tabelas como um gráfico.
# MAGIC
# MAGIC As restrições `NOT NULL` e `CHECK` são aplicadas na tabela.

# COMMAND ----------

# DBTITLE 1,CREATE silver.lab_customer
# MAGIC %sql
# MAGIC -- Crie a tabela lab_customer com uma restrição PRIMARY KEY no c_custkey
# MAGIC CREATE TABLE IF NOT EXISTS lab_customer 
# MAGIC (
# MAGIC   c_custkey INT,
# MAGIC   c_name STRING,
# MAGIC   c_address STRING,
# MAGIC   c_nationkey INT,
# MAGIC   c_phone STRING,
# MAGIC   c_acctbal DECIMAL(12,2),
# MAGIC   c_mktsegment STRING,
# MAGIC   c_comment STRING,
# MAGIC   CONSTRAINT pk_custkey PRIMARY KEY (c_custkey)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verifique o catálogo atual
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verifique o esquema atual
# MAGIC SELECT current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC **Nota:** Substitua os valores `<default_catalog_name>` e `<default_schema_name>` no código abaixo pelos nomes de catálogo e esquema default reais da saída da query nas células anteriores:
# MAGIC
# MAGIC **Target Code Line:**```dbsql
# MAGIC CONSTRAINT fk_custkey FOREIGN KEY (o_custkey) REFERENCES <default_catalog_name>.<default_schema_name>.lab_customer
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,CRIAR silver.lab_orders
# MAGIC %sql
# MAGIC -- Crie a tabela lab_orders com PRIMARY KEY no o_orderkey
# MAGIC -- e uma FOREIGN KEY referenciando lab_customer(c_custkey)
# MAGIC -- Nota: Forneça REFERENCES com namespace de três níveis
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lab_orders
# MAGIC (
# MAGIC   o_orderkey INT,
# MAGIC   o_custkey INT,
# MAGIC   o_orderstatus STRING,
# MAGIC   o_totalprice DECIMAL(12,2),
# MAGIC   o_orderdate DATE,
# MAGIC   o_orderpriority STRING,
# MAGIC   o_clerk STRING,
# MAGIC   o_shippriority INT,
# MAGIC   o_comment STRING,
# MAGIC   CONSTRAINT pk_orderkey PRIMARY KEY (o_orderkey),
# MAGIC   CONSTRAINT fk_custkey FOREIGN KEY (o_custkey) REFERENCES <default_catalog_name>.<default_schema_name>.lab_customer(c_custkey)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: Inserir dados das tabelas bronze TPC-H
# MAGIC
# MAGIC Preencheremos as tabelas recém-criadas `lab_customer` e `lab_orders` com os dados TPC-H localizados no esquema `bronze`.

# COMMAND ----------

# DBTITLE 1,INSERT INTO silver.lab_customer FROM bronze.customer
# MAGIC %sql
# MAGIC -- Inserir dados no lab_customer de bronze.cliente
# MAGIC INSERT INTO lab_customer
# MAGIC SELECT
# MAGIC   c_custkey,
# MAGIC   c_name,
# MAGIC   c_address,
# MAGIC   c_nationkey,
# MAGIC   c_phone,
# MAGIC   c_acctbal,
# MAGIC   c_mktsegment,
# MAGIC   c_comment
# MAGIC FROM bronze.customer;
# MAGIC

# COMMAND ----------

# DBTITLE 1,INSERT INTO silver.lab_orders FROM bronze.orders
# MAGIC %sql
# MAGIC -- Inserir dados em lab_orders de bronze.orders
# MAGIC INSERT INTO lab_orders
# MAGIC SELECT
# MAGIC   o_orderkey,
# MAGIC   o_custkey,
# MAGIC   o_orderstatus,
# MAGIC   o_totalprice,
# MAGIC   o_orderdate,
# MAGIC   o_orderpriority,
# MAGIC   o_clerk,
# MAGIC   o_shippriority,
# MAGIC   o_comment
# MAGIC FROM bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5: Demonstrar violações `CONSTRAINT`
# MAGIC
# MAGIC Como mencionado anteriormente, o Databricks não impõe as restrições que definimos para relacionamentos primários e externos. Vamos ver as implicações disso na prática.
# MAGIC
# MAGIC 1. **Foreign Key Violation**: Insira uma linha em `lab_orders` referenciando um `o_custkey` inexistente.  
# MAGIC 2. **Primary Key Violation**: Insira uma linha duplicada em `lab_customer`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Violação de Foreign Key
# MAGIC
# MAGIC Vamos inserir um pedido fazendo referência a um `o_custkey` inexistente. Na implementação atual, o Databricks não impõe essa relação por default, portanto, essa inserção pode ser bem-sucedida sem erros.

# COMMAND ----------

# DBTITLE 1,INSERT INTO silver.lab_orders
# MAGIC %sql
# MAGIC INSERT INTO lab_orders 
# MAGIC VALUES
# MAGIC (
# MAGIC   9999999,         -- o_orderkey
# MAGIC   9999999,         -- o_custkey (inexistente em lab_customer)
# MAGIC   'F',
# MAGIC   1000.00,
# MAGIC   current_date(),
# MAGIC   '3-LOW',
# MAGIC   'Clerk#000000001',
# MAGIC   0,
# MAGIC   'Testing invalid customer key'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### Violação de Primary Key
# MAGIC
# MAGIC Em seguida, inseriremos uma linha em `lab_customer` usando um valor para `c_custkey` que já existe na tabela. Novamente, isso não deve gerar um erro, devido ao comportamento default de nossas restrições não impostas.

# COMMAND ----------

# DBTITLE 1,INSERT INTO silver.lab_customer
# MAGIC %sql
# MAGIC -- Supondo que c_custkey = 1 existe em lab_customer
# MAGIC INSERT INTO lab_customer
# MAGIC VALUES
# MAGIC (
# MAGIC   1,
# MAGIC   'Duplicate Customer',
# MAGIC   'Duplicate Address',
# MAGIC   9999,
# MAGIC   '999-999-9999',
# MAGIC   9999.99,
# MAGIC   'DUPLICATE_SEGMENT',
# MAGIC   'Inserting a duplicate primary key for demonstration'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 6: Reverter para um estado limpo
# MAGIC
# MAGIC Para remover linhas problemáticas, podemos excluí-las diretamente, truncar as tabelas ou usar a viagem do tempo Delta para reverter. Abaixo está um exemplo de exclusão das linhas violadoras adicionadas recentemente (certifique-se de ajustar as keys, se você inseriu valores diferentes).

# COMMAND ----------

# DBTITLE 1,DELETE FROM silver.lab_orders
# MAGIC %sql
# MAGIC -- Remova a violação de foreign key (orderkey=9999999)
# MAGIC DELETE FROM lab_orders
# MAGIC WHERE o_orderkey = 9999999;
# MAGIC

# COMMAND ----------

# DBTITLE 1,DELETE FROM silver.lab_customer
# MAGIC %sql
# MAGIC -- Remover a linha de primary key duplicada
# MAGIC DELETE FROM lab_customer
# MAGIC WHERE c_custkey = 1 
# MAGIC   AND c_name = 'Duplicate Customer';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 7: Visualizando o diagrama de ER no Databricks
# MAGIC
# MAGIC Com base em nossa definição de restrições, podemos usar o Databricks para ver essas relações em um diagrama:
# MAGIC 1. No Databricks, navegue até o **Catalog** explorer à esquerda.  
# MAGIC 2. Selecione o catálogo (com base na identidade do usuário do laboratório) e, em seguida, o esquema (por exemplo, `silver`).  
# MAGIC 3. Localize a tabela `lab_orders`, que tem uma relação de foreign key com `lab_customer`.
# MAGIC 4. Clique no botão **View Relationships** para visualizar o diagrama de suas tabelas e suas relações.  
# MAGIC
# MAGIC Você deve ver que `lab_orders` tem uma foreign key que referencia `lab_customer`.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Passo 8: Desmontagem do laboratório para tabelas criadas
# MAGIC
# MAGIC Solte apenas as tabelas de laboratório prateadas que você criou, deixando suas tabelas TPC-H principais em bronze e o ambiente geral intacto.

# COMMAND ----------

# DBTITLE 1,DROP silver.lab_orders, DROP silver.lab_customer
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lab_orders;
# MAGIC DROP TABLE IF EXISTS lab_customer;

# COMMAND ----------

# MAGIC %md
# MAGIC Remova todos os widgets criados durante a demonstração para limpar o ambiente do notebook.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC Ao longo desta demonstração, aplicamos técnicas de modelagem de data warehousing para criar e gerenciar tabelas no Databricks. Estabelecemos relações de primary and foreign key usando restrições, inserimos dados e exploramos como o Databricks lida com violações de restrição. Ao utilizar Entity Relationship Diagrams, visualizamos as relações de tabela e compreendemos seu significado estrutural. Ao final desta demonstração, você terá desenvolvido habilidades práticas em modelagem de dados relacionais, gerenciamento de esquemas e solução de problemas de erros em uma plataforma de dados modernos, capacitando-o a criar e gerenciar soluções de dados escaláveis.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
